// Command plaita-worker runs the plaita-go runtime worker: consume flow tasks
// from the Redis stream queue, execute them on the plaita-go engine and
// persist execution state + events back.
//
// Every key the worker touches (queue stream, consumer group, DLQ, dispatch
// ledger, state / lease / fence / cancel / registry / event keys) derives from
// PLAITA_KEY_PREFIX (default "plaita"). Shadow mode:
//
//	PLAITA_KEY_PREFIX=plaita-go-shadow
//
// rewrites the whole key space (queue → plaita-go-shadow:flow:queue, group →
// plaita-go-shadow-workers, DLQ → <queue>:dlq, state rows under
// plaita-go-shadow:execution:*) so a shadow run can never touch production
// keys. Explicit PLAITA_QUEUE_NAME / PLAITA_CONSUMER_GROUP / PLAITA_DLQ_KEY /
// PLAITA_DISPATCH_LEDGER_KEY overrides win over the derived defaults.
//
// Tenant-scoped flows reach the redis node only through the deployment's
// target allowlist (#37): PLAITA_REDIS_TARGET_ALLOWLIST (comma/whitespace-
// separated redis URLs) or nodes.SetRedisTargetAllowlist — without one, every
// redis target is refused for non-default tenants.
//
// Execution budget (#35, plaita#267): one engine invocation may occupy the
// single consumer for at most PLAITA_EXECUTION_DEADLINE_SECONDS (default
// 1800s; an invalid or ≤0 value falls back to the default with a warning —
// never silently unlimited). Past the budget the run is cancelled
// cooperatively and terminalized with the named non-retryable
// execution_deadline_exceeded error; see docs/WORKER_PROTOCOL.md §9.5 for
// the stacking with node timeout / retryTimes caps.
//
// Shutdown is the protocol's D3 path: SIGINT/SIGTERM only requests drain
// (D1); the consumption loop exits at the task boundary, and a bounded timer
// force-stops (D2) if in-flight work outlives PLAITA_WORKER_DRAIN_TIMEOUT.
// A second signal exits immediately.
//
// DLQ replay (the single manual re-injection entry point) is available as a
// one-shot CLI mode:
//
//	plaita-worker -dlq-replay 1712345678901-0 -dlq-actor ops
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/internal/version"
	"github.com/jeffkit/plaita-go/pkg/worker"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	replayID := flag.String("dlq-replay", "", "replay one dead-letter entry by DLQ message id and exit")
	dlqKey := flag.String("dlq-key", "", "DLQ stream key (default: <queue>:dlq)")
	replayTarget := flag.String("dlq-target", "", "target stream (default: the worker queue)")
	replayActor := flag.String("dlq-actor", "", "audit actor for the replay")
	replayForce := flag.Bool("dlq-force", false, "bypass an existing replay marker")
	replayExecution := flag.String("dlq-execution-id", "", "explicit execution id for carriers that lack one")
	replayTenant := flag.String("dlq-tenant", "", "explicit tenant id for carriers that lack one")
	flag.Parse()
	if *showVersion {
		fmt.Println(version.Print("plaita-worker"))
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := worker.DefaultConfig()

	rdb, err := openRedis(cfg.RedisURL)
	if err != nil {
		logger.Error("redis connect failed", "url", cfg.RedisURL, "error", err)
		os.Exit(1)
	}
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Error("redis ping failed", "url", cfg.RedisURL, "error", err)
		os.Exit(1)
	}

	w := worker.New(cfg, rdb, logger)

	if *replayID != "" {
		runReplay(w, logger, worker.ReplayOptions{
			DLQKey:       *dlqKey,
			DLQID:        *replayID,
			TargetStream: *replayTarget,
			Actor:        *replayActor,
			Force:        *replayForce,
			ExecutionID:  *replayExecution,
			TenantID:     *replayTenant,
		})
		return
	}

	os.Exit(runWorker(context.Background(), cfg, w, logger))
}

// runWorker drives the consume loop until Run returns or a signal arrives,
// and returns the process exit code (#75): non-zero when Run itself failed
// (a startup failure must not masquerade as a clean shutdown), 0 on clean
// drain / cancellation.
func runWorker(ctx context.Context, cfg *worker.Config, w *worker.Worker, logger *slog.Logger) int {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// D3: the signal handler only *requests* drain — in-flight work is never
	// severed at an arbitrary instruction boundary.
	runDone := make(chan error, 1)
	go func() {
		logger.Info("plaita-worker running",
			"queue", cfg.QueueName,
			"group", cfg.ConsumerGroup,
			"consumer", cfg.ConsumerName,
			"key_prefix", cfg.KeyPrefix,
			"drain_timeout", cfg.DrainTimeout,
		)
		runDone <- w.Run(ctx)
	}()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-runDone:
		// Run only returns nil on ctx cancellation or drain completion; a
		// non-nil return (e.g. EnsureGroup failure) must exit non-zero even
		// before any signal arrives.
		return exitOnRunResult(err, logger)
	case sig := <-sigCh:
		logger.Info("shutdown signal received, entering drain (stop taking new tasks)",
			"signal", sig.String(), "drain_timeout", cfg.DrainTimeout)
		w.RequestDrain("signal:" + sig.String())
	}

	go func() {
		<-sigCh
		logger.Warn("second signal, force stop")
		w.ForceStop("second signal")
		cancel()
		os.Exit(130)
	}()

	deadline := cfg.DrainTimeout + 5*time.Second
	select {
	case err := <-runDone:
		return exitOnRunResult(err, logger)
	case <-time.After(deadline):
		logger.Warn("drain deadline exceeded, force-stopping in-flight work",
			"deadline", deadline)
		w.ForceStop("shutdown deadline")
		cancel()
		select {
		case err := <-runDone:
			return exitOnRunResult(err, logger)
		case <-time.After(5 * time.Second):
			logger.Error("worker did not stop after force-stop")
			return 1
		}
	}
}

// exitOnRunResult maps a Run return to the exit code and the conclusive log
// line: error → non-zero with the cause on stderr, nil → "stopped cleanly".
func exitOnRunResult(err error, logger *slog.Logger) int {
	if err != nil {
		logger.Error("worker exited with error", "error", err)
		return 1
	}
	logger.Info("worker stopped cleanly")
	return 0
}

// runReplay executes the one-shot DLQ replay mode and prints the result JSON.
func runReplay(w *worker.Worker, logger *slog.Logger, opts worker.ReplayOptions) {
	res := w.ReplayDeadLetter(context.Background(), opts)
	blob, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(blob))
	if !res.Replayed {
		logger.Warn("dlq replay did not re-enqueue", "reason", res.Reason, "detail", res.Detail)
		os.Exit(1)
	}
}

func openRedis(redisURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(opts), nil
}
