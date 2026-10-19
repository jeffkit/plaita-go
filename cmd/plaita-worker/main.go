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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		logger.Info("plaita-worker running",
			"queue", cfg.QueueName,
			"group", cfg.ConsumerGroup,
			"consumer", cfg.ConsumerName,
			"key_prefix", cfg.KeyPrefix,
			"drain_timeout", cfg.DrainTimeout,
		)
		if err := w.Run(ctx); err != nil {
			logger.Error("worker exited with error", "error", err)
		}
	}()

	// D3: the signal handler only *requests* drain — in-flight work is never
	// severed at an arbitrary instruction boundary.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutdown signal received, entering drain (stop taking new tasks)",
		"signal", sig.String(), "drain_timeout", cfg.DrainTimeout)
	w.RequestDrain("signal:" + sig.String())

	go func() {
		<-sigCh
		logger.Warn("second signal, force stop")
		w.ForceStop("second signal")
		cancel()
		os.Exit(130)
	}()

	deadline := cfg.DrainTimeout + 5*time.Second
	select {
	case <-done:
		logger.Info("worker stopped cleanly")
	case <-time.After(deadline):
		logger.Warn("drain deadline exceeded, force-stopping in-flight work",
			"deadline", deadline)
		w.ForceStop("shutdown deadline")
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			logger.Error("worker did not stop after force-stop")
			os.Exit(1)
		}
	}
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
