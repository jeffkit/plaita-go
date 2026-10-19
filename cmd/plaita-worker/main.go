// Command plaita-worker runs the plaita-go runtime worker MVP: consume flow
// tasks from the Redis stream queue, execute them on the plaita-go engine and
// persist execution state + events back.
//
// Every key the worker touches (queue stream, consumer group, DLQ, state /
// lease / fence / cancel / registry / event keys) derives from
// PLAITA_KEY_PREFIX (default "plaita"). Shadow mode:
//
//	PLAITA_KEY_PREFIX=plaita-go-shadow
//
// rewrites the whole key space (queue → plaita-go-shadow:flow:queue, group →
// plaita-go-shadow-workers, DLQ → <queue>:dlq, state rows under
// plaita-go-shadow:execution:*) so a shadow run can never touch production
// keys. Explicit PLAITA_QUEUE_NAME / PLAITA_CONSUMER_GROUP / PLAITA_DLQ_KEY
// overrides win over the derived defaults.
package main

import (
	"context"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		logger.Info("plaita-worker running",
			"queue", cfg.QueueName,
			"group", cfg.ConsumerGroup,
			"consumer", cfg.ConsumerName,
			"key_prefix", cfg.KeyPrefix,
		)
		if err := w.Run(ctx); err != nil {
			logger.Error("worker exited with error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, waiting for in-flight task", "drain_timeout", cfg.DrainTimeout)
	waitCh := make(chan struct{})
	go func() {
		// A second signal force-exits.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Warn("second signal, force exit")
		os.Exit(130)
	}()
	go func() {
		stop()
		<-done
		close(waitCh)
	}()
	select {
	case <-waitCh:
		logger.Info("worker stopped cleanly")
	case <-time.After(cfg.DrainTimeout):
		logger.Warn("drain timeout exceeded, exiting with in-flight work pending")
	}
}

func openRedis(redisURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(opts), nil
}
