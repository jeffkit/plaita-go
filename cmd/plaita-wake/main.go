// Command plaita-wake runs the plaita-go wake services: the companion
// processes that consume the suspend-service queues the worker's §5.2
// dispatch writes and publish the resume events that wake suspended flows.
// It is the Go counterpart of Python `python -m plaita.server.services wake
// --sources delay,...` — a pure-Go deployment (plaita-worker + plaita-wake,
// no Python services) gets the full managed execution face.
//
//	plaita-wake -sources delay
//
// Every key derives from PLAITA_KEY_PREFIX (default "plaita"): the delay
// queue `{prefix}:delay:queue` (+:scheduled/:attempts/:dead), the resume
// event channels `{ns}:events:{event_type}` and the roll-call registry
// `{prefix}:registry:delay:{instance_id}` (TTL 30s, 10s heartbeat). Shadow
// deployments set PLAITA_KEY_PREFIX=plaita-go-shadow like the worker.
//
// Shutdown mirrors the worker's D3 path: SIGINT/SIGTERM stops the consume
// loops at a safe boundary (in-flight triggers land or stay parked in the
// schedule ZSET — at-least-once), tombstones the registry record and exits;
// a second signal exits immediately.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/wake"
)

func main() {
	sources := flag.String("sources", wake.SourceDelay,
		"comma-separated wake sources to run (implemented: "+strings.Join(wake.Sources(), ",")+")")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("plaita-wake (plaita-go wake services)")
		return
	}

	logger := slog.Default()
	cfg := wake.ConfigFromEnv()
	cfg.Logger = logger

	list := parseSources(*sources)
	if len(list) == 0 {
		logger.Error("no wake sources requested", "implemented", wake.Sources())
		os.Exit(2)
	}

	rdb, err := openRedis(cfg.RedisURL)
	if err != nil {
		logger.Error("wake redis connect failed", "url", cfg.RedisURL, "error", err)
		os.Exit(1)
	}

	services := make([]wake.Service, 0, len(list))
	for _, source := range list {
		svc, err := wake.NewService(source, cfg, rdb)
		if err != nil {
			logger.Error("wake source build failed", "source", source, "error", err,
				"implemented", wake.Sources())
			os.Exit(2)
		}
		services = append(services, svc)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, len(services))
	for _, svc := range services {
		go func(s wake.Service) { errCh <- s.Run(ctx) }(svc)
	}
	logger.Info("plaita-wake running",
		"sources", list, "key_prefix", cfg.KeyPrefix, "instance", cfg.InstanceID)

	// First signal lands in ctx (graceful); a second one exits immediately
	// (D3) — same contract as cmd/plaita-worker.
	watchSecondSignal(logger)
	for range services {
		if err := <-errCh; err != nil {
			logger.Error("wake source exited with error", "error", err)
		}
	}
	logger.Info("plaita-wake stopped cleanly")
}

func parseSources(raw string) []string {
	out := make([]string, 0, 4)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func openRedis(url string) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5_000_000_000)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func watchSecondSignal(logger *slog.Logger) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		logger.Warn("second signal: exiting immediately (in-flight triggers stay parked in the schedule)")
		os.Exit(130)
	}()
}
