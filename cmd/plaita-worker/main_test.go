package main

import (
	"bytes"
	"context"
	"log/slog"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

// Issue #75: a failed Run (startup error such as XGROUP CREATE WRONGTYPE)
// must end the process non-zero with the cause on stderr — never linger
// waiting for signals, never masquerade as a clean shutdown on SIGTERM.
// runWorker returns the exit code (instead of os.Exit) so the paths are
// assertable in-process; these tests deliberately run serially (signal
// delivery to the test process is process-global).

// i75Config mirrors pkg/worker's testConfig() with a scratch key prefix so
// nothing here can touch production key spaces.
func i75Config(mr *miniredis.Miniredis) *worker.Config {
	queue := "plaita-i75:flow:queue"
	return &worker.Config{
		RedisURL:          mr.Addr(),
		QueueName:         queue,
		ConsumerGroup:     "plaita-i75-workers",
		ConsumerName:      "worker-test",
		InstanceID:        "worker-test",
		KeyPrefix:         "plaita-i75",
		DLQKey:            queue + ":dlq",
		ClaimMinIdleMS:    20,
		LeaseTTLSeconds:   120,
		MaxDeliveries:     5,
		DLQMaxLen:         1000,
		ReadBlockMS:       100,
		DefaultTenantID:   "default",
		EventTTLSeconds:   7 * 86400,
		CancelPollMS:      40,
		HeartbeatInterval: time.Hour,
		RegistryTTL:       30 * time.Second,
		SweepInterval:     time.Hour,
		DrainTimeout:      5 * time.Second,
		FlowCacheSize:     10,
		FlowCacheTTL:      time.Minute,
	}
}

// i75Worker wires a worker + capture logger against miniredis and poisons the
// queue key with a plain string so EnsureGroup fails WRONGTYPE.
func i75StartupFailWorker(t *testing.T) (*worker.Worker, *worker.Config, *bytes.Buffer) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := i75Config(mr)
	require.NoError(t, rdb.Do(context.Background(), "SET", cfg.QueueName, "not-a-stream").Err())
	var logBuf bytes.Buffer
	return worker.New(cfg, rdb, slog.New(slog.NewTextHandler(&logBuf, nil))), cfg, &logBuf
}

func waitExitCode(t *testing.T, rc <-chan int, what string) int {
	t.Helper()
	select {
	case code := <-rc:
		return code
	case <-time.After(10 * time.Second):
		t.Fatalf("runWorker did not return: %s", what)
		return -1
	}
}

// Case A: startup failure with no signal → exit non-zero, cause named, no
// "stopped cleanly". Pre-fix the process hung alive until a signal (red).
func Test_runWorker_startup_failure_exits_nonzero(t *testing.T) {
	w, cfg, logBuf := i75StartupFailWorker(t)
	rc := make(chan int, 1)
	go func() { rc <- runWorker(context.Background(), cfg, w, slog.New(slog.NewTextHandler(logBuf, nil))) }()

	code := waitExitCode(t, rc, "after startup failure")
	assert.Equal(t, 1, code)
	logs := logBuf.String()
	assert.Contains(t, logs, "worker exited with error")
	assert.Contains(t, logs, "ensure consumer group")
	assert.NotContains(t, logs, "worker stopped cleanly")
}

// Case B: startup failure + SIGTERM → still non-zero, still no "cleanly"
// (pre-fix: SIGTERM hit the closed done-channel and printed exit 0, red).
func Test_runWorker_startup_failure_then_signal_no_clean_shutdown(t *testing.T) {
	w, cfg, logBuf := i75StartupFailWorker(t)
	rc := make(chan int, 1)
	go func() { rc <- runWorker(context.Background(), cfg, w, slog.New(slog.NewTextHandler(logBuf, nil))) }()

	// Let Run fail and land the error in runDone first; the signal then races
	// a populated runDone. Both select outcomes must end non-zero without a
	// clean-shutdown log.
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))

	code := waitExitCode(t, rc, "after startup failure + signal")
	assert.Equal(t, 1, code)
	logs := logBuf.String()
	assert.NotContains(t, logs, "worker stopped cleanly")
	assert.Contains(t, logs, "worker exited with error")

	// Drop our signal registration so the stale second-signal goroutine (if
	// the sigCh branch won the select) can never eat a later test's SIGTERM
	// and os.Exit(130) the test binary.
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)
}

// Case C (positive control): healthy empty queue → SIGTERM enters D1 drain,
// Run returns nil, exit 0 with "worker stopped cleanly".
func Test_runWorker_clean_drain_on_signal(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := i75Config(mr)
	var logBuf bytes.Buffer
	w := worker.New(cfg, rdb, slog.New(slog.NewTextHandler(&logBuf, nil)))

	rc := make(chan int, 1)
	go func() { rc <- runWorker(context.Background(), cfg, w, slog.New(slog.NewTextHandler(&logBuf, nil))) }()

	// Wait until EnsureGroup created the stream (mkstream) before signalling,
	// so the drain path is exercised, not the startup-failure path.
	deadline := time.Now().Add(5 * time.Second)
	for !mr.Exists(cfg.QueueName) {
		if time.Now().After(deadline) {
			t.Fatal("queue stream never created; EnsureGroup did not complete")
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))

	code := waitExitCode(t, rc, "clean drain")
	assert.Equal(t, 0, code)
	logs := logBuf.String()
	assert.Contains(t, logs, "shutdown signal received")
	assert.Contains(t, logs, "worker stopped cleanly")
	assert.NotContains(t, logs, "worker exited with error")
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)
}
