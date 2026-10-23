package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
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
// runWorkerUntil returns the exit code (instead of os.Exit) and takes the
// shutdown-signal source as a parameter, so every path is assertable
// in-process through a plain channel.
//
// No test here raises a real process signal. signal.Notify registrations and
// the os/signal dispatch goroutine are process-global and asynchronous: a
// SIGTERM one test sent to itself could be dispatched after the next test had
// registered its own channel, enter that run's drain, and leave a stale
// "second signal → exit 130" goroutine behind — which then ate the next
// SIGTERM and killed the whole test binary (main CI runs red at 0.2s with
// "exit status 130", nothing else failing).

// logBuffer is a goroutine-safe log sink: the worker's own logger keeps
// writing from Run's deferred cleanup after runWorkerUntil has returned and
// the test is already reading.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

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

// i75StartupFailWorker wires a worker + capture logger against miniredis and
// poisons the queue key with a plain string so EnsureGroup fails WRONGTYPE.
func i75StartupFailWorker(t *testing.T) (*worker.Worker, *worker.Config, *logBuffer) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := i75Config(mr)
	require.NoError(t, rdb.Do(context.Background(), "SET", cfg.QueueName, "not-a-stream").Err())
	logBuf := &logBuffer{}
	return worker.New(cfg, rdb, slog.New(slog.NewTextHandler(logBuf, nil))), cfg, logBuf
}

// startRunWorker runs runWorkerUntil in the background with an injected
// signal channel (buffered like the production one) and returns both.
func startRunWorker(cfg *worker.Config, w drainableRunner, logBuf *logBuffer) (<-chan int, chan os.Signal) {
	rc := make(chan int, 1)
	sigCh := make(chan os.Signal, 2)
	go func() {
		rc <- runWorkerUntil(context.Background(), cfg, w, slog.New(slog.NewTextHandler(logBuf, nil)), sigCh)
	}()
	return rc, sigCh
}

func waitExitCode(t *testing.T, rc <-chan int, what string) int {
	t.Helper()
	select {
	case code := <-rc:
		return code
	case <-time.After(10 * time.Second):
		t.Fatalf("runWorkerUntil did not return: %s", what)
		return -1
	}
}

// Case A: startup failure with no signal → exit non-zero, cause named, no
// "stopped cleanly". Pre-fix the process hung alive until a signal (red).
func Test_runWorker_startup_failure_exits_nonzero(t *testing.T) {
	w, cfg, logBuf := i75StartupFailWorker(t)
	rc, _ := startRunWorker(cfg, w, logBuf)

	code := waitExitCode(t, rc, "after startup failure")
	assert.Equal(t, 1, code)
	logs := logBuf.String()
	assert.Contains(t, logs, "worker exited with error")
	assert.Contains(t, logs, "ensure consumer group")
	assert.NotContains(t, logs, "worker stopped cleanly")
}

// Case B: startup failure + SIGTERM → still non-zero, still no "cleanly"
// (pre-fix: SIGTERM hit the closed done-channel and printed exit 0, red).
// The signal is queued before the driver starts, so the first select sees it
// ready while Run is still failing: the drain-then-error path is the common
// outcome here, and if Run wins the race instead the result must be the same.
func Test_runWorker_startup_failure_then_signal_no_clean_shutdown(t *testing.T) {
	w, cfg, logBuf := i75StartupFailWorker(t)
	rc := make(chan int, 1)
	sigCh := make(chan os.Signal, 2)
	sigCh <- syscall.SIGTERM
	go func() {
		rc <- runWorkerUntil(context.Background(), cfg, w, slog.New(slog.NewTextHandler(logBuf, nil)), sigCh)
	}()

	code := waitExitCode(t, rc, "after startup failure + signal")
	assert.Equal(t, 1, code)
	logs := logBuf.String()
	assert.NotContains(t, logs, "worker stopped cleanly")
	assert.Contains(t, logs, "worker exited with error")
}

// Case C (positive control): healthy empty queue → SIGTERM enters D1 drain,
// Run returns nil, exit 0 with "worker stopped cleanly".
func Test_runWorker_clean_drain_on_signal(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := i75Config(mr)
	logBuf := &logBuffer{}
	w := worker.New(cfg, rdb, slog.New(slog.NewTextHandler(logBuf, nil)))
	rc, sigCh := startRunWorker(cfg, w, logBuf)

	// Wait until EnsureGroup created the stream (mkstream) before signalling,
	// so the drain path is exercised, not the startup-failure path.
	deadline := time.Now().Add(5 * time.Second)
	for !mr.Exists(cfg.QueueName) {
		if time.Now().After(deadline) {
			t.Fatal("queue stream never created; EnsureGroup did not complete")
		}
		time.Sleep(20 * time.Millisecond)
	}
	sigCh <- syscall.SIGTERM

	code := waitExitCode(t, rc, "clean drain")
	assert.Equal(t, 0, code)
	logs := logBuf.String()
	assert.Contains(t, logs, "shutdown signal received")
	assert.Contains(t, logs, "worker stopped cleanly")
	assert.NotContains(t, logs, "worker exited with error")
}

// scriptedRunner stands in for *worker.Worker: Run blocks until the driver
// force-stops it (or the context dies), so the second-signal branch can be
// pinned without racing a real drain that finishes in one read slice.
type scriptedRunner struct {
	drains   atomic.Int32
	forces   atomic.Int32
	stopped  chan struct{}
	stopOnce sync.Once
}

func newScriptedRunner() *scriptedRunner {
	return &scriptedRunner{stopped: make(chan struct{})}
}

func (s *scriptedRunner) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-s.stopped:
	}
	return nil
}

func (s *scriptedRunner) RequestDrain(string) { s.drains.Add(1) }

func (s *scriptedRunner) ForceStop(string) {
	s.forces.Add(1)
	s.stopOnce.Do(func() { close(s.stopped) })
}

// Case D (D3 second signal): drain requested by the first signal, a second
// one while work is still in flight → force-stop and exit 130 at once, no
// waiting for Run. Previously this path called os.Exit from a goroutine that
// outlived runWorker — untestable, and the source of the CI flake.
func Test_runWorker_second_signal_force_stops_with_130(t *testing.T) {
	// Only the logging fields and DrainTimeout are read on this path; no
	// redis is involved.
	cfg := &worker.Config{QueueName: "plaita-i75:flow:queue", DrainTimeout: 5 * time.Second}
	logBuf := &logBuffer{}
	s := newScriptedRunner()
	rc, sigCh := startRunWorker(cfg, s, logBuf)

	sigCh <- syscall.SIGTERM
	require.Eventually(t, func() bool { return s.drains.Load() == 1 }, 5*time.Second, 10*time.Millisecond,
		"first signal must request drain")
	select {
	case code := <-rc:
		t.Fatalf("driver returned %d before the second signal; Run was still in flight", code)
	default:
	}
	sigCh <- syscall.SIGINT

	code := waitExitCode(t, rc, "second signal")
	assert.Equal(t, exitCodeSecondSignal, code)
	assert.Equal(t, int32(1), s.forces.Load(), "ForceStop must run exactly once")
	logs := logBuf.String()
	assert.Contains(t, logs, "shutdown signal received")
	assert.Contains(t, logs, "second signal, force stop")
	assert.NotContains(t, logs, "worker stopped cleanly")
}
