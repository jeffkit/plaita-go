package worker

// Execution-level deadline (#35, plaita#267 Go 同型): a legal flow must not be
// able to monopolize the single-consumer worker. The negative control in this
// file is deliberately env-driven (DefaultConfig reads
// PLAITA_EXECUTION_DEADLINE_SECONDS), so the SAME file reds on a pre-#35
// baseline: there the variable is ignored, the self-loop keeps the consumer
// busy forever and the bounded watchdog below fires.

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// selfLoopFlow: a switch whose first branch always matches and routes back to
// itself; the escape branch exists and is statically reachable (validateDoc R1
// reachability) but its condition ($INPUT.stop) is absent from the params, so
// at runtime the flow steps route→route→… forever. Pre-#35 that occupied the
// only consumer without any bound (issue probe: retryTimes=300 held the
// consumer 4.96s; 1e9 extrapolates to ~191 days per delivery).
const selfLoopFlow = `{
  "flow_id": "exec_deadline_self_loop",
  "inputType": {"dataType": "object"},
  "nodes": [
    {"type": "start", "id": "start", "next": "route"},
    {"type": "switch", "id": "route", "branches": [
      {"name": "again", "condition": {"field": "$INPUT.loop", "operator": "truthy"}, "next": "route"},
      {"name": "done", "condition": {"field": "$INPUT.stop", "operator": "truthy"}, "next": "end"}
    ]},
    {"type": "end", "id": "end", "resultType": "success", "output": "escaped"}
  ]
}`

// deadlineTestConfig builds a shadow test config that honors the deadline env
// (unlike testConfig, which stubs the environment out).
func deadlineTestConfig(t *testing.T) *Config {
	t.Helper()
	t.Setenv("PLAITA_EXECUTION_DEADLINE_SECONDS", "2")
	cfg := DefaultConfig()
	cfg.ApplyShadow()
	cfg.ClaimMinIdleMS = 20
	cfg.ReadBlockMS = 100
	cfg.CancelPollMS = 40
	cfg.LeaseTTLSeconds = 120
	cfg.HeartbeatInterval = time.Hour // keep heartbeat ticks out of test windows
	cfg.SweepInterval = time.Hour
	return cfg
}

func newDeadlineTestWorker(t *testing.T) (*Worker, *redis.Client, *Config) {
	t.Helper()
	mr := miniredis.RunT(t)
	cfg := deadlineTestConfig(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return New(cfg, rdb, logger), rdb, cfg
}

// TestWorkerExecutionDeadlineTerminalizesSelfLoop: the self-loop is
// terminalized inside the worker budget with a named, non-retryable error
// signature, and the consumer moves on to the queued-behind task immediately
// (the recovery-latency half of the issue's acceptance).
func TestWorkerExecutionDeadlineTerminalizesSelfLoop(t *testing.T) {
	w, rdb, cfg := newDeadlineTestWorker(t)
	ctx := context.Background()
	if err := w.flows.PresetFlowDefinition(ctx, "", "exec_deadline_self_loop", "latest", []byte(selfLoopFlow)); err != nil {
		t.Fatal(err)
	}
	if err := w.flows.PresetFlowDefinition(ctx, "", "hello_world_go", "latest", mustHelloWorld(t)); err != nil {
		t.Fatal(err)
	}

	// Two carriers: the monopoly first, a second task queued behind it — the
	// exact starvation shape from the issue probe.
	monopoly := newExecutionID()
	follower := newExecutionID()
	startMonopoly := baseStartPayload(monopoly)
	startMonopoly["flow_id"] = "exec_deadline_self_loop"
	startMonopoly["params"] = map[string]any{"loop": true}
	enqueueTask(t, rdb, cfg, startMonopoly, "")
	enqueueTask(t, rdb, cfg, baseStartPayload(follower), "")

	// ProcessOne runs the monopoly to its terminal. The watchdog bounds a
	// pre-#35 baseline to a red verdict instead of a hang (there the loop
	// never returns: no execution budget exists).
	began := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := w.ProcessOne(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ProcessOne (monopoly): %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("self-loop still occupying the single consumer after 30s: no execution-level deadline (pre-#35 monopoly, negative control)")
	}
	occupied := time.Since(began)

	// The terminal lands inside the budget window: not before it (that would
	// mean the gate came from something else), not far past it.
	const budget = 2 * time.Second
	if occupied < budget {
		t.Fatalf("monopoly ended after %s — before the %s budget; the gate did not come from the execution deadline", occupied, budget)
	}
	if occupied > budget+6*time.Second {
		t.Fatalf("monopoly ended after %s — overshoot past the %s budget is too large", occupied, budget)
	}

	state := loadStateRow(t, rdb, cfg, monopoly)
	if state.Status != StatusError {
		t.Fatalf("monopoly status = %q, want error", state.Status)
	}
	if state.Error["error_type"] != "execution_deadline_exceeded" {
		t.Fatalf("error_type = %v, want execution_deadline_exceeded (named signature)", state.Error["error_type"])
	}
	details, _ := state.Error["details"].(map[string]any)
	if details == nil {
		t.Fatalf("error details missing: %v", state.Error)
	}
	if _, ok := details["deadline_seconds"]; !ok {
		t.Fatalf("error details missing deadline_seconds: %v", details)
	}
	if _, ok := details["occupied_seconds"]; !ok {
		t.Fatalf("error details missing occupied_seconds: %v", details)
	}
	// Timeline carries the marker event so the occupation is auditable (§5.1).
	timeline, _ := state.Timeline["events"].([]any)
	foundMarker := false
	for _, raw := range timeline {
		ev, _ := raw.(map[string]any)
		if ev["type"] == "execution_deadline_exceeded" {
			foundMarker = true
			break
		}
	}
	if !foundMarker {
		t.Fatalf("timeline has no execution_deadline_exceeded event: %v", state.Timeline)
	}

	// Recovery: the task queued behind the monopoly is processed right after
	// the terminal — the single consumer is free again.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne (follower): %v", err)
	}
	fstate := loadStateRow(t, rdb, cfg, follower)
	if fstate.Status != StatusCompleted {
		t.Fatalf("follower status = %q, want completed (consumer recovered)", fstate.Status)
	}

	// Queue hygiene: both carriers acked; nothing pending; NOTHING
	// dead-lettered; no retry carrier re-enqueued for the deadline terminal.
	if n := streamLen(t, rdb, cfg.QueueName); n != 0 {
		t.Fatalf("queue XLEN = %d, want 0 (no retry carrier after the deadline terminal)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 0 {
		t.Fatalf("PEL = %d, want 0", n)
	}
	if n := streamLen(t, rdb, cfg.DLQKey); n != 0 {
		t.Fatalf("DLQ XLEN = %d, want 0 (a deadline terminal is an ordinary error terminal, not a dead letter)", n)
	}
}

// TestWorkerExecutionDeadlineHookOptOut: the deployment hook's negative value
// disables the budget — the self-loop is NOT terminalized within what would
// have been the budget window (the gate demonstrably comes from the deadline,
// not from some other guard). Post-fix behavior-conservation control; the
// environment can never express this opt-out (envSecondsDuration rejects ≤0).
func TestWorkerExecutionDeadlineHookOptOut(t *testing.T) {
	mr := miniredis.RunT(t)
	t.Setenv("PLAITA_EXECUTION_DEADLINE_SECONDS", "2")
	cfg := DefaultConfig()
	cfg.ApplyShadow()
	cfg.ClaimMinIdleMS = 20
	cfg.ReadBlockMS = 100
	cfg.CancelPollMS = 40
	cfg.LeaseTTLSeconds = 120
	cfg.HeartbeatInterval = time.Hour
	cfg.SweepInterval = time.Hour
	cfg.ExecutionDeadline = -1 // deployment hook: explicit opt-out
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w := New(cfg, rdb, logger)
	ctx := context.Background()
	if err := w.flows.PresetFlowDefinition(ctx, "", "exec_deadline_self_loop", "latest", []byte(selfLoopFlow)); err != nil {
		t.Fatal(err)
	}

	monopoly := newExecutionID()
	startMonopoly := baseStartPayload(monopoly)
	startMonopoly["flow_id"] = "exec_deadline_self_loop"
	startMonopoly["params"] = map[string]any{"loop": true}
	enqueueTask(t, rdb, cfg, startMonopoly, "")

	done := make(chan error, 1)
	go func() {
		_, err := w.ProcessOne(ctx)
		done <- err
	}()

	// Observe past the would-be budget: no terminal may appear.
	time.Sleep(3500 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("self-loop returned early with the budget disabled (err=%v) — something else is gating", err)
	default:
	}
	rowRaw, err := rdb.Get(ctx, stateKeyOf(cfg, monopoly)).Result()
	if err != nil {
		t.Fatalf("state row read: %v", err)
	}
	var state ExecutionState
	if err := json.Unmarshal([]byte(rowRaw), &state); err != nil {
		t.Fatalf("state row decode: %v", err)
	}
	if state.Status != StatusRunning {
		t.Fatalf("status = %q with the budget disabled, want running (unbounded occupation preserved)", state.Status)
	}
}

// TestConfigExecutionDeadlineEnv: plaita#267's env contract — default 1800s,
// positive values win, and an invalid value (non-numeric / ≤0 / non-finite)
// falls back to the default instead of degrading to "unlimited". The
// deployment hook's explicit opt-out (negative) survives normalize.
func TestConfigExecutionDeadlineEnv(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"unset → default", "", DefaultExecutionDeadline},
		{"positive seconds", "3600", 3600 * time.Second},
		{"fractional seconds", "0.5", 500 * time.Millisecond},
		{"zero falls back (never unlimited)", "0", DefaultExecutionDeadline},
		{"negative falls back (never unlimited)", "-5", DefaultExecutionDeadline},
		{"non-numeric falls back", "abc", DefaultExecutionDeadline},
		{"inf falls back", "Inf", DefaultExecutionDeadline},
		{"whitespace tolerated", " 90 ", 90 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := configFromEnv(func(key string) string {
				if key == ExecutionDeadlineEnv {
					return tc.raw
				}
				return ""
			})
			if cfg.ExecutionDeadline != tc.want {
				t.Fatalf("ExecutionDeadline = %s, want %s (raw=%q)", cfg.ExecutionDeadline, tc.want, tc.raw)
			}
		})
	}
	// Hook opt-out: a literal Config gets the default on zero and keeps a
	// negative value through normalize.
	hook := &Config{}
	hook.normalize()
	if hook.ExecutionDeadline != DefaultExecutionDeadline {
		t.Fatalf("zero-value hook = %s, want default", hook.ExecutionDeadline)
	}
	optOut := &Config{ExecutionDeadline: -1}
	optOut.normalize()
	if optOut.ExecutionDeadline != -1 {
		t.Fatalf("negative hook = %s, want -1 (explicit opt-out preserved)", optOut.ExecutionDeadline)
	}
}
