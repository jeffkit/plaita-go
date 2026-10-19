package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestDrainStopsConsumingAndAbandonsInflight covers D1 + D2 end to end: the
// registry flips to draining immediately, the in-flight step is cancelled at
// the drain deadline, no terminal state is written and the message stays
// pending for another worker.
func TestDrainStopsConsumingAndAbandonsInflight(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg.DrainTimeout = 300 * time.Millisecond
	cfg.ReadBlockMS = 100
	_, arrived, cleanup := blockingHTTPFlow(t, w, "drain_flow")
	defer cleanup()

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "drain_flow"

	runDone := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(runDone)
	}()
	// Wait for the registry to reach running before enqueuing.
	if !waitFor(t, 5*time.Second, func() bool {
		raw := rdb.Get(ctx, cfg.KeyPrefix+":registry:flow_worker:"+cfg.InstanceID).Val()
		return raw != "" && containsStatus(raw, RegStatusRunning)
	}) {
		t.Fatal("worker registry never reached running")
	}
	enqueueTask(t, rdb, cfg, payload, "")

	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("http node never started")
	}
	if !waitFor(t, 5*time.Second, func() bool {
		return containsStatus(rdb.Get(ctx, stateKeyOf(cfg, executionID)).Val(), StatusRunning)
	}) {
		t.Fatal("running state row missing")
	}

	w.RequestDrain("test")
	// D1: the registry status is flushed immediately (not on the next 10s
	// heartbeat), so dispatchers stop routing here.
	var info ServiceInfo
	if err := json.Unmarshal([]byte(rdb.Get(ctx, cfg.KeyPrefix+":registry:flow_worker:"+cfg.InstanceID).Val()), &info); err != nil {
		t.Fatalf("registry decode: %v", err)
	}
	if info.Status != RegStatusDraining {
		t.Fatalf("registry status = %q, want draining", info.Status)
	}
	if !w.Draining() {
		t.Fatal("worker did not enter draining")
	}

	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("run loop did not exit after the drain deadline")
	}

	// D2: the step was abandoned — no terminal state, message still pending.
	state := loadStateRow(t, rdb, cfg, executionID)
	if state.Status != StatusRunning {
		t.Fatalf("status = %q, want running (drain must not terminalize)", state.Status)
	}
	if state.Error != nil {
		t.Fatalf("drain-aborted run must carry no error, got %v", state.Error)
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1 (message left pending)", n)
	}
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("lease must be released, got %q", v)
	}

	// After drain the worker takes nothing new: this carrier is never
	// delivered nor acked.
	enqueueTask(t, rdb, cfg, baseStartPayload(newExecutionID()), "")
	time.Sleep(300 * time.Millisecond)
	if n := streamLen(t, rdb, cfg.QueueName); n != 2 {
		t.Fatalf("queue XLEN = %d, want 2 (abandoned + untouched new task)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1 (the new task was never read)", n)
	}
}

// TestDrainPostReadGate: a delivery that arrives after draining began is not
// dispatched nor acked (§9.3 D1 post-read gate).
func TestDrainPostReadGate(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.DrainTimeout = time.Hour
	defer w.stopDrainTimer()

	w.RequestDrain("test")
	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")

	handled, err := w.ProcessOne(ctx)
	if err != nil || !handled {
		t.Fatalf("ProcessOne = (%v,%v), want handled without error", handled, err)
	}
	if _, err := rdb.Get(ctx, stateKeyOf(cfg, executionID)).Result(); err == nil {
		t.Fatal("post-drain message must not be dispatched (no state row)")
	}
	if n := streamLen(t, rdb, cfg.QueueName); n != 1 {
		t.Fatalf("queue XLEN = %d, want 1 (not acked)", n)
	}
	if n := pendingCount(t, rdb, cfg); n != 1 {
		t.Fatalf("PEL = %d, want 1 (left pending)", n)
	}
}

// TestRequestDrainIdempotent: a second request never resets the drain start or
// the bounded-wait timer.
func TestRequestDrainIdempotent(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.DrainTimeout = time.Hour
	defer w.stopDrainTimer()

	w.RequestDrain("first")
	w.RequestDrain("second")
	if !w.Draining() || w.ForceStopped() {
		t.Fatalf("drain state = (draining=%v forced=%v), want (true,false)", w.Draining(), w.ForceStopped())
	}
	var info ServiceInfo
	if err := json.Unmarshal([]byte(rdb.Get(ctx, cfg.KeyPrefix+":registry:flow_worker:"+cfg.InstanceID).Val()), &info); err != nil {
		t.Fatalf("registry decode: %v", err)
	}
	if info.Status != RegStatusDraining {
		t.Fatalf("registry status = %q, want draining", info.Status)
	}
}

// TestForceStopInterruptsBackoff (A46-06): D2 abandons a retry backoff
// immediately instead of sleeping it out.
func TestForceStopInterruptsBackoff(t *testing.T) {
	w, _, _, cfg := newTestWorker(t)
	cfg.DrainTimeout = time.Hour
	defer w.stopDrainTimer()
	w.RequestDrain("test")

	result := make(chan bool, 1)
	go func() {
		result <- w.interruptibleSleep(context.Background(), 5*time.Second)
	}()
	time.Sleep(100 * time.Millisecond)
	w.ForceStop("test")
	select {
	case interrupted := <-result:
		if !interrupted {
			t.Fatal("interruptibleSleep must report the force-stop interruption")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("force-stop did not interrupt the backoff")
	}
	if !w.ForceStopped() {
		t.Fatal("worker must report force-stopped")
	}
}

func containsStatus(raw, status string) bool {
	return strings.Contains(raw, fmt.Sprintf("\"status\":%q", status))
}
