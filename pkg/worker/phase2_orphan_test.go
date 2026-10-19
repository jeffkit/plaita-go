package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// blockingHTTPFlow presets a two-node flow (start → http → end) whose http node
// blocks until the request context is cancelled; it returns the server and a
// channel that fires when the request reaches the handler.
func blockingHTTPFlow(t *testing.T, w *Worker, flowID string) (*httptest.Server, <-chan struct{}, func()) {
	t.Helper()
	arrived := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		select {
		case arrived <- struct{}{}:
		default:
		}
		<-req.Context().Done()
	}))
	flow := fmt.Sprintf(`{
	  "flow_id": %q,
	  "nodes": [
	    {"type": "start", "id": "start", "next": "call"},
	    {"type": "http", "id": "call", "url": %q, "method": "GET", "next": "end"},
	    {"type": "end", "id": "end", "resultType": "success", "output": "done"}
	  ]
	}`, flowID, srv.URL+"/block")
	if err := w.flows.PresetFlowDefinition(context.Background(), "", flowID, "latest", []byte(flow)); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	cleanup := func() { once.Do(srv.Close) }
	return srv, arrived, cleanup
}

// TestOrphanWatchPublishesProgressAfterLeaseLoss (§3.4 + §8.2): when the
// watchdog's renew fails, the worker publishes one progress heartbeat (so the
// keeper does not kill a still-running execution at 1800s), aborts the step,
// writes NO terminal state and leaves the lease alone. The orphan entry is
// dropped when the run ends.
func TestOrphanWatchPublishesProgressAfterLeaseLoss(t *testing.T) {
	w, rdb, mr, cfg := newTestWorker(t)
	ctx := context.Background()
	cfg.LeaseTTLSeconds = 1 // watchdog cadence floors at 1s
	_, arrived, cleanup := blockingHTTPFlow(t, w, "orphan_flow")
	defer cleanup()

	executionID := fmt.Sprintf("orphan-%d", time.Now().UnixNano())
	payload := baseStartPayload(executionID)
	payload["flow_id"] = "orphan_flow"
	enqueueTask(t, rdb, cfg, payload, "")

	done := make(chan error, 1)
	go func() {
		_, err := w.ProcessOne(ctx)
		done <- err
	}()

	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("http node never started")
	}
	if !waitFor(t, 5*time.Second, func() bool {
		raw := rdb.Get(ctx, stateKeyOf(cfg, executionID)).Val()
		return strings.Contains(raw, StatusRunning)
	}) {
		t.Fatal("running state row missing")
	}
	before := loadStateRow(t, rdb, cfg, executionID)

	// Expire the lease without fencing: the fence key keeps generation 1, so a
	// progress write is still allowed — exactly the orphan-watch window.
	mr.FastForward(2 * time.Second)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("lease loss must leave the message pending (non-nil error)")
		}
		if !strings.Contains(err.Error(), "lease") {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("processing did not stop after the lease loss")
	}

	after := loadStateRow(t, rdb, cfg, executionID)
	if after.Status != StatusRunning {
		t.Fatalf("orphan heartbeat must not write a terminal status, got %q", after.Status)
	}
	if after.LastUpdateTime == before.LastUpdateTime {
		t.Fatal("orphan progress heartbeat did not refresh last_update_time")
	}
	// The open node marker reached the row before the step was aborted (keeper
	// liveness judgment ①).
	timing, _ := after.NodeTimings["call"].(map[string]any)
	if timing == nil || timing["started_at"] == "" {
		t.Fatalf("node timing missing after orphan heartbeat: %v", after.NodeTimings)
	}
	if n := w.orphanCount(); n != 0 {
		t.Fatalf("orphan observation set = %d, want 0 after the run ended", n)
	}
	// The run never re-acquired or re-created the lease.
	if v := rdb.Get(ctx, cfg.KeyPrefix+":execution:lease:"+executionID).Val(); v != "" {
		t.Fatalf("lease must stay released, got %q", v)
	}
}

// TestOrphanWatchPublishesProgressPeriodically drives the observation set
// directly: heartbeats keep refreshing the row while the run advances, and the
// entry is dropped once the execution ends (bounded observation, plaita#197).
func TestOrphanWatchPublishesProgressPeriodically(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	executionID := newExecutionID()
	old := "2020-01-01T00:00:00.000000"
	rdb.Set(context.Background(), stateKeyOf(cfg, executionID),
		fmt.Sprintf(`{"execution_id":%q,"status":"running","last_update_time":%q}`, executionID, old), 0)

	run := &executionRun{
		owner:       w,
		executionID: executionID,
		nodeTimings: map[string]map[string]any{
			"call": {"started_at": nowISO(), "ended_at": ""},
		},
	}
	entry := &orphanEntry{executionID: executionID, run: run}
	w.registerOrphan(entry)
	if w.orphanCount() != 1 {
		t.Fatalf("orphan set = %d, want 1", w.orphanCount())
	}

	time.Sleep(2 * time.Millisecond)
	w.publishOrphans()
	if got := loadStateRow(t, rdb, cfg, executionID).LastUpdateTime; got == old {
		t.Fatal("periodic orphan heartbeat did not refresh last_update_time")
	}
	if entry.rounds != 1 {
		t.Fatalf("rounds = %d, want 1", entry.rounds)
	}
	// The open-node marker survives (keeper liveness judgment ①).
	timing, _ := loadStateRow(t, rdb, cfg, executionID).NodeTimings["call"].(map[string]any)
	if timing == nil || timing["ended_at"] != "" {
		t.Fatalf("open node marker lost: %v", timing)
	}

	// A finished run with no open node is no longer advancing → dropped.
	run.markFinished()
	run.mu.Lock()
	run.nodeTimings["call"]["ended_at"] = nowISO()
	run.mu.Unlock()
	w.publishOrphans()
	if w.orphanCount() != 0 {
		t.Fatalf("orphan set = %d, want 0 after the run ended", w.orphanCount())
	}
}
