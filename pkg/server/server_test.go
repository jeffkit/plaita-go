package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

// ---- harness ----

// serverConfig is a production-shaped namespace (plaita / default tenant) so
// seeded keys read exactly like a real deployment. The base comes from
// worker.DefaultConfig() so every knob is normalized (unnormalized zero
// values like LeaseTTLSeconds=0 break the worker's lease Lua); the derived
// names are then pinned so the test stays hermetic against PLAITA_* env.
func serverConfig() *worker.Config {
	cfg := worker.DefaultConfig()
	cfg.KeyPrefix = "plaita"
	cfg.DefaultTenantID = "default"
	cfg.QueueName = "plaita:flow:queue"
	cfg.ConsumerGroup = "plaita-workers"
	cfg.DLQKey = cfg.QueueName + ":dlq"
	cfg.DispatchLedgerKey = "plaita:dispatch:ledger"
	return cfg
}

type testHarness struct {
	mr      *miniredis.Miniredis
	rdb     *redis.Client
	srv     *Server
	handler http.Handler
	store   *worker.ExecutionStore
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	srv := New(serverConfig(), rdb, WithLogger(slog.New(slog.NewTextHandler(&strings.Builder{}, &slog.HandlerOptions{Level: slog.LevelError}))))
	return &testHarness{
		mr:      mr,
		rdb:     rdb,
		srv:     srv,
		handler: srv.Handler(),
		store:   worker.NewExecutionStore(rdb, serverConfig()),
	}
}

// seedExecution writes one execution state row through the worker store
// (plain write path: SET + ZADD index), exactly as the worker does.
func (h *testHarness) seedExecution(t *testing.T, tenant, id, flowID, status, startTime string, mutate func(*worker.ExecutionState)) *worker.ExecutionState {
	t.Helper()
	state := &worker.ExecutionState{
		ExecutionID:    id,
		FlowID:         flowID,
		FlowName:       "Flow " + flowID,
		FlowVersion:    "v3",
		TenantID:       tenant,
		Status:         status,
		StartTime:      startTime,
		LastUpdateTime: "2026-10-20T10:00:00.000000",
		Context:        map[string]any{"$INPUT": map[string]any{"q": id}},
		Invoker:        "console",
	}
	if worker.TerminalStatuses[status] {
		state.EndTime = "2026-10-20T10:05:00.000000"
		state.Error = map[string]any{"message": "boom", "code": float64(-520)}
	}
	if mutate != nil {
		mutate(state)
	}
	if err := h.store.SaveExecutionState(context.Background(), tenant, state, 0); err != nil {
		t.Fatalf("seed execution: %v", err)
	}
	return state
}

// timelineOf builds a §5.1-shaped timeline dict with seq 1..n.
func timelineOf(n int) map[string]any {
	events := make([]map[string]any, 0, n)
	for i := 1; i <= n; i++ {
		typ := "node_start"
		extra := map[string]any{"node_id": fmt.Sprintf("n%d", i), "node_type": "assignment"}
		if i%2 == 0 {
			typ = "node_end"
			extra["failed"] = false
		}
		ev := map[string]any{
			"seq":   i,
			"type":  typ,
			"at":    fmt.Sprintf("2026-10-20T10:00:%02d.000000", i),
			"at_ms": 1760000000000 + int64(i)*1000,
		}
		for k, v := range extra {
			ev[k] = v
		}
		events = append(events, ev)
	}
	return map[string]any{
		"events":        events,
		"dropped":       2,
		"max_events":    500,
		"enqueued_at":   "2026-10-20T09:59:00.000000",
		"queue_wait_ms": float64(1200),
	}
}

func (h *testHarness) do(t *testing.T, method, target string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func (h *testHarness) getJSON(t *testing.T, target string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	rec := h.do(t, http.MethodGet, target, headers)
	if rec.Code != http.StatusOK {
		return rec.Code, map[string]any{"_raw": rec.Body.String()}
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v: %s", err, rec.Body.String())
	}
	return rec.Code, body
}

func keysOf(t *testing.T, m map[string]any) []string {
	t.Helper()
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func sameStringSet(t *testing.T, got []string, want ...string) {
	t.Helper()
	set := map[string]bool{}
	for _, k := range got {
		set[k] = true
	}
	missing := []string{}
	for _, k := range want {
		if !set[k] {
			missing = append(missing, k)
		}
		delete(set, k)
	}
	if len(missing) > 0 || len(set) > 0 {
		extra := []string{}
		for k := range set {
			extra = append(extra, k)
		}
		t.Fatalf("key set mismatch: missing=%v extra=%v", missing, extra)
	}
}

// ---- detail endpoint ----

func TestGetExecutionDetailReturnsStateFlowTenantTimeline(t *testing.T) {
	h := newHarness(t)
	h.seedExecution(t, "", "exec-1", "router", worker.StatusCompleted, "2026-10-20T10:00:00.000000",
		func(s *worker.ExecutionState) {
			s.NodeTimings = map[string]any{"n1": map[string]any{"started_at": "2026-10-20T10:00:01.000000", "attempts": 1}}
			s.Timeline = timelineOf(4)
			s.Usage = map[string]any{"source": "worker", "total_tokens": float64(42)}
		})

	code, body := h.getJSON(t, "/executions/exec-1", nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	sameStringSet(t, keysOf(t, body),
		// Python ExecutionInfo field set (api/executions.py)...
		"execution_id", "flow_id", "flow_version", "status", "tenant_id",
		"start_time", "end_time", "last_update_time", "context", "error",
		"invoker", "nodes", "output", "node_timings", "usage", "node_usage",
		"langfuse_trace_url",
		// ...flow_name (carried by the console summary, dropped by pydantic)...
		"flow_name",
		// ...and the Go-only embedded timeline (Python: /timeline endpoint).
		"timeline")
	if body["execution_id"] != "exec-1" || body["flow_id"] != "router" || body["status"] != "completed" {
		t.Fatalf("identity fields wrong: %v", body)
	}
	if body["tenant_id"] != "default" {
		t.Fatalf("tenant_id = %v, want default", body["tenant_id"])
	}
	tl, ok := body["timeline"].(map[string]any)
	if !ok {
		t.Fatalf("timeline missing or not an object: %v", body["timeline"])
	}
	events, _ := tl["events"].([]any)
	if len(events) != 4 {
		t.Fatalf("timeline events = %d, want 4", len(events))
	}
	if body["node_timings"] == nil || body["context"] == nil || body["usage"] == nil {
		t.Fatalf("detail must carry node_timings/context/usage: %v", body)
	}
	if body["nodes"] != nil || body["output"] != nil || body["node_usage"] != nil || body["langfuse_trace_url"] != nil {
		t.Fatalf("local-mode-only fields must stay null: %v", body)
	}
	// The unauthenticated default must be visible on the wire.
	rec := h.do(t, http.MethodGet, "/executions/exec-1", nil)
	if got := rec.Header().Get("X-Plaita-Auth"); got != "unauthenticated" {
		t.Fatalf("X-Plaita-Auth = %q, want unauthenticated", got)
	}
}

func TestGetExecutionNotFound(t *testing.T) {
	h := newHarness(t)
	rec := h.do(t, http.MethodGet, "/executions/missing", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not json: %v", err)
	}
	// Same detail format as the Python console.
	if body["detail"] != "执行不存在: missing" {
		t.Fatalf("detail = %v", body["detail"])
	}
}

func TestGetExecutionCorruptStateIs500(t *testing.T) {
	h := newHarness(t)
	if err := h.rdb.Set(context.Background(), "plaita:execution:bad", "{not json", 0).Err(); err != nil {
		t.Fatal(err)
	}
	rec := h.do(t, http.MethodGet, "/executions/bad", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (corrupt state is never swallowed as missing)", rec.Code)
	}
}

// ---- list endpoint ----

func TestListExecutionsOrderPagingTotal(t *testing.T) {
	h := newHarness(t)
	// Seed out of order; the response must sort by start_time desc.
	h.seedExecution(t, "", "old", "flow-a", worker.StatusCompleted, "2026-10-19T08:00:00.000000", nil)
	h.seedExecution(t, "", "new", "flow-a", worker.StatusRunning, "2026-10-20T09:00:00.000000", nil)
	h.seedExecution(t, "", "mid", "flow-b", worker.StatusError, "2026-10-19T18:00:00.000000", nil)

	code, body := h.getJSON(t, "/executions", nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	sameStringSet(t, keysOf(t, body), "executions", "total", "page", "size", "truncated")
	if body["total"] != float64(3) || body["page"] != float64(1) || body["size"] != float64(20) || body["truncated"] != false {
		t.Fatalf("list envelope wrong: %v", body)
	}
	rows, _ := body["executions"].([]any)
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	wantOrder := []string{"new", "mid", "old"}
	for i, id := range wantOrder {
		row, _ := rows[i].(map[string]any)
		if row["execution_id"] != id {
			t.Fatalf("row %d = %v, want %s", i, row["execution_id"], id)
		}
		// List rows carry the summary projection only — context/error/node
		// data stay null (console posture: full context out of list memory).
		sameStringSet(t, keysOf(t, row),
			"execution_id", "flow_id", "flow_name", "flow_version", "status", "tenant_id",
			"start_time", "end_time", "last_update_time", "invoker", "usage",
			"context", "error", "nodes", "output", "node_timings", "node_usage", "langfuse_trace_url")
		if row["context"] != nil || row["error"] != nil || row["node_timings"] != nil {
			t.Fatalf("list row must not carry full state: %v", row)
		}
	}
	// Page 2 with size=2 → only the oldest row.
	code, body = h.getJSON(t, "/executions?page=2&size=2", nil)
	if code != http.StatusOK {
		t.Fatalf("page2 status = %d", code)
	}
	rows, _ = body["executions"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["execution_id"] != "old" {
		t.Fatalf("page 2 rows = %v", rows)
	}
}

func TestListExecutionsFilters(t *testing.T) {
	h := newHarness(t)
	h.seedExecution(t, "", "r1", "flow-a", worker.StatusRunning, "2026-10-20T09:00:00.000000", nil)
	h.seedExecution(t, "", "r2", "flow-b", worker.StatusCompleted, "2026-10-19T09:00:00.000000", nil)
	h.seedExecution(t, "", "r3", "flow-b", worker.StatusCompleted, "2026-10-18T09:00:00.000000", nil)

	_, body := h.getJSON(t, "/executions?status=completed", nil)
	rows, _ := body["executions"].([]any)
	if len(rows) != 2 || body["total"] != float64(2) {
		t.Fatalf("status filter rows = %d total = %v", len(rows), body["total"])
	}
	for _, raw := range rows {
		if raw.(map[string]any)["status"] != "completed" {
			t.Fatalf("filter leaked: %v", raw)
		}
	}
	_, body = h.getJSON(t, "/executions?flow_id=flow-b", nil)
	if body["total"] != float64(2) {
		t.Fatalf("flow_id filter total = %v", body["total"])
	}
	// Issue-#33 spelling `flow=` is an alias of flow_id.
	_, body = h.getJSON(t, "/executions?flow=flow-a", nil)
	if body["total"] != float64(1) {
		t.Fatalf("flow alias total = %v", body["total"])
	}
	_, body = h.getJSON(t, "/executions?status=completed&flow_id=flow-a", nil)
	if body["total"] != float64(0) || len(body["executions"].([]any)) != 0 {
		t.Fatalf("combined filter should be empty: %v", body)
	}
}

func TestListExecutionsValidationAndTruncation(t *testing.T) {
	h := newHarness(t)
	for _, q := range []string{"?size=201", "?size=0", "?page=0", "?page=10001", "?page=abc", "?size=xx"} {
		rec := h.do(t, http.MethodGet, "/executions"+q, nil)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%s status = %d, want 422", q, rec.Code)
		}
	}
	// Candidate cap: shrink it so truncation is observable.
	h.srv.filterCap = 2
	h.seedExecution(t, "", "a", "f", worker.StatusRunning, "2026-10-20T09:00:00.000000", nil)
	h.seedExecution(t, "", "b", "f", worker.StatusRunning, "2026-10-19T09:00:00.000000", nil)
	h.seedExecution(t, "", "c", "f", worker.StatusRunning, "2026-10-18T09:00:00.000000", nil)
	_, body := h.getJSON(t, "/executions?status=running", nil)
	if body["truncated"] != true {
		t.Fatalf("truncated = %v, want true (3 candidates > cap 2)", body["truncated"])
	}
	if body["total"] != float64(2) {
		t.Fatalf("filtered total = %v, want matched lower bound 2", body["total"])
	}
}

// ---- events endpoint ----

func TestEventsPaginationAndIncremental(t *testing.T) {
	h := newHarness(t)
	h.seedExecution(t, "", "ev-1", "flow-x", worker.StatusSuspended, "2026-10-20T10:00:00.000000",
		func(s *worker.ExecutionState) { s.Timeline = timelineOf(5) })

	// Page 1: limit=2 → events 1..2, has_more, cursor 2.
	code, body := h.getJSON(t, "/executions/ev-1/events?limit=2", nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	sameStringSet(t, keysOf(t, body),
		// Python _timeline_envelope fields...
		"execution_id", "status", "source", "collected", "events", "dropped",
		"max_events", "enqueued_at", "queue_wait_ms", "start_time", "end_time",
		// ...plus the #33 incremental cursor fields.
		"after_seq", "next_after_seq", "has_more")
	if body["collected"] != true || body["source"] != "state" {
		t.Fatalf("envelope flags wrong: %v", body)
	}
	if body["dropped"] != float64(2) || body["max_events"] != float64(500) || body["queue_wait_ms"] != float64(1200) {
		t.Fatalf("envelope passthrough wrong: %v", body)
	}
	if body["status"] != "suspended" || body["execution_id"] != "ev-1" {
		t.Fatalf("envelope identity wrong: %v", body)
	}
	events := body["events"].([]any)
	if len(events) != 2 {
		t.Fatalf("page events = %d, want 2", len(events))
	}
	if body["next_after_seq"] != float64(2) || body["has_more"] != true || body["after_seq"] != float64(0) {
		t.Fatalf("cursor fields wrong: %v", body)
	}

	// Incremental: after_seq=2 → events 3..4 (limit 2), still has_more.
	_, body = h.getJSON(t, "/executions/ev-1/events?after_seq=2&limit=2", nil)
	events = body["events"].([]any)
	if len(events) != 2 || events[0].(map[string]any)["seq"] != float64(3) {
		t.Fatalf("incremental page wrong: %v", events)
	}
	if body["next_after_seq"] != float64(4) || body["has_more"] != true {
		t.Fatalf("incremental cursor wrong: %v", body)
	}

	// Drain: after_seq=4 → the last event, has_more=false.
	_, body = h.getJSON(t, "/executions/ev-1/events?after_seq=4", nil)
	events = body["events"].([]any)
	if len(events) != 1 || body["has_more"] != false || body["next_after_seq"] != float64(5) {
		t.Fatalf("drain page wrong: has_more=%v next=%v events=%v", body["has_more"], body["next_after_seq"], events)
	}

	// Cursor past the end → empty page, no error.
	_, body = h.getJSON(t, "/executions/ev-1/events?after_seq=99", nil)
	if len(body["events"].([]any)) != 0 {
		t.Fatalf("past-end page should be empty: %v", body["events"])
	}

	// Invalid params → 422 (FastAPI Query parity).
	for _, q := range []string{"?limit=0", "?limit=501", "?after_seq=-1", "?limit=zz"} {
		if rec := h.do(t, http.MethodGet, "/executions/ev-1/events"+q, nil); rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%s status = %d, want 422", q, rec.Code)
		}
	}
}

func TestEventsUncollectedTimeline(t *testing.T) {
	h := newHarness(t)
	h.seedExecution(t, "", "old-state", "flow-x", worker.StatusCompleted, "2026-10-20T10:00:00.000000", nil)
	_, body := h.getJSON(t, "/executions/old-state/events", nil)
	// collected=false = "never collected", NOT "empty log"; events still [].
	if body["collected"] != false {
		t.Fatalf("collected = %v, want false", body["collected"])
	}
	if events, ok := body["events"].([]any); !ok || len(events) != 0 {
		t.Fatalf("events = %v, want empty array", body["events"])
	}
}

func TestEventsMissingExecution404(t *testing.T) {
	h := newHarness(t)
	rec := h.do(t, http.MethodGet, "/executions/nope/events", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

// ---- tenant isolation ----

func TestTenantIsolation(t *testing.T) {
	h := newHarness(t)
	h.seedExecution(t, "t1", "iso-1", "flow-a", worker.StatusRunning, "2026-10-20T09:00:00.000000", nil)
	h.seedExecution(t, "t2", "iso-2", "flow-a", worker.StatusRunning, "2026-10-20T09:00:00.000000", nil)

	// Tenant t1 cannot see t2's execution (and vice versa): the read is
	// routed through {prefix}:{tenant}:execution:{id} only.
	rec := h.do(t, http.MethodGet, "/executions/iso-2", map[string]string{TenantHeader: "t1"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("t1 reading t2 execution: status = %d, want 404", rec.Code)
	}
	rec = h.do(t, http.MethodGet, "/executions/iso-2/events", map[string]string{TenantHeader: "t1"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("t1 reading t2 events: status = %d, want 404", rec.Code)
	}
	rec = h.do(t, http.MethodGet, "/executions/iso-1", map[string]string{TenantHeader: "t1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("t1 reading own execution: status = %d", rec.Code)
	}

	// List is namespace-scoped too: t1's list never contains t2 rows.
	_, body := h.getJSON(t, "/executions", map[string]string{TenantHeader: "t1"})
	if body["total"] != float64(1) {
		t.Fatalf("t1 list total = %v, want 1", body["total"])
	}
	rows := body["executions"].([]any)
	row := rows[0].(map[string]any)
	if row["execution_id"] != "iso-1" || row["tenant_id"] != "t1" {
		t.Fatalf("t1 list row wrong: %v", row)
	}
	// Default scope (no header) sees only the default namespace.
	_, body = h.getJSON(t, "/executions", nil)
	if body["total"] != float64(0) {
		t.Fatalf("default list total = %v, want 0", body["total"])
	}
}

// ---- auth injection ----

func TestCustomAuthenticatorInjection(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	var sawPath string
	auth := AuthenticatorFunc(func(r *http.Request) (*Identity, error) {
		sawPath = r.URL.Path
		token := r.Header.Get("Authorization")
		if token != "Bearer ok" {
			return nil, fmt.Errorf("bad token")
		}
		return &Identity{TenantID: "acme", Subject: "user-7"}, nil
	})
	srv := New(serverConfig(), rdb, WithAuthenticator(auth))
	h := &testHarness{srv: srv, handler: srv.Handler(), rdb: rdb,
		store: worker.NewExecutionStore(rdb, serverConfig())}
	h.seedExecution(t, "acme", "a-1", "flow-a", worker.StatusRunning, "2026-10-20T09:00:00.000000", nil)

	// Rejected → 401 {"detail": ...}.
	rec := h.do(t, http.MethodGet, "/executions/a-1", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", rec.Code)
	}
	// Accepted → tenant scope comes from the identity, not any header.
	rec = h.do(t, http.MethodGet, "/executions/a-1", map[string]string{"Authorization": "Bearer ok"})
	if rec.Code != http.StatusOK {
		t.Fatalf("authenticated status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Plaita-Auth"); got != "authenticated" {
		t.Fatalf("X-Plaita-Auth = %q, want authenticated", got)
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["tenant_id"] != "acme" {
		t.Fatalf("tenant_id = %v, want acme", body["tenant_id"])
	}
	if sawPath != "/executions/a-1" {
		t.Fatalf("authenticator did not see the request: %q", sawPath)
	}
}

// ---- end-to-end against a real worker run ----

// TestServerReadsRealWorkerExecution is the reuse-surface contract test:
// a hello-world flow runs through the actual worker (start carrier →
// ProcessOne → terminal write, §5.2 double-write + index) and the BFF reads
// the row back — no seeding shortcuts, no second storage path.
func TestServerReadsRealWorkerExecution(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := serverConfig()
	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, &slog.HandlerOptions{Level: slog.LevelError}))

	// Seed the flow definition and run one real execution.
	raw, err := os.ReadFile("../ir/testdata/hello_world.json")
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	flows := worker.NewFlowSource(rdb, cfg, logger)
	if err := flows.PresetFlowDefinition(context.Background(), "", "hello_world_go", "latest", raw); err != nil {
		t.Fatalf("preset flow: %v", err)
	}
	executionID := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	payload, _ := json.Marshal(map[string]any{
		"type":         "start",
		"flow_id":      "hello_world_go",
		"execution_id": executionID,
		"params":       map[string]any{"who": "server-e2e"},
		"tenant_id":    "",
		"timestamp":    "2026-10-20T10:00:00.000000",
	})
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.QueueName,
		Values: map[string]any{"payload": string(payload), "schema_version": "1"},
	}).Err(); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	w := worker.New(cfg, rdb, logger)
	if _, err := w.ProcessOne(context.Background()); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	h := &testHarness{
		mr:      mr,
		rdb:     rdb,
		srv:     New(cfg, rdb, WithLogger(logger)),
		handler: New(cfg, rdb, WithLogger(logger)).Handler(),
		store:   worker.NewExecutionStore(rdb, cfg),
	}

	// Detail: terminal row with the worker's own timeline/node timings.
	code, body := h.getJSON(t, "/executions/"+executionID, nil)
	if code != http.StatusOK {
		t.Fatalf("detail status = %d: %s", code, body["_raw"])
	}
	if body["status"] != "completed" || body["flow_id"] != "hello_world_go" || body["invoker"] != "worker" {
		t.Fatalf("detail identity wrong: %v", body)
	}
	tl, _ := body["timeline"].(map[string]any)
	events, _ := tl["events"].([]any)
	if len(events) < 6 {
		t.Fatalf("worker timeline missing: %v", tl)
	}
	if first := events[0].(map[string]any); first["type"] != "run_start" {
		t.Fatalf("first timeline event = %v, want run_start", first)
	}
	if last := events[len(events)-1].(map[string]any); last["type"] != "run_end" || last["final"] != true {
		t.Fatalf("last timeline event = %v, want final run_end", last)
	}
	if _, ok := body["node_timings"].(map[string]any); !ok {
		t.Fatalf("node_timings missing: %v", body["node_timings"])
	}

	// Events endpoint over the real timeline: incremental cursor walks it.
	_, evBody := h.getJSON(t, "/executions/"+executionID+"/events?after_seq=1&limit=3", nil)
	page := evBody["events"].([]any)
	if len(page) != 3 {
		t.Fatalf("incremental page = %d events, want 3", len(page))
	}
	if evBody["has_more"] != true || evBody["next_after_seq"] != float64(4) {
		t.Fatalf("cursor wrong: has_more=%v next=%v", evBody["has_more"], evBody["next_after_seq"])
	}

	// List with terminal filter finds the run; the index was written by the
	// worker's own save path.
	_, listBody := h.getJSON(t, "/executions?status=completed&flow_id=hello_world_go", nil)
	if total := listBody["total"].(float64); total != 1 {
		t.Fatalf("filtered list total = %v, want 1", listBody["total"])
	}
}

// ---- read-only by construction ----

func TestReadOnlyNoWriteEndpoints(t *testing.T) {
	h := newHarness(t)
	h.seedExecution(t, "", "ro-1", "flow-a", worker.StatusRunning, "2026-10-20T09:00:00.000000", nil)
	before := h.mr.Keys()
	// The three read routes accept GET only — every other verb is 405.
	readPaths := []string{
		"/executions",
		"/executions/ro-1",
		"/executions/ro-1/events",
	}
	for _, path := range readPaths {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			rec := h.do(t, method, path, nil)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s = %d, want 405", method, path, rec.Code)
			}
		}
	}
	// The write endpoints the console has (cancel/resume/start) do not exist
	// here at all — any verb lands on 404, and nothing in Redis moves.
	writePaths := []string{
		"/executions/ro-1/cancel",
		"/executions/ro-1/resume",
		"/executions/ro-1/retry",
	}
	for _, path := range writePaths {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			rec := h.do(t, method, path, nil)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s %s = %d, want 404 (write endpoints must not exist)", method, path, rec.Code)
			}
		}
	}
	after := h.mr.Keys()
	if len(before) != len(after) {
		t.Fatalf("write attempts changed the keyspace: %v -> %v", before, after)
	}
}
