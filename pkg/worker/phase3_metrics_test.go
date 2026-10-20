package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingMetrics captures every emission for assertions (#31: the key-path
// instrumentation is called, labels stay low-cardinality).
type recordingMetrics struct {
	mu         sync.Mutex
	counts     map[string]int
	lastLabels map[string]map[string]string
	gauges     map[string]float64
	durations  map[string][]time.Duration
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{
		counts:     map[string]int{},
		lastLabels: map[string]map[string]string{},
		gauges:     map[string]float64{},
		durations:  map[string][]time.Duration{},
	}
}

func (m *recordingMetrics) IncCounter(name string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[name]++
	m.lastLabels[name] = labels
}

func (m *recordingMetrics) SetGauge(name string, value float64, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name] = value
	m.lastLabels[name] = labels
}

func (m *recordingMetrics) ObserveDuration(name string, d time.Duration, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.durations[name] = append(m.durations[name], d)
	m.lastLabels[name] = labels
}

func (m *recordingMetrics) total(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[name]
}

func (m *recordingMetrics) labelOf(name, key string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastLabels[name][key]
}

func (m *recordingMetrics) observed(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.durations[name])
}

func (m *recordingMetrics) gauge(name string) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.gauges[name]
	return v, ok
}

// failOnceHTTPFlowTenant is failOnceHTTPFlow preset into a TENANT namespace
// (the flow store is tenant-partitioned like production).
func failOnceHTTPFlowTenant(t *testing.T, w *Worker, flowID, tenant string) *httptest.Server {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if calls.Add(1) == 1 {
			panic(http.ErrAbortHandler) // closes the connection: client sees EOF
		}
		rw.Header().Set("Content-Type", "application/json")
		_, _ = rw.Write([]byte(`{"ok":true}`))
	}))
	flow := fmt.Sprintf(`{
	  "flow_id": %q,
	  "nodes": [
	    {"type": "start", "id": "start", "next": "call"},
	    {"type": "http", "id": "call", "url": %q, "method": "GET", "next": "end"},
	    {"type": "end", "id": "end", "resultType": "success", "output": "done"}
	  ]
	}`, flowID, srv.URL+"/flaky")
	if err := w.flows.PresetFlowDefinition(context.Background(), tenant, flowID, "latest", []byte(flow)); err != nil {
		srv.Close()
		t.Fatal(err)
	}
	return srv
}

// TestMetricsNoopDefaultAndNilSafety: the default face is the no-op and a nil
// injection falls back to it — behavior is byte-identical (#31: "no-op 不影响
// 行为").
func TestMetricsNoopDefaultAndNilSafety(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	if _, ok := w.metrics.(NoopMetrics); !ok {
		t.Fatalf("default metrics = %T, want NoopMetrics", w.metrics)
	}
	w.SetMetrics(nil)
	if _, ok := w.metrics.(NoopMetrics); !ok {
		t.Fatalf("SetMetrics(nil) = %T, want NoopMetrics", w.metrics)
	}
	if _, ok := w.queue.metrics.(NoopMetrics); !ok {
		t.Fatalf("queue metrics = %T, want NoopMetrics", w.queue.metrics)
	}

	ctx := context.Background()
	presetHelloWorld(t, w)
	executionID := newExecutionID()
	enqueueTask(t, rdb, cfg, baseStartPayload(executionID), "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if state := loadStateRow(t, rdb, cfg, executionID); state.Status != StatusCompleted {
		t.Fatalf("status = %q, want completed (no-op metrics changed nothing)", state.Status)
	}
}

// TestMetricsKeyPathInstrumentation: one happy-path run emits the minimal
// metric set with low-cardinality labels only — never execution_id.
func TestMetricsKeyPathInstrumentation(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	presetHelloWorldTenant(t, w, "acme")

	executionID := newExecutionID()
	payload := baseStartPayload(executionID)
	payload["tenant_id"] = "acme"
	enqueueTask(t, rdb, cfg, payload, "")
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}

	if got := rec.total(MetricLeaseAcquired); got < 1 {
		t.Fatalf("lease_acquired = %d, want >= 1", got)
	}
	if got := rec.labelOf(MetricLeaseAcquired, "tenant"); got != "acme" {
		t.Fatalf("lease_acquired tenant label = %q, want acme", got)
	}
	if got := rec.total(MetricExecResult); got != 1 {
		t.Fatalf("exec_result = %d, want 1", got)
	}
	if got := rec.labelOf(MetricExecResult, "status"); got != "completed" {
		t.Fatalf("exec_result status = %q, want completed", got)
	}
	if got := rec.labelOf(MetricExecResult, "flow"); got != "hello_world_go" {
		t.Fatalf("exec_result flow label = %q, want hello_world_go", got)
	}
	if got := rec.observed(MetricExecDuration); got != 1 {
		t.Fatalf("exec_duration observations = %d, want 1", got)
	}
	if got := rec.total(MetricDLQTotal); got != 0 {
		t.Fatalf("dlq_total = %d, want 0 on a clean run", got)
	}
	// Cardinality contract: no metric may carry an execution_id label.
	rec.mu.Lock()
	for name, labels := range rec.lastLabels {
		if _, bad := labels["execution_id"]; bad {
			t.Errorf("metric %s carries an execution_id label (forbidden)", name)
		}
	}
	rec.mu.Unlock()
}

// TestMetricsQueueDepthGauge: the consumer loop samples queue_depth.
func TestMetricsQueueDepthGauge(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	presetHelloWorld(t, w)

	// Two backlogged tasks so the gauge has a non-zero value to read.
	for i := 0; i < 2; i++ {
		enqueueTask(t, rdb, cfg, baseStartPayload(newExecutionID()), "")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	if !waitFor(t, 5*time.Second, func() bool {
		v, ok := rec.gauge(MetricQueueDepth)
		return ok && v >= 0
	}) {
		cancel()
		<-done
		t.Fatal("queue_depth gauge never sampled")
	}
	cancel()
	<-done
}

// TestMetricsDLQCounter: every guarded/plain dead-letter bumps dlq_total with
// its reason label.
func TestMetricsDLQCounter(t *testing.T) {
	w, rdb, _, cfg := newTestWorker(t)
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	if err := rdb.XAdd(context.Background(), redisXAddArgs(cfg, "not-json", "1")).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ProcessOne(context.Background()); err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if got := rec.total(MetricDLQTotal); got != 1 {
		t.Fatalf("dlq_total = %d, want 1", got)
	}
	if got := rec.labelOf(MetricDLQTotal, "reason"); got != ReasonInvalidPayload {
		t.Fatalf("dlq reason = %q, want %q", got, ReasonInvalidPayload)
	}
}

// TestMetricsRetryAndWakeCounters: a retryable node failure bumps retry_total
// on the successful re-enqueue, and a decision carrier records its wake
// latency (both share failOnceHTTPFlow's retry machinery; the wake latency
// path uses a continue carrier carrying the enqueue timestamp).
func TestMetricsRetryAndWakeCounters(t *testing.T) {
	oldBase := nodeRetryBackoffBase
	nodeRetryBackoffBase = 0
	defer func() { nodeRetryBackoffBase = oldBase }()

	w, rdb, _, cfg := newTestWorker(t)
	ctx := context.Background()
	rec := newRecordingMetrics()
	w.SetMetrics(rec)
	srv := failOnceHTTPFlowTenant(t, w, "retry_metrics_flow", "acme")
	defer srv.Close()

	payload := baseStartPayload(newExecutionID())
	payload["flow_id"] = "retry_metrics_flow"
	payload["tenant_id"] = "acme"
	enqueueTask(t, rdb, cfg, payload, "")

	// Attempt 1: retryable failure → requeue (+retry_total), exec_result=failed.
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("first ProcessOne: %v", err)
	}
	if got := rec.total(MetricRetryTotal); got != 1 {
		t.Fatalf("retry_total = %d, want 1", got)
	}
	if got := rec.labelOf(MetricRetryTotal, "tenant"); got != "acme" {
		t.Fatalf("retry_total tenant = %q, want acme", got)
	}
	if got := rec.labelOf(MetricExecResult, "status"); got != "failed" {
		t.Fatalf("exec_result status after retryable failure = %q, want failed", got)
	}
	if got := rec.total(MetricLeaseLost); got != 0 {
		t.Fatalf("lease_lost = %d, want 0", got)
	}

	// Attempt 2: the converted continue carrier completes and reports the
	// wake latency (decision/continue carrier → event_wake_latency observed).
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatalf("second ProcessOne: %v", err)
	}
	if got := rec.observed(MetricEventWakeLatency); got != 1 {
		t.Fatalf("event_wake_latency observations = %d, want 1 (resume carrier)", got)
	}
	if got := rec.labelOf(MetricExecResult, "status"); got != "completed" {
		t.Fatalf("exec_result status = %q, want completed", got)
	}
}
