package worker

import "time"

// Runtime metrics face (issue #31). The worker calls the interface at its key
// path points — queue depth sampling, lease acquire/loss, execution
// duration/outcome, retry re-enqueue, dead-letter, event wake latency, task
// age-gate drops (#29) and tenant-quota hit/defer (#30). The default
// implementation is a no-op, so behavior is unchanged until a host injects
// one (Prometheus / OTel adapter) via Worker.SetMetrics.
//
// Cardinality contract: labels carry ONLY low-cardinality dimensions —
// tenant, flow, status, reason, node, resume_type, type, queue. execution_id
// is FORBIDDEN as a label (unbounded); hosts may rely on that for label
// pre-registration.
type Metrics interface {
	// IncCounter increments a monotonic counter by one.
	IncCounter(name string, labels map[string]string)
	// SetGauge stores a point-in-time value (last write wins).
	SetGauge(name string, value float64, labels map[string]string)
	// ObserveDuration records one duration sample (histogram-like).
	ObserveDuration(name string, d time.Duration, labels map[string]string)
}

// NoopMetrics is the default Metrics: every call is discarded. It exists so
// the nil check happens once at injection, never on the hot path.
type NoopMetrics struct{}

func (NoopMetrics) IncCounter(string, map[string]string)                     {}
func (NoopMetrics) SetGauge(string, float64, map[string]string)              {}
func (NoopMetrics) ObserveDuration(string, time.Duration, map[string]string) {}

// Metric names emitted by this build. Names are stable identifiers — hosts
// may map them 1:1 onto Prometheus/OTel instruments.
const (
	MetricQueueDepth       = "queue_depth"
	MetricLeaseAcquired    = "lease_acquired"
	MetricLeaseLost        = "lease_lost"
	MetricExecDuration     = "exec_duration"
	MetricExecResult       = "exec_result"
	MetricRetryTotal       = "retry_total"
	MetricDLQTotal         = "dlq_total"
	MetricEventWakeLatency = "event_wake_latency"

	// Wave-3 companions.
	MetricTaskExpiredTotal    = "task_expired_total"     // #29: age-gated carriers
	MetricTaskAgeUnknownTotal = "task_age_unknown_total" // #29: fail-closed drops (age undeterminable)
	MetricTaskDedupHitTotal   = "task_dedup_hit_total"   // #29: duplicate-start idempotent acks
	MetricQuotaExceededTotal  = "tenant_quota_exceeded_total"
	MetricQuotaDeferredTotal  = "tenant_quota_deferred_total"
	MetricQuotaBypassedTotal  = "tenant_quota_bypassed_total"
	MetricQuotaRunning        = "tenant_quota_running" // gauge: admitted starts per tenant
)

// metricLabels builds a label map from k/v pairs, dropping empty values so
// absent dimensions never widen the label space. Returns nil for no labels.
func metricLabels(kv ...string) map[string]string {
	if len(kv) == 0 {
		return nil
	}
	m := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] != "" {
			m[kv[i]] = kv[i+1]
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}
