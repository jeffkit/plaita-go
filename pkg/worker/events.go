package worker

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// Event persistence (protocol §6). Two distinct event families live here:
//
//  1. business events (§6.1 schema, §6.2 double-write + publish) — produced
//     by suspending nodes; none exist in this engine build yet, the publisher
//     is provided for protocol completeness;
//  2. execution timeline events (§6.2 worker-side) — node_start/node_end/
//     run_start/run_end merged into ExecutionState.timeline + node_timings,
//     and live-published on {prefix}:execution:events:{execution_id}.

// Event is the §6.1 schema (plaita/event/core.py:21-56).
type Event struct {
	EventID       string         `json:"event_id"`
	EventType     string         `json:"event_type"`
	Data          map[string]any `json:"data"`
	Timestamp     float64        `json:"timestamp"`
	Source        string         `json:"source,omitempty"`
	CorrelationID string         `json:"correlation_id,omitempty"`
	TenantID      string         `json:"tenant_id,omitempty"`
}

// EventPublisher writes business events (§6.2 Redis double-write):
// SETEX {ns}:event:events:{id} + ZADD {ns}:event:types:{type} + PUBLISH
// {ns}:events:{type}.
type EventPublisher struct {
	client *redis.Client
	cfg    *Config
}

func NewEventPublisher(client *redis.Client, cfg *Config) *EventPublisher {
	return &EventPublisher{client: client, cfg: cfg}
}

func (p *EventPublisher) eventsKey(tenantID, eventID string) string {
	return p.cfg.Namespace(tenantID) + ":event:events:" + eventID
}

func (p *EventPublisher) typesKey(tenantID, eventType string) string {
	return p.cfg.Namespace(tenantID) + ":event:types:" + eventType
}

func (p *EventPublisher) channel(tenantID, eventType string) string {
	return p.cfg.Namespace(tenantID) + ":events:" + eventType
}

// Publish performs the double-write + pub/sub notification.
func (p *EventPublisher) Publish(ctx context.Context, ev *Event) error {
	blob, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	pipe := p.client.Pipeline()
	pipe.SetEx(ctx, p.eventsKey(ev.TenantID, ev.EventID),
		string(blob), time.Duration(p.cfg.EventTTLSeconds)*time.Second)
	pipe.ZAdd(ctx, p.typesKey(ev.TenantID, ev.EventType),
		redis.Z{Score: ev.Timestamp, Member: ev.EventID})
	pipe.Expire(ctx, p.typesKey(ev.TenantID, ev.EventType),
		time.Duration(p.cfg.EventTTLSeconds)*time.Second)
	pipe.Publish(ctx, p.channel(ev.TenantID, ev.EventType), string(blob))
	_, err = pipe.Exec(ctx)
	return err
}

// Timeline cap (§5.1: cap 500 events, drop oldest, count dropped).
const timelineMaxEvents = 500

// executionRun collects the per-execution timeline + node timings fed by the
// engine event callbacks (pkg/workflow/event.go). It is owned by one
// execution; the global listeners fan in through the run pointer carried on
// the execution context.
type executionRun struct {
	owner       *Worker
	executionID string
	tenantID    string
	rootFlow    *workflow.Workflow

	mu          sync.Mutex
	seq         int64
	events      []map[string]any
	dropped     int
	enqueuedAt  string
	queueWaitMS *float64
	nodeTimings map[string]map[string]any
	failedNode  string
	nodeRetries int
	cancelHit   bool
	finished    bool
	rootRuntime any // root flow's runtime context (captured on flow end)

	// saveMu serializes state saves (progress heartbeat vs terminal write)
	// so a heartbeat save can never land after the terminal row.
	saveMu sync.Mutex
}

// finalize marks the run finished and performs the terminal state save under
// the save lock, so a racing progress-heartbeat save can never overwrite the
// terminal row.
func (r *executionRun) finalize(save func() error) error {
	if r == nil {
		return save()
	}
	r.mu.Lock()
	r.finished = true
	r.mu.Unlock()
	r.saveMu.Lock()
	defer r.saveMu.Unlock()
	return save()
}

// heartbeatSave performs a progress-heartbeat save unless the run already
// finished (§8.1 layer 3: best-effort, failures only warn). The finished
// check + save are serialized against finalize through saveMu.
func (r *executionRun) heartbeatSave(save func() error) error {
	if r == nil {
		return nil
	}
	r.saveMu.Lock()
	defer r.saveMu.Unlock()
	r.mu.Lock()
	fin := r.finished
	r.mu.Unlock()
	if fin {
		return nil
	}
	return save()
}

// recordTimeline appends one timeline event with seq/at/at_ms and caps the
// buffer (§5.1 timeline shape: {events, dropped, max_events, enqueued_at,
// queue_wait_ms}). Returns the stored event for live publishing.
func (r *executionRun) recordTimeline(typ string, extra map[string]any) map[string]any {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recordTimelineLocked(typ, extra)
}

// snapshotTimeline builds the persisted timeline dict (§5.1).
func (r *executionRun) snapshotTimeline() map[string]any {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	events := make([]map[string]any, len(r.events))
	copy(events, r.events)
	out := map[string]any{
		"events":     events,
		"dropped":    r.dropped,
		"max_events": timelineMaxEvents,
	}
	if r.enqueuedAt != "" {
		out["enqueued_at"] = r.enqueuedAt
	}
	if r.queueWaitMS != nil {
		out["queue_wait_ms"] = *r.queueWaitMS
	}
	return out
}

// snapshotNodeTimings builds the persisted node_timings dict (§5.1: a
// running node carries ended_at="" — the keeper liveness protocol).
func (r *executionRun) snapshotNodeTimings() map[string]any {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]any, len(r.nodeTimings))
	for k, v := range r.nodeTimings {
		entry := make(map[string]any, len(v))
		for kk, vv := range v {
			entry[kk] = vv
		}
		out[k] = entry
	}
	return out
}

// beginNode records a node start: the first attempt writes started_at; retry
// attempts only bump the counters. Returns the timeline event.
func (r *executionRun) beginNode(nodeID, nodeType, name string) map[string]any {
	if r == nil {
		return nil
	}
	now := nowISO()
	ms := time.Now().UnixMilli()
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.nodeTimings[nodeID]
	if !ok {
		entry = map[string]any{
			"started_at":        now,
			"started_ms":        ms,
			"ended_at":          "", // running-node marker (§6.2 keeper 判据①)
			"ended_ms":          nil,
			"duration_ms":       nil,
			"total_duration_ms": nil,
			"attempts":          0,
			"failed":            false,
		}
		r.nodeTimings[nodeID] = entry
	}
	entry["attempts"] = toInt(entry["attempts"]) + 1
	entry["ended_at"] = ""
	entry["ended_ms"] = nil
	entry["duration_ms"] = nil
	if n := toInt(entry["attempts"]); n > 1 {
		r.nodeRetries = n - 1
	}
	return r.recordTimelineLocked("node_start", map[string]any{
		"node_id": nodeID, "node_type": nodeType, "name": name,
	})
}

// endNode records a node end (§5.1 node_timings field set). Returns the
// timeline event.
func (r *executionRun) endNode(nodeID, nodeType, name string, failed bool) map[string]any {
	if r == nil {
		return nil
	}
	now := nowISO()
	ms := time.Now().UnixMilli()
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.nodeTimings[nodeID]
	if !ok {
		entry = map[string]any{
			"started_at": now, "started_ms": ms, "attempts": 1,
		}
		r.nodeTimings[nodeID] = entry
	}
	entry["ended_at"] = now
	entry["ended_ms"] = ms
	if started, ok := toFloat(entry["started_ms"]); ok {
		entry["duration_ms"] = ms - int64(started)
		entry["total_duration_ms"] = ms - int64(started)
	}
	entry["failed"] = failed
	if failed {
		r.failedNode = nodeID
	} else if r.failedNode == nodeID {
		// A retry of the same node succeeded — it is no longer the failed
		// node candidate.
		r.failedNode = ""
	}
	return r.recordTimelineLocked("node_end", map[string]any{
		"node_id": nodeID, "node_type": nodeType, "name": name, "failed": failed,
	})
}

func (r *executionRun) recordTimelineLocked(typ string, extra map[string]any) map[string]any {
	// caller holds r.mu
	seq := r.seq + 1
	r.seq = seq
	ev := map[string]any{
		"seq":   seq,
		"type":  typ,
		"at":    nowISO(),
		"at_ms": time.Now().UnixMilli(),
	}
	for k, v := range extra {
		ev[k] = v
	}
	if len(r.events) >= timelineMaxEvents {
		r.events = r.events[1:]
		r.dropped++
	}
	r.events = append(r.events, ev)
	return ev
}

// publishExecutionEvent is the worker-side live publish (§6.2: the worker is
// the only publisher of {prefix}:execution:events:{execution_id}).
func (w *Worker) publishExecutionEvent(ctx context.Context, executionID string, event map[string]any) {
	payload := map[string]any{"execution_id": executionID}
	for k, v := range event {
		payload[k] = v
	}
	blob, err := json.Marshal(payload)
	if err != nil {
		return
	}
	if err := w.rdb.Publish(ctx, w.cfg.KeyPrefix+":execution:events:"+executionID, string(blob)).Err(); err != nil {
		w.logger.Warn("execution event publish failed", "error", err)
	}
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		parsed, _ := strconv.Atoi(n)
		return parsed
	}
	return 0
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
}
