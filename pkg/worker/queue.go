package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue protocol (protocol §2): one stream, one consumer group per stream in
// this deployment, DLQ as a plain record stream, reclaim-before-read with a
// rotating cursor and an active-lease guard.

// Envelope is a raw stream message: the two wire fields (§2.2) plus metadata
// extracted at parse time.
type Envelope struct {
	MessageID     string
	SchemaVersion int
	PayloadRaw    string
	Fields        map[string]any
}

// Task is the parsed payload body (§2.3 field table).
type Task struct {
	Type             string         `json:"type"` // start | resume
	FlowID           string         `json:"flow_id"`
	ExecutionID      string         `json:"execution_id"`
	Version          string         `json:"version,omitempty"`
	Params           map[string]any `json:"params,omitempty"`
	TenantID         string         `json:"tenant_id,omitempty"`
	Timestamp        string         `json:"timestamp,omitempty"`
	DedupKey         string         `json:"dedup_key,omitempty"`
	ResumeType       string         `json:"resume_type,omitempty"`
	Data             map[string]any `json:"data,omitempty"`
	TargetInstanceID string         `json:"target_instance_id,omitempty"`
	QuotaDefers      int            `json:"quota_defers,omitempty"`
	QuotaBypass      bool           `json:"quota_bypass,omitempty"`

	// worker-side metadata (not part of the payload wire format)
	DeliveryCount        int
	EnvelopeSchemaVer    int
	MessageID            string
	SuspendedResumeChain bool // true when this is a decision resume carrier (§3.5)
}

// reclaimWindow mirrors task_queue.py:392-394 (#196 rotating window).
const reclaimWindow = 8

// reconnectBackoff is RECONNECT_BACKOFF_SECONDS (task_queue.py:59).
const reconnectBackoff = time.Second

// DeadLetterGuard is consulted before a guarded dead-letter (§9.2
// _dead_letter_guard). It returns true to allow the dead-letter to proceed.
// MVP guard semantics live in worker.go; the recovery-clone re-enqueue branch
// is phase 2.
type DeadLetterGuard func(ctx context.Context, task *Task, reason string) bool

// TaskQueue implements the Redis stream task queue face.
type TaskQueue struct {
	client *redis.Client
	cfg    *Config
	logger Logger

	reclaimCursor string
	dlGuard       DeadLetterGuard
}

// Logger is the minimal logging face the worker needs (slog-compatible).
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

func NewTaskQueue(client *redis.Client, cfg *Config, logger Logger) *TaskQueue {
	return &TaskQueue{client: client, cfg: cfg, logger: logger, reclaimCursor: "0"}
}

// SetDeadLetterGuard wires the guarded dead-letter callback.
func (q *TaskQueue) SetDeadLetterGuard(g DeadLetterGuard) { q.dlGuard = g }

// EnsureGroup creates the consumer group idempotently (§2.1: XGROUP CREATE
// id=0 mkstream, BUSYGROUP tolerated).
func (q *TaskQueue) EnsureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.cfg.QueueName, q.cfg.ConsumerGroup, "0").Err()
	if err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists" {
		return nil
	}
	return err
}

// Read performs one read round (§2.4): reclaim first, then XREADGROUP
// COUNT 1 BLOCK. Returns a nil delivery when nothing is available (including
// transient Redis failures, which log + back off instead of failing the
// loop). Malformed payloads and over-version envelopes are dead-lettered
// inside this call.
func (q *TaskQueue) Read(ctx context.Context) (*Delivery, error) {
	// §2.4 step 1: ensure the group every round — idempotent, and load
	// bearing: a fully drained stream (XDEL everything) loses its group with
	// the key, so it must be recreated.
	if err := q.EnsureGroup(ctx); err != nil {
		return nil, err
	}
	if env, count := q.reclaimOne(ctx); env != nil {
		return q.deliveryFor(ctx, env, count, true)
	}
	res, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.cfg.ConsumerGroup,
		Consumer: q.cfg.ConsumerName,
		Streams:  []string{q.cfg.QueueName, ">"},
		Count:    1,
		Block:    time.Duration(q.cfg.ReadBlockMS) * time.Millisecond,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		// BLOCK expiry surfaces as a context error on some client paths;
		// transient failure family (§2.4): warn + backoff, keep polling.
		q.logger.Warn("queue read transient failure", "error", err)
		select {
		case <-ctx.Done():
		case <-time.After(reconnectBackoff):
		}
		return nil, nil
	}
	if len(res) == 0 || len(res[0].Messages) == 0 {
		return nil, nil
	}
	msg := res[0].Messages[0]
	env := &Envelope{MessageID: msg.ID, Fields: msg.Values}
	// First delivery: delivery_count is always 1 (§2.4), the ceiling gate
	// never applies to it.
	return q.deliveryFor(ctx, env, 1, false)
}

// deliveryFor parses + schema-checks one envelope into a Delivery.
// deliveryCount is the delivery count this round carries (1 for first
// delivery, the pre-XCLAIM times_delivered for reclaim); it drives the DLQ
// envelope and is stamped on the task. The delivery ceiling gate applies on
// the reclaim path only (§2.4).
func (q *TaskQueue) deliveryFor(ctx context.Context, env *Envelope, deliveryCount int, reclaim bool) (*Delivery, error) {
	task, perr := q.parseEnvelope(env)
	if perr != nil {
		// Malformed payload at the parse point: DLQ + ack in place (§2.4).
		q.deadLetterInvalid(ctx, env, perr, deliveryCount)
		return nil, nil
	}
	task.DeliveryCount = deliveryCount
	if err := q.checkSchema(ctx, env, task, deliveryCount); err != nil {
		return nil, nil // already dead-lettered
	}
	if reclaim && q.reclaimDeliveryCeiling(ctx, env, task, deliveryCount) {
		return nil, nil // guarded dead-letter done
	}
	return &Delivery{Envelope: env, Task: task}, nil
}

// parseEnvelope fills env.SchemaVersion / env.PayloadRaw and decodes the
// payload into a Task. Missing schema_version = v1; an unparsable
// schema_version also falls back to v1 with a warning (§2.2). A payload that
// is not a JSON object is ErrInvalidPayload.
func (q *TaskQueue) parseEnvelope(env *Envelope) (*Task, error) {
	raw, _ := env.Fields["payload"].(string)
	env.PayloadRaw = raw
	switch sv := env.Fields["schema_version"].(type) {
	case string:
		if sv == "" {
			env.SchemaVersion = 1 // missing = v1
			break
		}
		n, err := strconv.Atoi(sv)
		if err != nil {
			q.logger.Warn("unparsable schema_version, falling back to v1", "value", sv)
			env.SchemaVersion = 1
			break
		}
		env.SchemaVersion = n
	case nil:
		env.SchemaVersion = 1
	default:
		q.logger.Warn("unexpected schema_version field type, falling back to v1",
			"type", fmt.Sprintf("%T", sv))
		env.SchemaVersion = 1
	}
	if raw == "" {
		return nil, fmt.Errorf("%w: missing payload field", ErrInvalidPayload)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(raw), &body); err != nil || body == nil {
		return nil, fmt.Errorf("%w: payload is not a JSON object: %v", ErrInvalidPayload, err)
	}
	task := &Task{EnvelopeSchemaVer: env.SchemaVersion, MessageID: env.MessageID}
	if err := json.Unmarshal([]byte(raw), task); err != nil {
		return nil, fmt.Errorf("%w: payload decode: %v", ErrInvalidPayload, err)
	}
	switch task.ResumeType {
	case "event", "timeout", "cancel":
		// Decision carriers: the only wake-up source for suspended
		// executions — never acked on lease conflict (§3.5 A47-F2).
		task.SuspendedResumeChain = true
	}
	return task, nil
}

// checkSchema rejects envelopes above this build's write version into the
// DLQ (§2.2, both first-delivery and reclaim paths must make the same call).
func (q *TaskQueue) checkSchema(ctx context.Context, env *Envelope, task *Task, deliveryCount int) error {
	if env.SchemaVersion <= TaskSchemaVersion {
		return nil
	}
	q.deadLetter(ctx, env, task, deliveryCount, ReasonUnsupportedSchema, nil)
	return ErrUnsupportedSchema
}

// Dead-letter reason constants. The protocol document does not pin the exact
// Python reason literals (§2.2/§2.4 quote call sites, not strings) — these
// are the Go-side canonical names; shadow reconciliation treats them as
// opaque.
const (
	ReasonInvalidPayload       = "invalid_task_payload"
	ReasonUnsupportedSchema    = "unsupported_schema_version"
	ReasonMaxDeliveries        = "max_deliveries_exceeded"
	ReasonDeadLetterGuardFired = "dead_letter_guard_fired"
)

// reclaimOne implements the reclaim pass (§2.4 step 2): scan the PEL from the
// rotating cursor, apply idle threshold + active-lease guard, XCLAIM single
// messages. Returns the envelope and its pre-XCLAIM times_delivered.
func (q *TaskQueue) reclaimOne(ctx context.Context) (*Envelope, int) {
	pending, err := q.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: q.cfg.QueueName,
		Group:  q.cfg.ConsumerGroup,
		Start:  q.scanStart(),
		End:    "+",
		Count:  reclaimWindow,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			q.logger.Warn("reclaim scan failed", "error", err)
		}
		return nil, 0
	}
	if len(pending) == 0 {
		// Tail wrap-around (#196): restart the cursor from the beginning so a
		// stuck tail cannot pin the window.
		q.reclaimCursor = "0"
		return nil, 0
	}
	if len(pending) < reclaimWindow {
		// Short window: next round starts over from the head.
		q.reclaimCursor = "0"
	} else {
		q.reclaimCursor = pending[len(pending)-1].ID
	}
	for _, p := range pending {
		if time.Duration(p.Idle) < time.Duration(q.cfg.ClaimMinIdleMS)*time.Millisecond {
			continue
		}
		if env, count := q.reclaimMessage(ctx, p); env != nil {
			return env, count
		}
	}
	return nil, 0
}

func (q *TaskQueue) scanStart() string {
	if q.reclaimCursor == "" || q.reclaimCursor == "0" {
		return "-"
	}
	// Inclusive boundary scan: a re-scanned boundary message is harmless —
	// the idle threshold gates it, and re-claiming our own eligible pending
	// message is exactly the redelivery semantics we want. (Exclusive ranges
	// "(" would be nicer but are not portable to embedded Redis test doubles.)
	return q.reclaimCursor
}

// reclaimMessage applies the lease guard + XCLAIM for one pending message.
// timesDelivered is the count BEFORE the XCLAIM (§2.4: the net semantics make
// it exactly "max_deliveries processed attempts → dead letter"); parse /
// schema / delivery-ceiling gates run in the caller (same code path as first
// delivery, §2.2 两处必须同判).
func (q *TaskQueue) reclaimMessage(ctx context.Context, p redis.XPendingExt) (*Envelope, int) {
	// Active-lease guard (§2.4 #47): read the body without side effects and
	// skip when the execution lease is still held. On probe failure,
	// conservatively skip.
	msgs, err := q.client.XRange(ctx, q.cfg.QueueName, p.ID, p.ID).Result()
	if err != nil || len(msgs) == 0 {
		q.logger.Warn("reclaim probe failed, skipping", "id", p.ID, "error", err)
		return nil, 0
	}
	env := &Envelope{MessageID: msgs[0].ID, Fields: msgs[0].Values}
	if task, perr := q.parseEnvelope(env); perr == nil && task.ExecutionID != "" {
		holder, herr := NewExecutionLease(q.client, q.cfg).GetHolder(ctx, task.TenantID, task.ExecutionID)
		if herr != nil {
			q.logger.Warn("reclaim lease probe failed, skipping", "id", p.ID, "error", herr)
			return nil, 0
		}
		if holder != "" {
			return nil, 0 // still held: not reclaimable yet
		}
	}
	claimed, err := q.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   q.cfg.QueueName,
		Group:    q.cfg.ConsumerGroup,
		Consumer: q.cfg.ConsumerName,
		MinIdle:  time.Duration(q.cfg.ClaimMinIdleMS) * time.Millisecond,
		Messages: []string{p.ID},
	}).Result()
	if err != nil || len(claimed) == 0 {
		if err != nil {
			q.logger.Warn("reclaim xclaim failed", "id", p.ID, "error", err)
		}
		return nil, 0
	}
	return &Envelope{MessageID: claimed[0].ID, Fields: claimed[0].Values}, int(p.RetryCount)
}

// reclaimDeliveryCeiling is the delivery-budget gate (§2.4): only the reclaim
// path applies it (first deliveries always process).
func (q *TaskQueue) reclaimDeliveryCeiling(ctx context.Context, env *Envelope, task *Task, deliveryCount int) bool {
	if deliveryCount < q.cfg.MaxDeliveries {
		return false
	}
	q.deadLetterGuarded(ctx, env, task, deliveryCount, ReasonMaxDeliveries)
	return true
}

// Ack XACKs the message then best-effort XDELs the entry (§2.4: every stream
// in this deployment carries exactly one group; a failed XDEL never breaks
// the ack semantics).
func (q *TaskQueue) Ack(ctx context.Context, messageID string) error {
	n, err := q.client.XAck(ctx, q.cfg.QueueName, q.cfg.ConsumerGroup, messageID).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		q.logger.Warn("ack matched no pending message", "id", messageID)
	}
	_ = q.client.XDel(ctx, q.cfg.QueueName, messageID).Err()
	return nil
}

// DeadLetter writes the DLQ envelope (§2.4 schema): XADD → XTRIM (exact, per
// §2.1 approximate=False) → ack. The task dict is embedded when available,
// otherwise the raw payload string. Malformed messages additionally carry
// raw_fields (values truncated to 500 chars) + truncated.
func (q *TaskQueue) deadLetter(ctx context.Context, env *Envelope, task *Task, deliveryCount int, reason string, rawFields map[string]any) {
	entry := map[string]any{
		"reason":           reason,
		"source_stream":    q.cfg.QueueName,
		"source_id":        env.MessageID,
		"delivery_count":   deliveryCount,
		"dead_lettered_at": floatEpoch(),
		"schema_version":   env.SchemaVersion,
	}
	if task != nil {
		entry["payload"] = taskPayloadDict(task)
	} else {
		entry["payload"] = env.PayloadRaw
	}
	if rawFields != nil {
		entry["raw_fields"] = rawFields
		if _, truncated := rawFields["truncated"]; !truncated {
			entry["truncated"] = false
		}
	}
	blob, err := json.Marshal(entry)
	if err != nil {
		q.logger.Error("dead letter marshal failed", "error", err, "reason", reason)
		return
	}
	if err := q.client.XAdd(ctx, &redis.XAddArgs{Stream: q.cfg.DLQKey, Values: map[string]any{
		"payload":        string(blob),
		"schema_version": strconv.Itoa(TaskSchemaVersion),
	}}).Err(); err != nil {
		q.logger.Error("dead letter XADD failed", "error", err, "reason", reason)
		return
	}
	if err := q.client.XTrimMaxLen(ctx, q.cfg.DLQKey, q.cfg.DLQMaxLen).Err(); err != nil {
		q.logger.Warn("dead letter XTRIM failed", "error", err)
	}
	if err := q.Ack(ctx, env.MessageID); err != nil {
		// §2.4: an ack failure after a successful DLQ write is a named,
		// loud failure — the message stays pending and will dead-letter
		// again rather than vanish.
		q.logger.Error("dead letter ACK failed", "error", err, "id", env.MessageID)
	}
}

// deadLetterInvalid handles malformed payloads: DLQ envelope carries the
// truncated raw fields + truncated flag (§2.4).
func (q *TaskQueue) deadLetterInvalid(ctx context.Context, env *Envelope, perr error, deliveryCount int) {
	q.logger.Warn("invalid task payload dead-lettered", "id", env.MessageID, "error", perr)
	q.deadLetter(ctx, env, nil, deliveryCount, ReasonInvalidPayload, truncatedRawFields(env.Fields))
}

// deadLetterGuarded runs the dead-letter guard first (§9.2); a skipped guard
// leaves the message pending.
func (q *TaskQueue) deadLetterGuarded(ctx context.Context, env *Envelope, task *Task, deliveryCount int, reason string) {
	if q.dlGuard != nil && !q.dlGuard(ctx, task, reason) {
		q.logger.Warn("dead letter guard skipped dead-lettering", "id", env.MessageID, "reason", reason)
		return
	}
	q.deadLetter(ctx, env, task, deliveryCount, reason, nil)
}

// taskPayloadDict re-parses the raw payload into a generic dict for the DLQ
// envelope ("payload": 任务体 dict 或原始串).
func taskPayloadDict(task *Task) map[string]any {
	blob, err := json.Marshal(task)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(blob, &out); err != nil {
		return map[string]any{}
	}
	return out
}

// truncatedRawFields truncates every stream field value to 500 chars and
// reports whether truncation happened (§2.4).
func truncatedRawFields(fields map[string]any) map[string]any {
	const maxLen = 500
	out := map[string]any{"truncated": false}
	for k, v := range fields {
		s := fmt.Sprintf("%v", v)
		if len(s) > maxLen {
			out[k] = s[:maxLen]
			out["truncated"] = true
			continue
		}
		out[k] = s
	}
	return out
}

// SweepAckedResidue deletes entries with id <= group last-delivered-id that
// are not in the PEL (§2.4 fallback sweep; MVP uses a bounded scan).
func (q *TaskQueue) SweepAckedResidue(ctx context.Context) error {
	groups, err := q.client.XInfoGroups(ctx, q.cfg.QueueName).Result()
	if err != nil {
		return err
	}
	var lastDelivered string
	for _, g := range groups {
		if g.Name == q.cfg.ConsumerGroup {
			lastDelivered = g.LastDeliveredID
		}
	}
	if lastDelivered == "" || lastDelivered == "0" || lastDelivered == "0-0" {
		return nil
	}
	// PEL membership set (single bounded page: the sweep is a best-effort
	// residue cleaner, so an oversized PEL skips this round rather than risk
	// deleting a pending message off a partial set).
	pel := map[string]bool{}
	page, err := q.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: q.cfg.QueueName,
		Group:  q.cfg.ConsumerGroup,
		Start:  "-",
		End:    "+",
		Count:  1000,
	}).Result()
	if err != nil {
		return err
	}
	if len(page) >= 1000 {
		q.logger.Warn("sweep skipped: PEL too large for bounded scan", "size", len(page))
		return nil
	}
	for _, p := range page {
		pel[p.ID] = true
	}
	var residue []string
	msgs, err := q.client.XRange(ctx, q.cfg.QueueName, "-", lastDelivered).Result()
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if !pel[m.ID] {
			residue = append(residue, m.ID)
		}
	}
	if len(residue) > 0 {
		return q.client.XDel(ctx, q.cfg.QueueName, residue...).Err()
	}
	return nil
}

func floatEpoch() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}
