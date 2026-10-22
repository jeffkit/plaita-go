package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// Dispatch ledger (protocol §6.3, server/dispatch_ledger.py). One append-only
// Stream records "who dispatched what to whom, and why". The entry and the
// enqueued message body share one source of truth for target_instance_id, so
// the ledger cannot disagree with the queue.
//
// Boundary: a ledger write failure never rolls back an enqueue — it is
// reported through EnqueueResult.LedgerRecorded=false, never an error that
// swallows an already-dispatched task.

// Ledger decision values (§6.3 DECISION_TARGETED / DECISION_BROADCAST).
const (
	DecisionTargeted  = "targeted"
	DecisionBroadcast = "broadcast"
)

// Handover reasons recorded by the consuming worker.
const (
	ReasonHandover             = "TaskNotForThisWorker: handover"
	ReasonTargetUnavailable    = "TaskNotForThisWorker: target unavailable, broadcast"
	ReasonHandoverExhausted    = "TaskNotForThisWorker: handover budget exhausted"
	ReasonHandoverUnbounded    = "TaskNotForThisWorker: handover counter unavailable"
	ReasonHandoverProbeFailure = "TaskNotForThisWorker: target probe failed"
	// ReasonHandoverLiveHolder (#53): the exhaustion terminal write was
	// refused because another worker's live lease owns the execution.
	ReasonHandoverLiveHolder = "TaskNotForThisWorker: live lease holder owns the execution, terminal write refused"
)

// LedgerEntry is the §6.3 entry schema.
type LedgerEntry struct {
	LedgerSchemaVersion int              `json:"ledger_schema_version"`
	DecidedAt           string           `json:"decided_at"`
	DecidedBy           string           `json:"decided_by"`
	IssueID             string           `json:"issue_id,omitempty"`
	Queue               string           `json:"queue"`
	Decision            string           `json:"decision"`
	Reason              string           `json:"reason,omitempty"`
	TargetInstanceID    string           `json:"target_instance_id,omitempty"`
	Enqueued            bool             `json:"enqueued"`
	MessageID           string           `json:"message_id,omitempty"`
	CandidateCount      int              `json:"candidate_count,omitempty"`
	Allowed             []map[string]any `json:"allowed,omitempty"`
	Rejected            []map[string]any `json:"rejected,omitempty"`
	Stale               []map[string]any `json:"stale,omitempty"`
	TenantID            string           `json:"tenant_id,omitempty"`
	TenantGeneration    string           `json:"tenant_generation,omitempty"`
	Actor               string           `json:"actor,omitempty"`
}

// DispatchLedger writes and reads the ledger stream.
type DispatchLedger struct {
	client *redis.Client
	cfg    *Config
	logger Logger
}

func NewDispatchLedger(client *redis.Client, cfg *Config, logger Logger) *DispatchLedger {
	return &DispatchLedger{client: client, cfg: cfg, logger: logger}
}

// newEntry seeds the schema version, decision time and dispatcher identity so
// callers only supply the decision facts.
func (l *DispatchLedger) newEntry(queue, actor string) *LedgerEntry {
	if actor == "" {
		actor = l.cfg.DispatcherID
	}
	return &LedgerEntry{
		LedgerSchemaVersion: ledgerSchemaVersion,
		DecidedAt:           nowISO(),
		DecidedBy:           l.cfg.DispatcherID,
		Queue:               queue,
		Enqueued:            false,
		Actor:               actor,
	}
}

// Record appends one entry. A failure is returned for observability but must
// never fail the dispatch that produced it (§6.3 red line).
func (l *DispatchLedger) Record(ctx context.Context, entry *LedgerEntry) (string, error) {
	if entry == nil {
		return "", errors.New("nil ledger entry")
	}
	if entry.LedgerSchemaVersion == 0 {
		entry.LedgerSchemaVersion = ledgerSchemaVersion
	}
	if entry.DecidedAt == "" {
		entry.DecidedAt = nowISO()
	}
	if entry.DecidedBy == "" {
		entry.DecidedBy = l.cfg.DispatcherID
	}
	blob, err := json.Marshal(entry)
	if err != nil {
		return "", err
	}
	id, err := l.client.XAdd(ctx, &redis.XAddArgs{
		Stream: l.cfg.DispatchLedgerKey,
		Values: map[string]any{
			"payload":        string(blob),
			"schema_version": strconv.Itoa(ledgerSchemaVersion),
		},
	}).Result()
	if err != nil {
		l.logger.Error("dispatch ledger write failed (dispatch not rolled back)",
			"error", err, "queue", entry.Queue, "decision", entry.Decision)
		return "", err
	}
	return id, nil
}

// EnqueueResult is the §6.3 enqueue_with_ledger return shape.
type EnqueueResult struct {
	Enqueued       bool
	MessageID      string
	LedgerID       string
	LedgerRecorded bool
	Error          string
}

// EnqueueWithLedger is the single "enqueue + ledger" entry point (§6.3): the
// target is derived from the entry only, written into the message body and
// the recorded entry alike.
func (l *DispatchLedger) EnqueueWithLedger(ctx context.Context, queue *TaskQueue,
	task map[string]any, entry *LedgerEntry) (*EnqueueResult, error) {
	out := &EnqueueResult{}
	if entry == nil {
		entry = l.newEntry(queueNameOf(queue), "")
	}
	entry.Queue = queueNameOf(queue)

	body := make(map[string]any, len(task)+1)
	for k, v := range task {
		body[k] = v
	}
	if entry.TargetInstanceID == "" {
		delete(body, "target_instance_id")
	} else {
		body["target_instance_id"] = entry.TargetInstanceID
	}
	entry.Enqueued = true

	messageID, err := queue.Enqueue(ctx, body, TaskSchemaVersion)
	if err != nil {
		l.logger.Error("dispatch enqueue failed (no ledger entry: an entry without an enqueue would lie)",
			"error", err)
		out.Error = "enqueue-failed: " + err.Error()
		return out, nil // the producer decides whether to retry; never a fake ledger row
	}
	out.Enqueued = true
	out.MessageID = messageID
	entry.MessageID = messageID

	ledgerID, lerr := l.Record(ctx, entry)
	out.LedgerID = ledgerID
	out.LedgerRecorded = lerr == nil
	if lerr != nil {
		out.Error = "ledger-write-failed"
	}
	return out, nil
}

func queueNameOf(q *TaskQueue) string {
	if q == nil || q.cfg == nil {
		return ""
	}
	return q.cfg.QueueName
}

// Read returns ledger entries oldest → newest, skipping unreadable rows
// (§6.3 read side never throws on bad data).
func (l *DispatchLedger) Read(ctx context.Context, count int64) ([]LedgerEntry, error) {
	var msgs []redis.XMessage
	var err error
	if count > 0 {
		msgs, err = l.client.XRangeN(ctx, l.cfg.DispatchLedgerKey, "-", "+", count).Result()
	} else {
		msgs, err = l.client.XRange(ctx, l.cfg.DispatchLedgerKey, "-", "+").Result()
	}
	if err != nil {
		return nil, err
	}
	out := make([]LedgerEntry, 0, len(msgs))
	for _, m := range msgs {
		raw, _ := m.Values["payload"].(string)
		var entry LedgerEntry
		if raw == "" || json.Unmarshal([]byte(raw), &entry) != nil {
			continue
		}
		out = append(out, entry)
	}
	return out, nil
}

// Length returns XLEN(ledger) (§6.3 parity probe).
func (l *DispatchLedger) Length(ctx context.Context) (int64, error) {
	return l.client.XLen(ctx, l.cfg.DispatchLedgerKey).Result()
}

// Parity checks XLEN(ledger) == XLEN(queue) (§6.3 phase-1 assertion). A read
// failure reports parity=false rather than silently pretending equality.
func (l *DispatchLedger) Parity(ctx context.Context, queueName string) (bool, int64, int64, error) {
	ledgerN, err := l.Length(ctx)
	if err != nil {
		return false, 0, 0, fmt.Errorf("ledger length: %w", err)
	}
	queueN, err := l.client.XLen(ctx, queueName).Result()
	if err != nil {
		return false, ledgerN, 0, fmt.Errorf("queue length: %w", err)
	}
	return ledgerN == queueN, ledgerN, queueN, nil
}

// workerHandoverEntry builds the ledger row a consuming worker writes when it
// forwards a task routed to another instance (§6.3 + §9.2).
func (w *Worker) handoverEntry(task *Task, decision, reason, messageID string) *LedgerEntry {
	return &LedgerEntry{
		LedgerSchemaVersion: ledgerSchemaVersion,
		DecidedAt:           nowISO(),
		DecidedBy:           w.cfg.InstanceID,
		Queue:               w.cfg.QueueName,
		Decision:            decision,
		Reason:              reason,
		TargetInstanceID:    task.TargetInstanceID,
		Enqueued:            messageID != "",
		MessageID:           messageID,
		TenantID:            task.TenantID,
		Actor:               "worker:" + w.cfg.InstanceID,
	}
}
