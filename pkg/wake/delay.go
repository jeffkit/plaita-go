package wake

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

// DelayService is the Go counterpart of Python plaita DelayService: it
// consumes the delay queue the worker's §5.2 dispatch writes
// (`{prefix}:delay:queue`, body = service_config + execution_id + tenant_id)
// and publishes the resume event when the delay is due.
//
// Two-phase list→ZSET scheduling (Python C4-2, kept byte-compatible):
//
//	convey:   LRANGE list → ZADD {queue}:scheduled (member=raw JSON,
//	          score=due epoch ms) → LREM list. ZADD first, remove second: a
//	          crash in between re-conveys next round and ZADD is idempotent
//	          per member — no loss.
//	trigger:  ZRANGEBYSCORE due → publish resume event → ZREM only after the
//	          publish is confirmed (at-least-once). A failed publish leaves
//	          the entry scheduled, records the attempt in the {queue}:attempts
//	          hash ({"attempts": n, "next": retry-after ms}) and backs off
//	          min(base·2^(n-1), max); past max attempts the entry moves to the
//	          {queue}:dead list (RPUSH before ZREM — prefer a duplicate over a
//	          loss) and a best-effort service_error event is broadcast.
//
// Duplicate resume events are converged by the consumer side's NX dedup
// (worker EventSubscriptions / Python EventFilter) — this service only has to
// be at-least-once.
type DelayService struct {
	cfg *Config
	rdb *redis.Client
	pub *worker.EventPublisher
	reg *ServiceRegistry

	queueKey     string
	scheduledKey string
	attemptsKey  string
	deadKey      string

	pollInterval time.Duration
	backoffBase  time.Duration
	backoffMax   time.Duration
	maxAttempts  int
	conveyBatch  int
	dueBatch     int
	logger       *slog.Logger

	// publishFn is the event publish seam (tests inject failures).
	publishFn func(context.Context, *worker.Event) error

	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
	done     chan struct{}
}

// NewDelayService builds the delay wake source. The Redis round-trip keys are
// derived from cfg (DelayQueueKey); the resume event face reuses the worker's
// EventPublisher so the double-write keys and channel naming stay
// byte-compatible.
func NewDelayService(cfg *Config, rdb *redis.Client) (*DelayService, error) {
	queueKey, err := cfg.DelayQueueKey()
	if err != nil {
		return nil, err
	}
	wcfg := &worker.Config{
		KeyPrefix:       cfg.KeyPrefix,
		DefaultTenantID: cfg.DefaultTenantID,
		EventTTLSeconds: cfg.EventTTLSeconds,
	}
	s := &DelayService{
		cfg:          cfg,
		rdb:          rdb,
		pub:          worker.NewEventPublisher(rdb, wcfg),
		reg:          NewServiceRegistry(rdb, cfg, SourceDelay),
		queueKey:     queueKey,
		scheduledKey: queueKey + ":scheduled",
		attemptsKey:  queueKey + ":attempts",
		deadKey:      queueKey + ":dead",
		pollInterval: cfg.PollInterval,
		backoffBase:  cfg.RetryBackoffBase,
		backoffMax:   cfg.RetryBackoffMax,
		maxAttempts:  cfg.MaxTriggerAttempts,
		conveyBatch:  cfg.ConveyBatch,
		dueBatch:     cfg.DueBatch,
		logger:       cfg.Logger,
		inflight:     make(map[string]struct{}),
		done:         make(chan struct{}),
	}
	s.publishFn = s.pub.Publish
	return s, nil
}

// Source reports the wake source identity (registry key segment = the
// {prefix}:{source}:queue subtype the worker dispatches to).
func (s *DelayService) Source() string { return SourceDelay }

// Run drives the consume loop until ctx is done: registry heartbeats + the
// periodic convey/due cycle. Shutdown is graceful: in-flight triggers finish
// (a publish racing the deadline stays parked — see processTask), then the
// registry record is tombstoned and removed.
func (s *DelayService) Run(ctx context.Context) error {
	if err := s.reg.Beat(ctx, RegStatusStarting, 0); err != nil {
		s.logger.Warn("wake delay registry start beat failed; continuing (TTL roll-call will show the gap)",
			"error", err)
	}
	regCtx, cancelReg := context.WithCancel(context.Background())
	regDone := make(chan struct{})
	go func() { s.registryLoop(regCtx); close(regDone) }()
	s.logger.Info("wake delay service started",
		"queue", s.queueKey, "scheduled", s.scheduledKey, "instance", s.cfg.InstanceID)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cancelReg() // stop the heartbeat loop first: no beats after tombstone
			<-regDone
			s.wg.Wait() // in-flight triggers land or stay parked on their own
			s.reg.Stop(context.Background())
			s.stopOnce.Do(func() { close(s.done) })
			s.logger.Info("wake delay service stopped", "queue", s.queueKey)
			return nil
		case <-ticker.C:
			s.conveyOnce(ctx)
			s.submitDueOnce(ctx)
		}
	}
}

// Done closes when the service has fully stopped (registry tombstoned).
func (s *DelayService) Done() <-chan struct{} { return s.done }

func (s *DelayService) registryLoop(ctx context.Context) {
	if err := s.reg.Beat(ctx, RegStatusRunning, 0); err != nil {
		s.logger.Warn("wake delay registry beat failed", "error", err)
	}
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.reg.Beat(ctx, RegStatusRunning, s.activeCount()); err != nil {
				s.logger.Warn("wake delay registry beat failed", "error", err)
			}
		}
	}
}

// ---- list → ZSET convey ----

// conveyOnce moves up to conveyBatch list entries into the scheduling ZSET.
// Invalid entries (undecodable, non-object, or missing node_id/event_type/
// delay fields) are logged and dropped from the list, never scheduled.
func (s *DelayService) conveyOnce(ctx context.Context) {
	raws, err := s.rdb.LRange(ctx, s.queueKey, 0, int64(s.conveyBatch)-1).Result()
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Error("wake delay convey read failed", "error", err)
		}
		return
	}
	if len(raws) == 0 {
		return
	}
	nowMs := time.Now().UnixMilli()
	type validEntry struct {
		raw   string
		score int64
	}
	valid := make([]validEntry, 0, len(raws))
	for _, raw := range raws {
		var task map[string]any
		if err := json.Unmarshal([]byte(raw), &task); err != nil {
			s.logger.Error("wake delay invalid task JSON (dropped)", "raw", truncate(raw, 200), "error", err)
			s.lrem(ctx, raw)
			continue
		}
		if !validateDelayTask(task) {
			s.logger.Error("wake delay task failed validation (dropped)", "raw", truncate(raw, 200))
			s.lrem(ctx, raw)
			continue
		}
		valid = append(valid, validEntry{raw: raw, score: dueScoreMs(task, nowMs)})
	}
	if len(valid) == 0 {
		return
	}
	pipe := s.rdb.Pipeline()
	for _, e := range valid {
		pipe.ZAdd(ctx, s.scheduledKey, redis.Z{Score: float64(e.score), Member: e.raw})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		s.logger.Error("wake delay convey ZADD failed; list untouched, next round re-conveys", "error", err)
		return
	}
	// ZADD succeeded — now (and only now) remove from the list.
	pipe = s.rdb.Pipeline()
	for _, e := range valid {
		pipe.LRem(ctx, s.queueKey, 1, e.raw)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		s.logger.Error("wake delay convey LREM failed; ZADD is idempotent, next round re-conveys", "error", err)
	}
	s.logger.Debug("wake delay conveyed tasks", "count", len(valid))
}

func (s *DelayService) lrem(ctx context.Context, raw string) {
	if err := s.rdb.LRem(ctx, s.queueKey, 1, raw).Err(); err != nil {
		s.logger.Error("wake delay bad-entry removal failed", "error", err)
	}
}

// validateDelayTask mirrors Python validate_task_config: node_id + event_type
// required (base), and at least one of delay_ms / trigger_timestamp (delay
// specific).
func validateDelayTask(task map[string]any) bool {
	if str(task["node_id"]) == "" || str(task["event_type"]) == "" {
		return false
	}
	if _, ok := task["delay_ms"]; ok {
		return true
	}
	_, ok := task["trigger_timestamp"]
	return ok
}

// dueScoreMs mirrors Python _due_score_ms: trigger_timestamp wins, else
// now + delay_ms (a zero delay is immediately due).
func dueScoreMs(task map[string]any, nowMs int64) int64 {
	if ts := num(task["trigger_timestamp"]); ts > 0 {
		return int64(ts)
	}
	return nowMs + int64(num(task["delay_ms"]))
}

// ---- due poll + trigger ----

func (s *DelayService) submitDueOnce(ctx context.Context) {
	nowMs := time.Now().UnixMilli()
	due, err := s.rdb.ZRangeByScore(ctx, s.scheduledKey, &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprint(nowMs), Offset: 0, Count: int64(s.dueBatch),
	}).Result()
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Error("wake delay due poll failed", "error", err)
		}
		return
	}
	if len(due) == 0 {
		return
	}
	attempts := s.loadAttempts(ctx, due)
	for _, member := range due {
		if rec := attempts[member]; rec.Next > nowMs {
			continue // backoff window — next round
		}
		if !s.inflightTryAdd(member) {
			continue
		}
		s.wg.Add(1)
		go func(member string) {
			defer s.wg.Done()
			defer s.inflightDiscard(member)
			s.processTask(ctx, member)
		}(member)
	}
}

func (s *DelayService) processTask(ctx context.Context, member string) {
	var task map[string]any
	if err := json.Unmarshal([]byte(member), &task); err != nil || task == nil {
		s.logger.Error("wake delay scheduled task undecodable (removed)", "raw", truncate(member, 200), "error", err)
		s.finalizeTask(ctx, member)
		return
	}
	if ctx.Err() != nil {
		return // shutdown: the entry stays parked for the next start
	}
	ev := s.buildResumeEvent(task, time.Now().UnixMilli())
	if err := s.publishFn(ctx, ev); err != nil {
		if ctx.Err() != nil {
			return // shutdown raced the publish: no attempt counted, stay parked
		}
		s.deferTask(ctx, member)
		return
	}
	s.finalizeTask(ctx, member)
}

// buildResumeEvent mirrors Python handle_task's event_data shape and
// publish_resume_event's trusted-tenant wiring: correlation_id = execution_id,
// tenant from the service task (never from event data alone).
func (s *DelayService) buildResumeEvent(task map[string]any, nowMs int64) *worker.Event {
	tenant := str(task["tenant_id"])
	if tenant == "" {
		tenant = s.cfg.DefaultTenantID
	}
	executionID := str(task["execution_id"])
	data := map[string]any{
		"node_id":                   task["node_id"],
		"execution_id":              executionID,
		"flow_id":                   task["flow_id"],
		"tenant_id":                 tenant,
		"trigger_type":              "delay_completed",
		"delay_ms":                  task["delay_ms"],
		"actual_trigger_timestamp":  nowMs,
		"planned_trigger_timestamp": task["trigger_timestamp"],
		"success":                   true,
	}
	return &worker.Event{
		EventID:       uuid.NewString(),
		EventType:     str(task["event_type"]),
		Data:          data,
		Timestamp:     float64(nowMs) / 1000.0, // wire/storage convention: Python Event.timestamp = time.time() (epoch seconds); the type-index ZADD score shares the unit (RS reconcile scans seconds windows)
		CorrelationID: executionID,
		TenantID:      tenant,
	}
}

// deferTask keeps the entry scheduled (never dequeues on failure), records
// the attempt with an exponential backoff, and dead-letters past the attempt
// ceiling. Registry failures inside this path also leave the entry parked
// (at-least-once; the event-side NX dedup converges duplicate triggers).
func (s *DelayService) deferTask(ctx context.Context, member string) {
	rec := s.loadAttempts(ctx, []string{member})[member]
	attempts := rec.Attempts + 1
	if attempts >= s.maxAttempts {
		pipe := s.rdb.Pipeline()
		pipe.RPush(ctx, s.deadKey, member)
		pipe.ZRem(ctx, s.scheduledKey, member)
		pipe.HDel(ctx, s.attemptsKey, member)
		if _, err := pipe.Exec(ctx); err != nil {
			s.logger.Error("wake delay dead-letter move failed; entry stays scheduled (duplicate possible, loss impossible)",
				"error", err)
			return
		}
		s.logger.Error("wake delay trigger retries exhausted; dead-lettered",
			"attempts", attempts, "dead_key", s.deadKey, "task", truncate(member, 200))
		s.publishDeadLetterEvent(member, attempts)
		return
	}
	backoff := time.Duration(float64(s.backoffBase) * math.Pow(2, float64(attempts-1)))
	if backoff > s.backoffMax {
		backoff = s.backoffMax
	}
	next := time.Now().UnixMilli() + backoff.Milliseconds()
	blob, _ := json.Marshal(attemptRecord{Attempts: attempts, Next: next})
	if err := s.rdb.HSet(ctx, s.attemptsKey, member, string(blob)).Err(); err != nil {
		s.logger.Error("wake delay backoff record failed; entry stays scheduled, next round retries immediately",
			"error", err)
		return
	}
	s.logger.Warn("wake delay trigger failed; kept scheduled for retry",
		"attempt", fmt.Sprintf("%d/%d", attempts, s.maxAttempts), "backoff_ms", backoff.Milliseconds(),
		"task", truncate(member, 200))
}

func (s *DelayService) publishDeadLetterEvent(member string, attempts int) {
	var task map[string]any
	_ = json.Unmarshal([]byte(member), &task)
	if task == nil {
		task = map[string]any{}
	}
	tenant := str(task["tenant_id"])
	if tenant == "" {
		tenant = s.cfg.DefaultTenantID
	}
	ev := &worker.Event{
		EventID:   uuid.NewString(),
		EventType: "service_error",
		Data: map[string]any{
			"node_id":       task["node_id"],
			"execution_id":  task["execution_id"],
			"flow_id":       task["flow_id"],
			"tenant_id":     tenant,
			"error_type":    "delay_trigger_dead_letter",
			"error_message": fmt.Sprintf("延迟任务到期触发重试超限（%d 次），已转死信: %s", attempts, s.deadKey),
			"service_type":  SourceDelay,
			"timestamp":     time.Now().UnixMilli(),
		},
		Timestamp:     float64(time.Now().UnixNano()) / 1e9, // epoch seconds (Python Event.timestamp convention)
		CorrelationID: str(task["execution_id"]),
		TenantID:      tenant,
	}
	// Best-effort broadcast: the dead list itself is the discovery surface.
	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.publishFn(bgCtx, ev); err != nil {
		s.logger.Warn("wake delay dead-letter event broadcast failed (dead list remains)", "error", err)
	}
}

// finalizeTask removes the confirmed entry from the schedule and clears its
// attempt counter (prevents hash leak and stale backoff windows).
func (s *DelayService) finalizeTask(ctx context.Context, member string) {
	if err := s.rdb.ZRem(ctx, s.scheduledKey, member).Err(); err != nil {
		s.logger.Error("wake delay finalize ZREM failed; a duplicate trigger may follow (event-side dedup converges)",
			"error", err)
	}
	if err := s.rdb.HDel(ctx, s.attemptsKey, member).Err(); err != nil {
		s.logger.Error("wake delay finalize HDel failed", "error", err)
	}
}

// ---- attempts hash ----

type attemptRecord struct {
	Attempts int   `json:"attempts"`
	Next     int64 `json:"next"`
}

// loadAttempts reads the retry records for the given members. Read failures
// are fail-open (no backoff — retry immediately): a broken counter must never
// park a task out of reach, mirroring Python.
func (s *DelayService) loadAttempts(ctx context.Context, members []string) map[string]attemptRecord {
	out := make(map[string]attemptRecord, len(members))
	raws, err := s.rdb.HMGet(ctx, s.attemptsKey, members...).Result()
	if err != nil {
		s.logger.Error("wake delay attempt records read failed (fail-open)", "error", err)
		return out
	}
	for i, raw := range raws {
		if raw == nil || i >= len(members) {
			continue
		}
		strRaw, ok := raw.(string)
		if !ok {
			continue
		}
		var rec attemptRecord
		if err := json.Unmarshal([]byte(strRaw), &rec); err != nil {
			continue
		}
		out[members[i]] = rec
	}
	return out
}

// ---- inflight ----

func (s *DelayService) inflightTryAdd(member string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inflight[member]; ok {
		return false
	}
	s.inflight[member] = struct{}{}
	return true
}

func (s *DelayService) inflightDiscard(member string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, member)
}

func (s *DelayService) activeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflight)
}

// ---- small helpers ----

func str(v any) string {
	s, _ := v.(string)
	return s
}

func num(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int64:
		return float64(t)
	case int:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	default:
		return 0
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
