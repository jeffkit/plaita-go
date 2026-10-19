package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// Event subscriptions + wake-up dispatch (M6, protocol §6.1/§6.2).
//
// A suspending event/hitl_await node registers a subscription at the suspend
// boundary (engine -> SubscriptionRegistry). The subscription is persisted in
// Redis so ANY worker (and any process restart) can resolve it:
//
//	{ns}:event:subscriptions:{subscription_id}       = subscription JSON (TTL 7d)
//	{ns}:event:subscriptions:type:{event_type}       = SET of subscription ids (TTL 7d)
//	{ns}:event:subscriptions:timeouts                = ZSET sub-key by due time
//	{ns}:event_filter:dedup:{event_id}:{sub_id}      = "1" (1h, event dedup)
//	{ns}:event_filter:timeout:{sub_id}               = "1" (1h, timeout dedup)
//
// Event delivery mirrors plaita's EventFilter: match trusted tenant +
// correlation_id (execution id) + subscription filter, take a per
// (event, subscription) NX dedup key, then enqueue a resume_type=event
// carrier. The timeout checker mirrors SubscriptionTimeoutChecker and
// enqueues resume_type=timeout.
//
// The event payload itself is never trusted for tenant routing: tenant comes
// from Event.TenantID (bound by the publisher, protocol §6.1), never from
// Event.Data.

const eventSubscriptionSchemaVersion = 1

// EventSubscription is the persisted subscription record.
type EventSubscription struct {
	SchemaVersion  int            `json:"schema_version"`
	SubscriptionID string         `json:"subscription_id"`
	EventType      string         `json:"event_type"`
	Filter         map[string]any `json:"filter,omitempty"`
	CorrelationID  string         `json:"correlation_id,omitempty"`
	FlowID         string         `json:"flow_id,omitempty"`
	NodeID         string         `json:"node_id,omitempty"`
	TenantID       string         `json:"tenant_id,omitempty"`
	CreatedAt      string         `json:"created_at"`
	Timeout        float64        `json:"timeout,omitempty"`
	TimeoutAt      float64        `json:"timeout_at,omitempty"`
}

// EventSubscriptions implements workflow.SubscriptionRegistry on Redis.
type EventSubscriptions struct {
	client *redis.Client
	cfg    *Config
	logger Logger
}

// NewEventSubscriptions returns the registry for one worker.
func NewEventSubscriptions(client *redis.Client, cfg *Config, logger Logger) *EventSubscriptions {
	return &EventSubscriptions{client: client, cfg: cfg, logger: logger}
}

func (s *EventSubscriptions) subKey(tenantID, subscriptionID string) string {
	return s.cfg.Namespace(tenantID) + ":event:subscriptions:" + subscriptionID
}

func (s *EventSubscriptions) typeKey(tenantID, eventType string) string {
	return s.cfg.Namespace(tenantID) + ":event:subscriptions:type:" + eventType
}

func (s *EventSubscriptions) timeoutsKey() string {
	return s.cfg.KeyPrefix + ":event:subscriptions:timeouts"
}

// Register persists a subscription and (when TimeoutSeconds > 0) arms the
// timeout index. The subscription id is minted here when the engine left it
// empty; the engine copies the assigned id into the pending node state so a
// resume carrier can be bound to it.
func (s *EventSubscriptions) Register(ctx context.Context, req *workflow.SubscriptionRequest) error {
	if req == nil {
		return fmt.Errorf("event subscription request is nil")
	}
	if strings.TrimSpace(req.EventType) == "" {
		return fmt.Errorf("event subscription requires a non-empty event type")
	}
	if req.SubscriptionID == "" {
		req.SubscriptionID = newExecutionID()
	}
	now := float64(time.Now().UnixNano()) / 1e9
	sub := EventSubscription{
		SchemaVersion:  eventSubscriptionSchemaVersion,
		SubscriptionID: req.SubscriptionID,
		EventType:      req.EventType,
		Filter:         req.Filter,
		CorrelationID:  req.CorrelationID,
		FlowID:         req.FlowID,
		NodeID:         req.NodeID,
		TenantID:       req.TenantID,
		CreatedAt:      nowISO(),
		Timeout:        req.TimeoutSeconds,
	}
	if req.TimeoutSeconds > 0 {
		sub.TimeoutAt = now + req.TimeoutSeconds
	}
	blob, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("event subscription marshal: %w", err)
	}
	ttl := time.Duration(s.cfg.EventTTLSeconds) * time.Second
	subKey := s.subKey(sub.TenantID, sub.SubscriptionID)
	pipe := s.client.TxPipeline()
	pipe.Set(ctx, subKey, string(blob), ttl)
	pipe.SAdd(ctx, s.typeKey(sub.TenantID, sub.EventType), sub.SubscriptionID)
	pipe.Expire(ctx, s.typeKey(sub.TenantID, sub.EventType), ttl)
	if sub.TimeoutAt > 0 {
		pipe.ZAdd(ctx, s.timeoutsKey(), redis.Z{Score: sub.TimeoutAt, Member: subKey})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("event subscription persist: %w", err)
	}
	return nil
}

// Unregister removes a subscription (idempotent). tenantID is required to
// address the namespaced key.
func (s *EventSubscriptions) Unregister(ctx context.Context, subscriptionID, tenantID string) error {
	if subscriptionID == "" {
		return nil
	}
	subKey := s.subKey(tenantID, subscriptionID)
	raw, err := s.client.Get(ctx, subKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	pipe := s.client.TxPipeline()
	pipe.Del(ctx, subKey)
	pipe.ZRem(ctx, s.timeoutsKey(), subKey)
	if raw != "" {
		var sub EventSubscription
		if json.Unmarshal([]byte(raw), &sub) == nil && sub.EventType != "" {
			pipe.SRem(ctx, s.typeKey(sub.TenantID, sub.EventType), subscriptionID)
		}
	}
	_, err = pipe.Exec(ctx)
	return err
}

// Get returns one subscription, or nil when absent.
func (s *EventSubscriptions) Get(ctx context.Context, tenantID, subscriptionID string) (*EventSubscription, error) {
	raw, err := s.client.Get(ctx, s.subKey(tenantID, subscriptionID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var sub EventSubscription
	if err := json.Unmarshal([]byte(raw), &sub); err != nil {
		return nil, fmt.Errorf("corrupt event subscription json: %w", err)
	}
	return &sub, nil
}

// SubscriptionIDs lists the subscription ids registered for an event type.
func (s *EventSubscriptions) SubscriptionIDs(ctx context.Context, tenantID, eventType string) ([]string, error) {
	return s.client.SMembers(ctx, s.typeKey(tenantID, eventType)).Result()
}

// HandleEvent resolves one business event into resume carriers (protocol
// §6.2 EventFilter semantics). It is exported so tests (and hosts reading
// events outside Pub/Sub) can inject a fake event directly.
func (w *Worker) HandleEvent(ctx context.Context, ev *Event) error {
	if ev == nil {
		return nil
	}
	if strings.TrimSpace(ev.EventType) == "" || ev.EventID == "" {
		return fmt.Errorf("event requires event_id and event_type (got id=%q type=%q)", ev.EventID, ev.EventType)
	}
	if ev.CorrelationID == "" {
		// No execution to wake — same skip as EventFilter.
		return nil
	}
	tenant := ev.TenantID
	ids, err := w.eventSubs.SubscriptionIDs(ctx, tenant, ev.EventType)
	if err != nil {
		return fmt.Errorf("event subscription lookup: %w", err)
	}
	if len(ids) == 0 {
		return nil
	}

	for _, subscriptionID := range ids {
		sub, err := w.eventSubs.Get(ctx, tenant, subscriptionID)
		if err != nil {
			w.logger.Warn("event subscription read failed, skipping", "subscription_id", subscriptionID, "error", err)
			continue
		}
		if sub == nil {
			continue
		}
		if sub.CorrelationID != "" && sub.CorrelationID != ev.CorrelationID {
			continue
		}
		if !subscriptionFilterMatches(sub.Filter, ev.Data) {
			continue
		}

		state, err := w.store.LoadExecutionState(ctx, tenant, ev.CorrelationID)
		if err != nil {
			w.logger.Warn("event dispatch: state load failed, skipping", "execution_id", ev.CorrelationID, "error", err)
			continue
		}
		if state == nil {
			continue
		}
		if !sameTenant(state.TenantID, tenant, w.cfg.DefaultTenantID) {
			w.logger.Warn("event tenant does not match execution state, dropping",
				"event_id", ev.EventID, "tenant", tenant, "state_tenant", state.TenantID, "execution_id", ev.CorrelationID)
			continue
		}
		if TerminalStatuses[state.Status] {
			// Terminal executions must not accumulate dead subscriptions.
			_ = w.eventSubs.Unregister(ctx, subscriptionID, tenant)
			continue
		}

		dedupKey := w.cfg.KeyPrefix + ":event_filter:dedup:" + ev.EventID + ":" + subscriptionID
		first, err := w.rdb.SetNX(ctx, dedupKey, "1", time.Hour).Result()
		if err != nil {
			return fmt.Errorf("event dedup claim: %w", err)
		}
		if !first {
			continue
		}

		body := map[string]any{
			"type":         "resume",
			"flow_id":      sub.FlowID,
			"execution_id": ev.CorrelationID,
			"resume_type":  "event",
			"timestamp":    nowISO(),
			"data": map[string]any{
				"event_id":        ev.EventID,
				"event_type":      ev.EventType,
				"event_data":      ev.Data,
				"subscription_id": subscriptionID,
			},
		}
		if tenant != "" {
			body["tenant_id"] = tenant
		}
		if _, err := w.queue.Enqueue(ctx, body, TaskSchemaVersion); err != nil {
			// #87: a claimed dedup key with no queued carrier would suppress
			// the event for an hour; release it so a retry/reconcile can work.
			w.rdb.Del(ctx, dedupKey)
			return fmt.Errorf("event resume enqueue: %w", err)
		}
		w.logger.Info("event matched, resume carrier enqueued",
			"event_id", ev.EventID, "event_type", ev.EventType,
			"execution_id", ev.CorrelationID, "subscription_id", subscriptionID)
	}
	return nil
}

// SweepEventTimeouts mirrors SubscriptionTimeoutChecker: subscriptions whose
// deadline elapsed enqueue a resume_type=timeout carrier (deduped for 1h).
// Terminal executions get their leftover subscriptions reclaimed.
func (w *Worker) SweepEventTimeouts(ctx context.Context) error {
	now := float64(time.Now().UnixNano()) / 1e9
	keys, err := w.rdb.ZRangeByScore(ctx, w.eventSubs.timeoutsKey(), &redis.ZRangeBy{
		Min:   "0",
		Max:   fmt.Sprintf("%.6f", now),
		Count: 100,
	}).Result()
	if err != nil {
		return fmt.Errorf("event timeout sweep: %w", err)
	}
	for _, subKey := range keys {
		raw, err := w.rdb.Get(ctx, subKey).Result()
		if errors.Is(err, redis.Nil) {
			w.rdb.ZRem(ctx, w.eventSubs.timeoutsKey(), subKey)
			continue
		}
		if err != nil {
			return fmt.Errorf("event timeout subscription read: %w", err)
		}
		var subscription EventSubscription
		if err := json.Unmarshal([]byte(raw), &subscription); err != nil {
			w.logger.Warn("event timeout: corrupt subscription, dropping from index", "key", subKey, "error", err)
			w.rdb.ZRem(ctx, w.eventSubs.timeoutsKey(), subKey)
			continue
		}
		if subscription.CorrelationID == "" {
			w.rdb.ZRem(ctx, w.eventSubs.timeoutsKey(), subKey)
			continue
		}
		state, err := w.store.LoadExecutionState(ctx, subscription.TenantID, subscription.CorrelationID)
		if err != nil {
			w.logger.Warn("event timeout: state load failed, will retry next sweep", "error", err)
			continue
		}
		if state == nil {
			// Python keeps the subscription when the state cannot be loaded;
			// a later sweep retries (the subscription TTL bounds the wait).
			continue
		}
		if TerminalStatuses[state.Status] {
			_ = w.eventSubs.Unregister(ctx, subscription.SubscriptionID, subscription.TenantID)
			continue
		}
		dedupKey := w.cfg.KeyPrefix + ":event_filter:timeout:" + subscription.SubscriptionID
		first, err := w.rdb.SetNX(ctx, dedupKey, "1", time.Hour).Result()
		if err != nil {
			return fmt.Errorf("event timeout dedup claim: %w", err)
		}
		if !first {
			continue
		}
		body := map[string]any{
			"type":         "resume",
			"flow_id":      subscription.FlowID,
			"execution_id": subscription.CorrelationID,
			"resume_type":  "timeout",
			"timestamp":    nowISO(),
			"data": map[string]any{
				"subscription_id": subscription.SubscriptionID,
				"event_type":      subscription.EventType,
				"node_id":         subscription.NodeID,
			},
		}
		if subscription.TenantID != "" {
			body["tenant_id"] = subscription.TenantID
		}
		if _, err := w.queue.Enqueue(ctx, body, TaskSchemaVersion); err != nil {
			w.rdb.Del(ctx, dedupKey)
			return fmt.Errorf("event timeout resume enqueue: %w", err)
		}
		w.logger.Info("subscription timed out, resume carrier enqueued",
			"subscription_id", subscription.SubscriptionID, "execution_id", subscription.CorrelationID)
	}
	return nil
}

// eventBridgeLoop turns Redis Pub/Sub business events into resume carriers.
// It reconnects with the consumer-loop backoff: a Redis restart must not
// permanently disable wake-ups in a long-lived worker.
func (w *Worker) eventBridgeLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		w.consumeEventChannel(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectBackoff):
		}
	}
}

// consumeEventChannel runs one Pub/Sub session until the subscription fails or
// the context is cancelled.
func (w *Worker) consumeEventChannel(ctx context.Context) {
	pubsub := w.rdb.PSubscribe(ctx, w.cfg.KeyPrefix+"*:events:*")
	defer pubsub.Close()
	// Wait for the subscription confirmation before consuming: PSubscribe
	// alone is fire-and-forget, and an event published in that window would
	// otherwise be missed with no reconciler in this build.
	if _, err := pubsub.Receive(ctx); err != nil {
		if ctx.Err() == nil {
			w.logger.Warn("event bridge subscription failed (will retry)", "error", err)
		}
		return
	}
	channel := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-channel:
			if !ok {
				w.logger.Warn("event bridge channel closed (will reconnect)")
				return
			}
			// The execution timeline channel {ns}:execution:events:{id} shares
			// the ":events:" shape with business events; it is not an EventBus
			// channel and must never be parsed as one.
			if strings.Contains(message.Channel, ":execution:events:") {
				continue
			}
			var ev Event
			if err := json.Unmarshal([]byte(message.Payload), &ev); err != nil {
				w.logger.Warn("event bridge: undecodable event payload", "channel", message.Channel, "error", err)
				continue
			}
			if err := w.HandleEvent(ctx, &ev); err != nil {
				w.logger.Warn("event bridge: dispatch failed", "channel", message.Channel, "error", err)
			}
		}
	}
}

// eventTimeoutLoop drives the subscription timeout checker (10s, plaita
// SubscriptionTimeoutChecker cadence).
func (w *Worker) eventTimeoutLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.SweepEventTimeouts(ctx); err != nil {
				w.logger.Warn("event timeout sweep failed", "error", err)
			}
		}
	}
}

// subscriptionFilterMatches applies plaita's dot-path filter to an event
// payload. An empty filter matches.
func subscriptionFilterMatches(filter map[string]any, data map[string]any) bool {
	if len(filter) == 0 {
		return true
	}
	for key, want := range filter {
		var current any = data
		for _, segment := range strings.Split(key, ".") {
			m, ok := current.(map[string]any)
			if !ok {
				return false
			}
			value, exists := m[segment]
			if !exists {
				return false
			}
			current = value
		}
		if !subscriptionValuesEqual(current, want) {
			return false
		}
	}
	return true
}

func subscriptionValuesEqual(a, b any) bool {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		return af == bf
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func sameTenant(a, b, defaultTenant string) bool {
	norm := func(v string) string {
		if v == "" {
			return defaultTenant
		}
		return v
	}
	return norm(a) == norm(b)
}
