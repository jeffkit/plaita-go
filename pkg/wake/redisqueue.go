package wake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

// RedisQueueSource is the Go counterpart of Python plaita RedisQueueService.
// Two faces:
//
//  1. Dispatch bridge: consume `{prefix}:redis_queue:queue` (the worker's §5.2
//     RPUSH) — each task carries redis_config (the USER redis: host/port/db/
//     password) + queue_config {name, type: list|stream|pubsub, message_format}
//     and starts a listener on that user queue.
//  2. Listener: per task, poll/subscribe the user queue and publish a
//     `redis_message` resume event per message (the redis_queue node's fixed
//     event contract).
//
// Completion semantics mirror the Python listener: the task_complete key
// (`task_complete:{execution_id}`, 1h TTL on the user redis) short-circuits
// the loop; after a message was processed, the listener keeps consuming until
// `max_wait_after_message` (default 10s) of idleness, then marks the task
// complete and returns. Python additionally stops early when the flow has no
// active subscriptions left (subscription_storage lookup); this engine lacks
// a correlation index, so the idle window alone terminates the listen — a
// documented, strictly-lower-efficiency divergence (same external behavior:
// the execution resumes on the first event either way).
//
// The queue entry stays un-acked for the whole listen (at-least-once): a
// service crash mid-listen leaves the entry for the next start to re-consume.
type RedisQueueSource struct {
	*listSource

	cfg                 *Config
	pub                 *worker.EventPublisher
	dialMaxRetries      int
	dialBackoffBase     time.Duration
	dialBackoffMax      time.Duration
	maxWaitAfterMessage time.Duration
	maxConsecutiveErrs  int
}

func NewRedisQueueSource(cfg *Config, rdb *redis.Client) (*RedisQueueSource, error) {
	core := newListSource(SourceRedisQueue, cfg, rdb)
	s := &RedisQueueSource{
		listSource:          core,
		cfg:                 cfg,
		pub:                 worker.NewEventPublisher(rdb, &worker.Config{KeyPrefix: cfg.KeyPrefix, DefaultTenantID: cfg.DefaultTenantID, EventTTLSeconds: cfg.EventTTLSeconds}),
		dialMaxRetries:      5,
		dialBackoffBase:     time.Second,
		dialBackoffMax:      60 * time.Second,
		maxWaitAfterMessage: 10 * time.Second,
		maxConsecutiveErrs:  5,
	}
	s.publishFn = s.pub.Publish
	core.validate = func(task map[string]any) bool {
		return str(task["node_id"]) != "" && str(task["event_type"]) != ""
	}
	core.handle = s.handleListen
	return s, nil
}

// userRedis dials the task's user redis with retry (Python
// _create_redis_client_with_retry: max 5 retries, exponential backoff capped).
func (s *RedisQueueSource) userRedis(ctx context.Context, rc map[string]any) (*redis.Client, error) {
	host := strOr(rc["host"], "localhost")
	port := fmt.Sprintf("%d", int(numOr(rc["port"], 6379)))
	addr := host + ":" + port
	socketTimeout := time.Duration(numOr(rc["socket_timeout"], 5)) * time.Second
	var lastErr error
	for attempt := 0; attempt <= s.dialMaxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		client := redis.NewClient(&redis.Options{
			Addr:         addr,
			Password:     str(rc["password"]),
			DB:           int(numOr(rc["db"], 0)),
			DialTimeout:  time.Duration(numOr(rc["socket_connect_timeout"], 5)) * time.Second,
			ReadTimeout:  socketTimeout,
			WriteTimeout: socketTimeout,
		})
		pingCtx, cancel := context.WithTimeout(ctx, socketTimeout)
		err := client.Ping(pingCtx).Err()
		cancel()
		if err == nil {
			return client, nil
		}
		_ = client.Close()
		lastErr = err
		if attempt < s.dialMaxRetries {
			delay := time.Duration(float64(s.dialBackoffBase) * pow2(attempt))
			if delay > s.dialBackoffMax {
				delay = s.dialBackoffMax
			}
			s.logger.Warn("redis_queue user dial failed; retrying", "addr", addr,
				"attempt", fmt.Sprintf("%d/%d", attempt+1, s.dialMaxRetries+1), "delay", delay, "error", err)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return nil, lastErr
}

func (s *RedisQueueSource) handleListen(ctx context.Context, raw string, task map[string]any) bool {
	rc, _ := task["redis_config"].(map[string]any)
	qc, _ := task["queue_config"].(map[string]any)
	if rc == nil || qc == nil {
		s.logger.Error("redis_queue task missing redis_config/queue_config (dropped)", "raw", truncate(raw, 200))
		return true
	}
	executionID := str(task["execution_id"])

	client, err := s.userRedis(ctx, rc)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		s.logger.Error("redis_queue user dial failed permanently; error event + ack", "execution_id", executionID, "error", err)
		s.publishErrorEvent(ctx, task, err.Error())
		return true
	}
	defer func() { _ = client.Close() }()

	completeKey := "task_complete:" + executionID
	if err := client.SetEx(ctx, completeKey, "0", time.Hour).Err(); err != nil {
		s.logger.Warn("redis_queue task_complete key write failed (continuing)", "error", err)
	}

	queueType := strOr(qc["type"], "list")
	queueName := str(qc["name"])
	format := strOr(qc["message_format"], "json")
	lc, _ := task["listen_config"].(map[string]any)
	maxWait := s.maxWaitAfterMessage
	if v := numOr(lc["max_wait_after_message"], 0); v > 0 {
		maxWait = time.Duration(v) * time.Second
	}

	switch queueType {
	case "list":
		return s.listenList(ctx, client, task, completeKey, queueName, format, maxWait)
	case "stream":
		return s.listenStream(ctx, client, task, completeKey, queueName, format, maxWait)
	case "pubsub":
		return s.listenPubsub(ctx, client, task, completeKey, queueName, format, maxWait)
	default:
		s.logger.Error("redis_queue unsupported queue type (dropped)", "type", queueType)
		return true
	}
}

// listenList mirrors Python _listen_list_queue: BLPOP with a 2s timeout, one
// resume event per message, terminate on complete flag / idle window /
// consecutive-error ceiling.
func (s *RedisQueueSource) listenList(ctx context.Context, client *redis.Client, task map[string]any, completeKey, queueName, format string, maxWait time.Duration) bool {
	var lastMessage time.Time
	processed := false
	consecutiveErrs := 0
	for {
		if ctx.Err() != nil {
			return false
		}
		if s.completeFlagSet(ctx, client, completeKey) {
			return true
		}
		if processed && time.Since(lastMessage) > maxWait {
			_ = client.SetEx(ctx, completeKey, "1", time.Hour).Err()
			return true
		}
		result, err := client.BLPop(ctx, 2*time.Second, queueName).Result()
		if errors.Is(err, redis.Nil) {
			consecutiveErrs = 0
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			consecutiveErrs++
			if consecutiveErrs >= s.maxConsecutiveErrs {
				s.logger.Error("redis_queue list listener giving up after consecutive errors",
					"queue", queueName, "errors", consecutiveErrs, "error", err)
				return true
			}
			s.logger.Warn("redis_queue list listener error", "queue", queueName, "error", err)
			continue
		}
		if len(result) < 2 {
			continue
		}
		message := result[1]
		ev := s.buildMessageEvent(task, map[string]any{
			"trigger_type": "redis_message",
			"queue_type":   "list",
			"queue_name":   queueName,
			"message":      parseMessage(message, format),
			"raw_message":  message,
			"timestamp":    time.Now().UnixMilli(),
			"success":      true,
		})
		if err := s.publishFn(ctx, ev); err != nil {
			if ctx.Err() != nil {
				return false
			}
			// Trigger not confirmed: the entry stays parked and the whole
			// listen restarts next cycle (the message was already consumed
			// from the user queue — BLPOP semantics, same as Python).
			s.logger.Error("redis_queue resume event publish failed; entry kept", "error", err)
			return false
		}
		processed = true
		lastMessage = time.Now()
		consecutiveErrs = 0
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *RedisQueueSource) listenStream(ctx context.Context, client *redis.Client, task map[string]any, completeKey, streamName, format string, maxWait time.Duration) bool {
	var lastMessage time.Time
	processed := false
	consecutiveErrs := 0
	lastID := "$"
	for {
		if ctx.Err() != nil {
			return false
		}
		if s.completeFlagSet(ctx, client, completeKey) {
			return true
		}
		if processed && time.Since(lastMessage) > maxWait {
			_ = client.SetEx(ctx, completeKey, "1", time.Hour).Err()
			return true
		}
		streams, err := client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamName, lastID},
			Block:   2 * time.Second,
		}).Result()
		if errors.Is(err, redis.Nil) {
			consecutiveErrs = 0
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			consecutiveErrs++
			if consecutiveErrs >= s.maxConsecutiveErrs {
				s.logger.Error("redis_queue stream listener giving up after consecutive errors",
					"stream", streamName, "errors", consecutiveErrs, "error", err)
				return true
			}
			s.logger.Warn("redis_queue stream listener error", "stream", streamName, "error", err)
			continue
		}
		for _, st := range streams {
			for _, msg := range st.Messages {
				fields := map[string]any{}
				for k, v := range msg.Values {
					fields[k] = v
				}
				ev := s.buildMessageEvent(task, map[string]any{
					"trigger_type": "redis_message",
					"queue_type":   "stream",
					"stream_name":  streamName,
					"message_id":   msg.ID,
					"message":      parseStreamFields(fields, format),
					"raw_fields":   fields,
					"timestamp":    time.Now().UnixMilli(),
					"success":      true,
				})
				if err := s.publishFn(ctx, ev); err != nil {
					if ctx.Err() != nil {
						return false
					}
					s.logger.Error("redis_queue resume event publish failed; entry kept", "error", err)
					return false
				}
				lastID = msg.ID
				processed = true
				lastMessage = time.Now()
				consecutiveErrs = 0
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}

func (s *RedisQueueSource) listenPubsub(ctx context.Context, client *redis.Client, task map[string]any, completeKey, channelName, format string, maxWait time.Duration) bool {
	sub := client.Subscribe(ctx, channelName)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		s.logger.Error("redis_queue pubsub subscribe failed", "channel", channelName, "error", err)
		s.publishErrorEvent(ctx, task, err.Error())
		return true
	}
	var lastMessage time.Time
	processed := false
	for {
		if ctx.Err() != nil {
			return false
		}
		if s.completeFlagSet(ctx, client, completeKey) {
			return true
		}
		if processed && time.Since(lastMessage) > maxWait {
			_ = client.SetEx(ctx, completeKey, "1", time.Hour).Err()
			return true
		}
		msg, err := sub.ReceiveTimeout(ctx, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			continue // receive timeout — normal idle path
		}
		message, ok := msg.(*redis.Message)
		if !ok {
			continue
		}
		ev := s.buildMessageEvent(task, map[string]any{
			"trigger_type": "redis_message",
			"queue_type":   "pubsub",
			"channel_name": channelName,
			"message":      parseMessage(message.Payload, format),
			"raw_message":  message.Payload,
			"timestamp":    time.Now().UnixMilli(),
			"success":      true,
		})
		if err := s.publishFn(ctx, ev); err != nil {
			if ctx.Err() != nil {
				return false
			}
			s.logger.Error("redis_queue resume event publish failed; entry kept", "error", err)
			return false
		}
		processed = true
		lastMessage = time.Now()
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *RedisQueueSource) completeFlagSet(ctx context.Context, client *redis.Client, completeKey string) bool {
	v, err := client.Get(ctx, completeKey).Result()
	if err != nil {
		return false
	}
	return v == "1"
}

// buildMessageEvent wraps one user-queue message into the redis_message
// resume event (trusted tenant top-level, correlation_id = execution_id).
func (s *RedisQueueSource) buildMessageEvent(task map[string]any, data map[string]any) *worker.Event {
	tenant := strOr(task["tenant_id"], s.cfg.DefaultTenantID)
	executionID := str(task["execution_id"])
	data["node_id"] = task["node_id"]
	data["execution_id"] = executionID
	data["flow_id"] = task["flow_id"]
	data["tenant_id"] = tenant
	now := time.Now().UnixMilli()
	data["timestamp"] = now
	return &worker.Event{
		EventID:       uuid.NewString(),
		EventType:     str(task["event_type"]),
		Data:          data,
		Timestamp:     float64(now) / 1000.0, // epoch seconds (Python Event.timestamp convention)
		CorrelationID: executionID,
		TenantID:      tenant,
	}
}

func (s *RedisQueueSource) publishErrorEvent(ctx context.Context, task map[string]any, errMsg string) {
	ev := s.buildMessageEvent(task, map[string]any{
		"trigger_type":  "redis_error",
		"error_message": errMsg,
		"success":       false,
	})
	bgCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.publishFn(bgCtx, ev); err != nil {
		s.logger.Warn("redis_queue error event publish failed", "error", err)
	}
}

// parseMessage mirrors Python _parse_message: json → decode (fallback raw on
// failure), text → string, anything else → raw string.
func parseMessage(raw, format string) any {
	if format == "json" {
		var v any
		if err := json.Unmarshal([]byte(raw), &v); err == nil {
			return v
		}
		return raw // decode failure → raw passthrough (Python same)
	}
	return raw
}

// parseStreamFields mirrors Python _parse_stream_message: json format reads
// the `data` field; missing/unparsable → the raw fields map.
func parseStreamFields(fields map[string]any, format string) any {
	if format == "json" {
		if data, ok := fields["data"].(string); ok && data != "" {
			var v any
			if err := json.Unmarshal([]byte(data), &v); err == nil {
				return v
			}
		}
		return fields
	}
	return fields
}

func numOr(v any, def float64) float64 {
	if f := num(v); f != 0 {
		return f
	}
	return def
}

func pow2(n int) float64 {
	out := 1.0
	for i := 0; i < n && out < 1e6; i++ {
		out *= 2
	}
	return out
}
