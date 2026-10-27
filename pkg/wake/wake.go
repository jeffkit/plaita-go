// Package wake implements the plaita wake services: companion processes that
// consume the suspend-service queues (`{prefix}:{subtype}:queue`, §5.2) and
// turn each due signal into a resume event (`{ns}:events:{event_type}`).
//
// Semantics authority: Python plaita/server/services/ — DelayService's
// list→ZSET two-phase scheduling (C4-2) with the #35 retry/backoff/dead-letter
// trigger-confirmation semantics. Protocol authority: plaita-go pkg/worker —
// the queue key names, the service task body (service_config fields +
// execution_id + tenant_id, worker.go rpushServiceTask) and the resume event
// wire shape (worker.EventPublisher double-write: SETEX {ns}:event:events +
// ZADD {ns}:event:types + PUBLISH {ns}:events) are byte-compatible with both
// the Go worker and the Python services, so mixed fleets share one Redis and
// either side's wake process can serve the other's workers.
//
// Registry: each source registers `{prefix}:registry:{source}:{instance_id}`
// (SETEX, TTL 30s, 10s heartbeat) — the roll-call data source for the Python
// cluster-tier acceptance gate (plaita:registry:{service_type}:* live keys).
package wake

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Wake source identities (registry key segments = {prefix}:{source}:queue
// subtypes the worker dispatches to).
const (
	SourceDelay      = "delay"
	SourceApproval   = "approval"
	SourceRedisQueue = "redis_queue"
)

// Sources returns the implemented wake sources.
func Sources() []string { return []string{SourceDelay, SourceApproval, SourceRedisQueue} }

// Service is one wake source's runtime face.
type Service interface {
	// Source is the wake source identity (the registry key segment and the
	// {prefix}:{source}:queue subtype the worker dispatches to).
	Source() string
	// Run drives the source until ctx is done; it returns after a graceful
	// stop (registry tombstoned).
	Run(ctx context.Context) error
	// Done closes once the source has fully stopped.
	Done() <-chan struct{}
}

// NewService builds an implemented wake source by name.
func NewService(source string, cfg *Config, rdb *redis.Client) (Service, error) {
	switch source {
	case SourceDelay:
		return NewDelayService(cfg, rdb)
	case SourceApproval:
		return NewApprovalSource(cfg, rdb)
	case SourceRedisQueue:
		return NewRedisQueueSource(cfg, rdb)
	default:
		return nil, fmt.Errorf("unknown wake source %q (implemented: %s)", source, strings.Join(Sources(), ","))
	}
}

// Config carries the wake process settings. Env names follow the worker's
// conventions (PLAITA_*); the delay-specific knobs mirror the Python
// DelayService constants.
type Config struct {
	// RedisURL: PLAITA_REDIS_URL / REDIS_URL (default
	// redis://localhost:6379/0) — same env pair the worker uses.
	RedisURL string
	// KeyPrefix: PLAITA_KEY_PREFIX (default "plaita") — the namespace root
	// for every key this process touches.
	KeyPrefix string
	// InstanceID: PLAITA_INSTANCE_ID (default delay-<pid>) — registry key
	// suffix; must be unique per running source instance.
	InstanceID string
	// DefaultTenantID: PLAITA_DEFAULT_TENANT_ID (default "default").
	DefaultTenantID string
	// EventTTLSeconds: PLAITA_EVENT_TTL_SECONDS (default 7d) — business
	// event storage TTL (worker.EventPublisher double-write).
	EventTTLSeconds int
	// DelayQueue: PLAITA_DELAY_QUEUE explicit override for the delay queue
	// key (default {prefix}:delay:queue). An empty string value is refused
	// (identity/routing — Python B2R31-03), unset it for the default.
	DelayQueue string
	// PollInterval: PLAITA_DELAY_POLL_INTERVAL_SECONDS (default 0.5s) —
	// convey + due poll cadence (Python DELAY_POLL_INTERVAL_SECONDS).
	PollInterval time.Duration
	// HeartbeatInterval / RegistryTTL: PLAITA_HEARTBEAT_INTERVAL_SECONDS
	// (10s) / PLAITA_REGISTRY_TTL_SECONDS (30s).
	HeartbeatInterval time.Duration
	RegistryTTL       time.Duration
	// MaxTriggerAttempts: PLAITA_DELAY_MAX_TRIGGER_ATTEMPTS (default 5,
	// Python DELAY_MAX_TRIGGER_ATTEMPTS) — publish failures before the entry
	// moves to the dead-letter list.
	MaxTriggerAttempts int
	// RetryBackoffBase/Max: 1s / 30s (Python DELAY_RETRY_BACKOFF_*), the n-th
	// failure waits min(base·2^(n-1), max).
	RetryBackoffBase time.Duration
	RetryBackoffMax  time.Duration
	// ConveyBatch/DueBatch: 200/50 (Python DELAY_CONVEY_BATCH/DELAY_DUE_BATCH).
	ConveyBatch int
	DueBatch    int

	Logger *slog.Logger
}

// ConfigFromEnv builds the wake config from the process environment.
func ConfigFromEnv() *Config {
	getenv := func(k string) string { return os.Getenv(k) }
	return configFromGetter(getenv)
}

func configFromGetter(getenv func(string) string) *Config {
	cfg := &Config{
		RedisURL:           getenv("PLAITA_REDIS_URL"),
		KeyPrefix:          getenv("PLAITA_KEY_PREFIX"),
		InstanceID:         getenv("PLAITA_INSTANCE_ID"),
		DefaultTenantID:    getenv("PLAITA_DEFAULT_TENANT_ID"),
		EventTTLSeconds:    envInt(getenv, "PLAITA_EVENT_TTL_SECONDS", 7*86400),
		DelayQueue:         getenv("PLAITA_DELAY_QUEUE"),
		PollInterval:       envMillis(getenv, "PLAITA_DELAY_POLL_INTERVAL_SECONDS", 500),
		HeartbeatInterval:  envMillis(getenv, "PLAITA_HEARTBEAT_INTERVAL_SECONDS", 10_000),
		RegistryTTL:        envMillis(getenv, "PLAITA_REGISTRY_TTL_SECONDS", 30_000),
		MaxTriggerAttempts: envInt(getenv, "PLAITA_DELAY_MAX_TRIGGER_ATTEMPTS", 5),
		RetryBackoffBase:   envMillis(getenv, "PLAITA_DELAY_RETRY_BACKOFF_BASE_SECONDS", 1_000),
		RetryBackoffMax:    envMillis(getenv, "PLAITA_DELAY_RETRY_BACKOFF_MAX_SECONDS", 30_000),
		ConveyBatch:        envInt(getenv, "PLAITA_DELAY_CONVEY_BATCH", 200),
		DueBatch:           envInt(getenv, "PLAITA_DELAY_DUE_BATCH", 50),
	}
	cfg.normalize()
	return cfg
}

func (c *Config) normalize() {
	if c.RedisURL == "" {
		if v := os.Getenv("REDIS_URL"); v != "" {
			c.RedisURL = v
		} else {
			c.RedisURL = "redis://localhost:6379/0"
		}
	}
	if c.KeyPrefix == "" {
		c.KeyPrefix = "plaita"
	}
	if c.InstanceID == "" {
		c.InstanceID = fmt.Sprintf("delay-%d", os.Getpid())
	}
	if c.DefaultTenantID == "" {
		c.DefaultTenantID = "default"
	}
	if c.EventTTLSeconds <= 0 {
		c.EventTTLSeconds = 7 * 86400
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.RegistryTTL <= 0 {
		c.RegistryTTL = 30 * time.Second
	}
	if c.RegistryTTL < time.Second {
		// go-redis SETEX is second-granular: anything below 1s silently
		// truncates and the roll-call record would flap.
		c.RegistryTTL = time.Second
	}
	if c.MaxTriggerAttempts < 1 {
		c.MaxTriggerAttempts = 5
	}
	if c.RetryBackoffBase <= 0 {
		c.RetryBackoffBase = time.Second
	}
	if c.RetryBackoffMax < c.RetryBackoffBase {
		c.RetryBackoffMax = 30 * time.Second
	}
	if c.ConveyBatch <= 0 {
		c.ConveyBatch = 200
	}
	if c.DueBatch <= 0 {
		c.DueBatch = 50
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// DelayQueueKey derives the delay queue key: explicit override wins; an
// empty-string override is a named configuration error (an identity/routing
// value must never silently become the default — Python B2R31-03).
func (c *Config) DelayQueueKey() (string, error) {
	if v, ok := os.LookupEnv("PLAITA_DELAY_QUEUE"); ok && strings.TrimSpace(v) == "" {
		return "", fmt.Errorf("PLAITA_DELAY_QUEUE: empty value is not allowed (identity/routing); unset it to use the default")
	}
	if c.DelayQueue != "" {
		return c.DelayQueue, nil
	}
	return c.KeyPrefix + ":delay:queue", nil
}

func envInt(getenv func(string) string, key string, def int) int {
	raw := getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

// envMillis reads fractional seconds ("0.5") into a duration.
func envMillis(getenv func(string) string, key string, defMS int64) time.Duration {
	raw := getenv(key)
	if raw == "" {
		return time.Duration(defMS) * time.Millisecond
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil || f <= 0 {
		return time.Duration(defMS) * time.Millisecond
	}
	return time.Duration(f * float64(time.Second))
}
