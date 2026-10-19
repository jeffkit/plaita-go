// Package worker implements the plaita-go runtime worker MVP: stream
// consumption (XREADGROUP + reclaim), fenced execution leases, flow IR
// loading from RedisFlowStorage, engine execution with event persistence,
// cancellation polling and worker heartbeats.
//
// The protocol authority is docs/WORKER_PROTOCOL.md (plaita Python worker
// field study); every protocol element references a chapter of that document.
package worker

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Protocol constants shared by the queue/lease/state layers (protocol §1.2,
// §2.2, §3.1, §3.3).
const (
	// TaskSchemaVersion is this build's envelope write version (§2.2
	// TASK_SCHEMA_VERSION=1). Messages with a HIGHER schema_version are
	// rejected into the DLQ; a missing field means v1.
	TaskSchemaVersion = 1

	// FlowHashAlgoV2 is the flow definition fingerprint algorithm marker
	// (protocol §7: flow-definition-json-sortkeys-v2).
	FlowHashAlgoV2 = "flow-definition-json-sortkeys-v2"

	// DefaultKeyPrefix is the default key namespace root (§3.1 ns = plaita).
	DefaultKeyPrefix = "plaita"

	// ShadowKeyPrefix rewrites every derived key into the shadow namespace so
	// a shadow run can never touch production keys (MVP requirement 8).
	ShadowKeyPrefix = "plaita-go-shadow"

	fenceKeyTTLSeconds = 7 * 86400 // §3.2 FENCE_KEY_TTL_SECONDS

	// counterTTLSeconds is the sliding TTL of every worker-side counter key
	// (§8.3: retry/handover/dedup counters all live 7 days).
	counterTTLSeconds = 7 * 86400

	// DefaultNodeAbortCode is plaita's DEFAULT_NODE_ABORT_CODE
	// (core/errors.py:102), used by worker-judged node-abort terminals (§5.4).
	DefaultNodeAbortCode = -520

	// AffinityHandoverMax bounds TaskNotForThisWorker handovers per execution
	// (§9.2, AFFINITY_HANDOVER_MAX=3).
	AffinityHandoverMax = 3

	// NodeRetryTotalMax is the fallback total retry ceiling per execution
	// (§9.1 layer 2, NODE_RETRY_TOTAL_MAX=100).
	NodeRetryTotalMax = 100

	// DeterministicFailureMax is the consecutive deterministic-node-failure
	// ceiling (§9.1 layer 3, DETERMINISTIC_FAILURE_MAX=12).
	DeterministicFailureMax = 12

	// ledgerSchemaVersion is the dispatch-ledger entry schema version (§6.3).
	ledgerSchemaVersion = 1
)

// Config carries every knob of the worker. All fields are overridable through
// environment variables so a shadow deployment can rewrite the full key space
// (queue/group/DLQ/state prefixes) without touching production keys.
type Config struct {
	// RedisURL: PLAITA_REDIS_URL / REDIS_URL (default redis://localhost:6379/0).
	RedisURL string
	// QueueName: PLAITA_QUEUE_NAME (default <KeyPrefix>:flow:queue).
	QueueName string
	// ConsumerGroup: PLAITA_CONSUMER_GROUP (default plaita-workers, or
	// <KeyPrefix>-workers when KeyPrefix is overridden for shadow runs).
	ConsumerGroup string
	// ConsumerName: PLAITA_CONSUMER_NAME (default worker-<pid>).
	ConsumerName string
	// InstanceID: PLAITA_INSTANCE_ID (default worker-<pid>); embedded in
	// lease holder tokens and the registry key (§3.3, §8.1).
	InstanceID string

	// ClaimMinIdleMS: PLAITA_CLAIM_MIN_IDLE_MS (default 60000) — pending
	// message XCLAIM reclaim threshold (§2.4).
	ClaimMinIdleMS int64
	// LeaseTTLSeconds: PLAITA_LEASE_TTL_SECONDS (default 120) (§3).
	LeaseTTLSeconds int
	// MaxDeliveries: PLAITA_MAX_DELIVERIES (default 5) — delivery ceiling
	// before dead-letter (§2.4).
	MaxDeliveries int
	// DLQKey: PLAITA_DLQ_KEY (default <queue>:dlq) (§2.1).
	DLQKey string
	// DLQMaxLen: PLAITA_DLQ_MAX_LEN (default 1000) — DLQ XTRIM retention.
	DLQMaxLen int64
	// ReadBlockMS: XREADGROUP BLOCK cap (default 1000, floor 100) (§1.2).
	ReadBlockMS int

	// KeyPrefix is the namespace root for every derived key (state, lease,
	// fence, cancel, registry, events, flow storage). Default plaita; shadow
	// runs set PLAITA_KEY_PREFIX=plaita-go-shadow.
	KeyPrefix string
	// DefaultTenantID: PLAITA_DEFAULT_TENANT_ID (default "default"). A task
	// tenant equal to the default (or empty) uses the unpartitioned namespace
	// (§3.1: ns = plaita for default/empty, plaita:{tenant} otherwise).
	DefaultTenantID string

	// EventTTLSeconds: business event storage TTL (PLAITA_EVENT_TTL_SECONDS,
	// default 7 days — protocol U1 marks the Python value as unverified, the
	// event_reconcile.py:9 comment says 7 days).
	EventTTLSeconds int
	// CancelPollMS: cancel flag polling interval while executing (default
	// 1000, plaita _cancel_poll_interval §4.2).
	CancelPollMS int
	// HeartbeatInterval: registry heartbeat period (default 10s, §8.1).
	HeartbeatInterval time.Duration
	// RegistryTTL: registry key TTL (default 30s, §8.1).
	RegistryTTL time.Duration
	// StateTTLDays: terminal execution state TTL in days (default 30,
	// PLAITA_EXECUTION_STATE_TTL_DAYS §5.2).
	StateTTLDays int
	// SweepInterval: acked-residue sweep period (default 300s, §2.4).
	SweepInterval time.Duration
	// DrainTimeout bounds the graceful in-flight wait on shutdown (§9.3,
	// default 30s; the full three-state drain is phase 2).
	DrainTimeout time.Duration
	// FlowCacheSize / FlowCacheTTL: flow definition TTL cache (§1.2:
	// 100 entries / 300s).
	FlowCacheSize int
	FlowCacheTTL  time.Duration
	// DispatchLedgerKey: PLAITA_DISPATCH_LEDGER_KEY (default
	// <KeyPrefix>:dispatch:ledger) — the append-only dispatch ledger stream
	// (§6.3). Overridden per key so a shadow run stays in its own namespace.
	DispatchLedgerKey string
	// DispatcherID: PLAITA_DISPATCHER_ID (default hostname) — the
	// "who dispatched" half of a ledger entry (§6.3).
	DispatcherID string
}

// DefaultConfig returns the protocol-default configuration for the current
// process (queue/DLQ names derived from KeyPrefix).
func DefaultConfig() *Config {
	return configFromEnv(os.Getenv)
}

func configFromEnv(getenv func(string) string) *Config {
	cfg := &Config{
		RedisURL:          getenvFallback2(getenv, "PLAITA_REDIS_URL", "REDIS_URL"),
		QueueName:         getenv("PLAITA_QUEUE_NAME"),
		ConsumerGroup:     getenv("PLAITA_CONSUMER_GROUP"),
		ConsumerName:      getenv("PLAITA_CONSUMER_NAME"),
		InstanceID:        getenv("PLAITA_INSTANCE_ID"),
		ClaimMinIdleMS:    int64(envInt(getenv, "PLAITA_CLAIM_MIN_IDLE_MS", 60000)),
		LeaseTTLSeconds:   envInt(getenv, "PLAITA_LEASE_TTL_SECONDS", 120),
		MaxDeliveries:     envInt(getenv, "PLAITA_MAX_DELIVERIES", 5),
		DLQKey:            getenv("PLAITA_DLQ_KEY"),
		DLQMaxLen:         int64(envInt(getenv, "PLAITA_DLQ_MAX_LEN", 1000)),
		ReadBlockMS:       envInt(getenv, "PLAITA_READ_BLOCK_MS", 1000),
		KeyPrefix:         getenv("PLAITA_KEY_PREFIX"),
		DefaultTenantID:   getenv("PLAITA_DEFAULT_TENANT_ID"),
		EventTTLSeconds:   envInt(getenv, "PLAITA_EVENT_TTL_SECONDS", 7*86400),
		CancelPollMS:      envInt(getenv, "PLAITA_CANCEL_POLL_MS", 1000),
		HeartbeatInterval: time.Duration(envInt(getenv, "PLAITA_HEARTBEAT_INTERVAL_SECONDS", 10)) * time.Second,
		RegistryTTL:       time.Duration(envInt(getenv, "PLAITA_REGISTRY_TTL_SECONDS", 30)) * time.Second,
		StateTTLDays:      envInt(getenv, "PLAITA_EXECUTION_STATE_TTL_DAYS", 30),
		SweepInterval:     time.Duration(envInt(getenv, "PLAITA_SWEEP_INTERVAL_SECONDS", 300)) * time.Second,
		DrainTimeout:      time.Duration(envFloat(getenv, "PLAITA_WORKER_DRAIN_TIMEOUT", 30.0) * float64(time.Second)),
		FlowCacheSize:     envInt(getenv, "PLAITA_FLOW_CACHE_SIZE", 100),
		FlowCacheTTL:      time.Duration(envInt(getenv, "PLAITA_FLOW_CACHE_TTL_SECONDS", 300)) * time.Second,
		DispatchLedgerKey: getenv("PLAITA_DISPATCH_LEDGER_KEY"),
		DispatcherID:      getenv("PLAITA_DISPATCHER_ID"),
	}
	cfg.normalize()
	return cfg
}

// normalize fills the derived defaults. Order matters: KeyPrefix first, then
// the queue/group/DLQ names derive from it, so a shadow run only needs
// PLAITA_KEY_PREFIX=plaita-go-shadow to stay off every production key.
func (c *Config) normalize() {
	if c.KeyPrefix == "" {
		c.KeyPrefix = DefaultKeyPrefix
	}
	if c.RedisURL == "" {
		c.RedisURL = "redis://localhost:6379/0"
	}
	if c.QueueName == "" {
		c.QueueName = c.KeyPrefix + ":flow:queue"
	}
	if c.ConsumerGroup == "" {
		// Shadow derivation: an overridden namespace also moves the default
		// group so shadow consumers never join the production group.
		if c.KeyPrefix == DefaultKeyPrefix {
			c.ConsumerGroup = "plaita-workers"
		} else {
			c.ConsumerGroup = c.KeyPrefix + "-workers"
		}
	}
	if c.InstanceID == "" {
		c.InstanceID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	if c.ConsumerName == "" {
		c.ConsumerName = c.InstanceID
	}
	if c.DLQKey == "" {
		c.DLQKey = c.QueueName + ":dlq"
	}
	if c.DefaultTenantID == "" {
		c.DefaultTenantID = "default"
	}
	if c.ClaimMinIdleMS < 0 {
		c.ClaimMinIdleMS = 60000
	}
	if c.LeaseTTLSeconds <= 0 {
		c.LeaseTTLSeconds = 120
	}
	if c.MaxDeliveries <= 0 {
		c.MaxDeliveries = 5
	}
	if c.DLQMaxLen <= 0 {
		c.DLQMaxLen = 1000
	}
	if c.ReadBlockMS < 100 { // §1.2 read-block-ms floor
		c.ReadBlockMS = 100
	}
	if c.EventTTLSeconds <= 0 {
		c.EventTTLSeconds = 7 * 86400
	}
	if c.CancelPollMS <= 0 {
		c.CancelPollMS = 1000
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.RegistryTTL <= 0 {
		c.RegistryTTL = 30 * time.Second
	}
	if c.StateTTLDays <= 0 {
		c.StateTTLDays = 30
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = 300 * time.Second
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 30 * time.Second
	}
	if c.FlowCacheSize <= 0 {
		c.FlowCacheSize = 100
	}
	if c.FlowCacheTTL <= 0 {
		c.FlowCacheTTL = 300 * time.Second
	}
	if c.DispatchLedgerKey == "" {
		c.DispatchLedgerKey = c.KeyPrefix + ":dispatch:ledger"
	}
	if c.DispatcherID == "" {
		c.DispatcherID = hostnameOrUnknown()
	}
}

// Namespace returns the tenant-routed key namespace (§3.1): the plain prefix
// for the default/empty tenant, prefix:{tenant} otherwise.
func (c *Config) Namespace(tenantID string) string {
	if tenantID == "" || tenantID == c.DefaultTenantID {
		return c.KeyPrefix
	}
	return c.KeyPrefix + ":" + tenantID
}

// ApplyShadow rewrites the namespace root into the shadow prefix. Explicit
// per-key overrides (queue/group/DLQ) set BEFORE this call win; everything
// derived is recomputed so no key can fall back to production naming.
func (c *Config) ApplyShadow() {
	c.KeyPrefix = ShadowKeyPrefix
	if c.QueueName == DefaultKeyPrefix+":flow:queue" || c.QueueName == ShadowKeyPrefix+":flow:queue" {
		c.QueueName = ShadowKeyPrefix + ":flow:queue"
	}
	if c.ConsumerGroup == "plaita-workers" || c.ConsumerGroup == ShadowKeyPrefix+"-workers" {
		c.ConsumerGroup = ShadowKeyPrefix + "-workers"
	}
	if c.DLQKey == c.QueueName+":dlq" || c.DLQKey == DefaultKeyPrefix+":flow:queue:dlq" {
		c.DLQKey = c.QueueName + ":dlq"
	}
	if c.DispatchLedgerKey == DefaultKeyPrefix+":dispatch:ledger" ||
		c.DispatchLedgerKey == ShadowKeyPrefix+":dispatch:ledger" {
		c.DispatchLedgerKey = ShadowKeyPrefix + ":dispatch:ledger"
	}
}

// hostnameOrUnknown is dispatcher_id_from_env's fallback (§6.3).
func hostnameOrUnknown() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown-dispatcher"
}

func envInt(getenv func(string) string, key string, def int) int {
	v := getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envFloat(getenv func(string) string, key string, def float64) float64 {
	v := getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func getenvFallback2(getenv func(string) string, keys ...string) string {
	for _, k := range keys {
		if v := getenv(k); v != "" {
			return v
		}
	}
	return ""
}
