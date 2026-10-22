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
	"log"
	"math"
	"os"
	"strconv"
	"strings"
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
	// (protocol §7). #60: the marker is Go-stamped ("-v2-go") instead of
	// sharing Python's "flow-definition-json-sortkeys-v2" — the canonical
	// semantics are byte-equal to Python's since #60, but a shared label on
	// a separately-maintained implementation is exactly the "same name,
	// different algorithm" blind spot that made upgrade_suspected useless
	// while this build mis-computed fingerprints. Rows written by pre-#60 Go
	// builds carry the old label AND a wrong hash; the label flip routes any
	// such in-flight row into the explainable upgrade_suspected=true branch
	// of the resume gate instead of looking like a silent definition change.
	// Operations remedy for those rows: allow_flow_hash_change=true on the
	// resume carrier. Python rows (label v2, identical hash) keep resuming
	// normally — hash equality passes the gate regardless of label.
	//
	// The capability manifest's ir_contract_version deliberately does NOT
	// participate in this gate: it moves with every manifest evolution
	// (supported-node changes etc.) without any fingerprint-relevant
	// semantic, it has no Python-side counterpart to compare against, and
	// algorithm identity is already carried by this marker. Recorded in
	// docs/WORKER_PROTOCOL.md §7.
	FlowHashAlgoV2 = "flow-definition-json-sortkeys-v2-go"

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

	// DefaultTaskMaxAgeSeconds is the message age-gate horizon (§1.2,
	// PLAITA_TASK_MAX_AGE_SECONDS=2592000); a negative config disables the
	// gate.
	DefaultTaskMaxAgeSeconds = 30 * 86400

	// DefaultTenantRunningTTLSeconds is the per-tenant start-slot entry TTL
	// (tenant_quota.py DEFAULT_RUNNING_TTL_SECONDS) — the crash self-heal for
	// a slot whose execution never reached a terminal write.
	DefaultTenantRunningTTLSeconds = 86400

	// DefaultQuotaDeferMax bounds over-quota tail re-enqueues per carrier
	// (§1.2, PLAITA_TENANT_QUOTA_DEFER_MAX=64) before quota_bypass admits it.
	DefaultQuotaDeferMax = 64

	// DefaultExecutionDeadline is the worker-level execution budget (#35,
	// plaita#267: DEFAULT_EXECUTION_DEADLINE_SECONDS=1800) — the wall-clock
	// ceiling for ONE engine invocation (one carrier's occupancy span of the
	// single consumer). A suspended execution waiting for its decision
	// carrier does not occupy the worker and does not accumulate budget;
	// every resume/continue carrier gets a fresh span (plaita distributed
	// strategy: 每步独立计,挂起等待不占预算).
	DefaultExecutionDeadline = 1800 * time.Second
)

// ExecutionDeadlineEnv is the operator override for the execution budget
// (seconds, decimals allowed). Invalid values (non-numeric / non-finite /
// ≤0) NEVER degrade to "unlimited": they fall back to the default with a
// warning (plaita#267 降级必须可观测). Disabling the budget is a deployment
// decision expressible only through the Config hook (a negative
// ExecutionDeadline), not through the environment.
const ExecutionDeadlineEnv = "PLAITA_EXECUTION_DEADLINE_SECONDS"

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

	// TaskMaxAgeSeconds: PLAITA_TASK_MAX_AGE_SECONDS (default 30 days) — the
	// task age-gate horizon (#29): carriers older than this are acked without
	// executing. A negative value disables the gate; 0 normalizes to the
	// default.
	TaskMaxAgeSeconds int

	// TenantMaxRunning: PLAITA_TENANT_MAX_RUNNING (default 0 = UNLIMITED) —
	// the global default cap of concurrent starts per tenant (#30). A
	// per-tenant entry in the {prefix}:tenant_quota HASH overrides it.
	// Enforcement requires an explicit value: unconfigured = no quota gate.
	TenantMaxRunning int
	// TenantRunningTTLSeconds: PLAITA_TENANT_RUNNING_TTL_SECONDS (default
	// 86400) — the admitted-start slot TTL (crash self-heal).
	TenantRunningTTLSeconds int
	// QuotaDeferMax: PLAITA_TENANT_QUOTA_DEFER_MAX (default 64) — over-quota
	// tail re-enqueues per carrier before it is admitted with quota_bypass.
	QuotaDeferMax int
	// DisableTenantQuota: PLAITA_DISABLE_TENANT_QUOTA=1 — rollback switch
	// bypassing the whole tenant-quota gate.
	DisableTenantQuota bool

	// ExecutionDeadline is the worker-level execution budget (#35, plaita#267):
	// when one engine invocation outlives it, the run is cancelled
	// cooperatively (no kill -9 — ctx-aware nodes bail out, the engine
	// fail-fasts at the next attempt boundary) and terminalized with the
	// named non-retryable error execution_deadline_exceeded. Zero means the
	// DefaultExecutionDeadline (1800s); a NEGATIVE value is the deployment
	// hook's explicit opt-out (the environment can never disable the gate —
	// an invalid or ≤0 env value falls back to the default with a warning).
	// An engine-level flow timeout (flow.timeout) composes as the stricter of
	// the two: whichever deadline fires first ends the execution.
	ExecutionDeadline time.Duration

	// ConsoleFlowSourceURL: PLAITA_CONSOLE_FLOW_SOURCE_URL (e.g.
	// http://console.internal:8080) — enables the #299 §2 read-through: a
	// Redis flow-definition miss is fetched from the console admin API
	// (GET /api/flows/{flow_id}/versions/{version}) before failing
	// ErrFlowNotFound. Empty (default) = feature off, miss semantics unchanged.
	ConsoleFlowSourceURL string
	// ConsoleFlowSourceAPIKey: PLAITA_CONSOLE_FLOW_SOURCE_API_KEY — sent as
	// X-Admin-API-Key (console PLAITA_CONSOLE_ADMIN_API_KEY). Empty means no
	// auth header (only viable against a local dev console with
	// PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN).
	ConsoleFlowSourceAPIKey string
	// ConsoleFlowSourceTimeout: per-request cap
	// (PLAITA_CONSOLE_FLOW_SOURCE_TIMEOUT_SECONDS, default 5s).
	ConsoleFlowSourceTimeout time.Duration
	// ConsoleFlowSourceNegativeTTL: negative-cache window for a deterministic
	// console miss (PLAITA_CONSOLE_FLOW_SOURCE_NEGATIVE_TTL_SECONDS, default
	// 30s) — repeated misses inside the window fail without re-hitting the
	// console. Transient failures are never negative-cached.
	ConsoleFlowSourceNegativeTTL time.Duration
	// ConsoleFlowSourceAttempts: read-through attempt cap for TRANSIENT
	// failures (PLAITA_CONSOLE_FLOW_SOURCE_ATTEMPTS, default 2; 1 = no retry),
	// with a fixed 250ms pause between attempts.
	ConsoleFlowSourceAttempts int
	// ConsoleFlowSourceWriteBack: PLAITA_CONSOLE_FLOW_SOURCE_WRITEBACK=1 —
	// write a pulled definition back into the engine storage shape so peers
	// stop missing too. OFF by default: the pull-through stays read-only with
	// respect to #303's「删除后目标侧不复活」 purge semantics; enabling it is
	// an explicit deployment decision.
	ConsoleFlowSourceWriteBack bool
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

		TaskMaxAgeSeconds:       envInt(getenv, "PLAITA_TASK_MAX_AGE_SECONDS", DefaultTaskMaxAgeSeconds),
		TenantMaxRunning:        envInt(getenv, "PLAITA_TENANT_MAX_RUNNING", 0),
		TenantRunningTTLSeconds: envInt(getenv, "PLAITA_TENANT_RUNNING_TTL_SECONDS", DefaultTenantRunningTTLSeconds),
		QuotaDeferMax:           envInt(getenv, "PLAITA_TENANT_QUOTA_DEFER_MAX", DefaultQuotaDeferMax),
		DisableTenantQuota:      getenv("PLAITA_DISABLE_TENANT_QUOTA") == "1",

		ExecutionDeadline: envSecondsDuration(getenv, ExecutionDeadlineEnv, DefaultExecutionDeadline),

		ConsoleFlowSourceURL:         getenv("PLAITA_CONSOLE_FLOW_SOURCE_URL"),
		ConsoleFlowSourceAPIKey:      getenv("PLAITA_CONSOLE_FLOW_SOURCE_API_KEY"),
		ConsoleFlowSourceTimeout:     time.Duration(envInt(getenv, "PLAITA_CONSOLE_FLOW_SOURCE_TIMEOUT_SECONDS", 5)) * time.Second,
		ConsoleFlowSourceNegativeTTL: time.Duration(envInt(getenv, "PLAITA_CONSOLE_FLOW_SOURCE_NEGATIVE_TTL_SECONDS", 30)) * time.Second,
		ConsoleFlowSourceAttempts:    envInt(getenv, "PLAITA_CONSOLE_FLOW_SOURCE_ATTEMPTS", 2),
		ConsoleFlowSourceWriteBack:   getenv("PLAITA_CONSOLE_FLOW_SOURCE_WRITEBACK") == "1",
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
	// Age gate: 0 (unset struct literal) means the protocol default; only a
	// negative value disables the gate (#29).
	if c.TaskMaxAgeSeconds == 0 {
		c.TaskMaxAgeSeconds = DefaultTaskMaxAgeSeconds
	}
	// Tenant quota (#30): defaults keep the deployment unlimited (cap 0) —
	// explicit configuration is what turns the gate on.
	if c.TenantRunningTTLSeconds <= 0 {
		c.TenantRunningTTLSeconds = DefaultTenantRunningTTLSeconds
	}
	if c.TenantMaxRunning < 0 {
		c.TenantMaxRunning = 0
	}
	if c.QuotaDeferMax <= 0 {
		c.QuotaDeferMax = DefaultQuotaDeferMax
	}
	// Execution budget (#35): a zero Config literal means "unset" and takes
	// the default; a negative value is the deployment hook's explicit
	// opt-out and is kept as-is (the env can never express it — envSeconds
	// rejects ≤0 back to the default, plaita#267's "never silently
	// unlimited").
	if c.ExecutionDeadline == 0 {
		c.ExecutionDeadline = DefaultExecutionDeadline
	}
	// Console read-through (#299 §2): URL empty keeps the whole feature off —
	// the remaining knobs only normalize their floors so a partial config
	// cannot produce a zero timeout / non-retrying client by accident.
	if c.ConsoleFlowSourceTimeout <= 0 {
		c.ConsoleFlowSourceTimeout = 5 * time.Second
	}
	if c.ConsoleFlowSourceNegativeTTL <= 0 {
		c.ConsoleFlowSourceNegativeTTL = 30 * time.Second
	}
	if c.ConsoleFlowSourceAttempts <= 0 {
		c.ConsoleFlowSourceAttempts = 2
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

// envSecondsDuration parses a positive finite seconds value into a Duration
// (plaita#267 default_execution_deadline_ms): invalid input falls back to the
// default WITH a warning — an operator typo must never turn the execution
// budget into "unlimited".
func envSecondsDuration(getenv func(string) string, key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(getenv(key))
	if raw == "" {
		return def
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) || f <= 0 {
		log.Printf("[worker] %s=%q is not a positive finite number of seconds; falling back to the default %s (never silently unlimited, plaita#267)", key, raw, def)
		return def
	}
	return time.Duration(f * float64(time.Second))
}

func getenvFallback2(getenv func(string) string, keys ...string) string {
	for _, k := range keys {
		if v := getenv(k); v != "" {
			return v
		}
	}
	return ""
}
