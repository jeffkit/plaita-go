package worker

import (
	"testing"
	"time"
)

// TestConfigShadowDerivation: PLAITA_KEY_PREFIX rewrites the whole key space
// (MVP requirement 8) — queue, group, DLQ, state keys.
func TestConfigShadowDerivation(t *testing.T) {
	env := map[string]string{"PLAITA_KEY_PREFIX": ShadowKeyPrefix}
	cfg := configFromEnv(func(k string) string { return env[k] })
	if cfg.QueueName != ShadowKeyPrefix+":flow:queue" {
		t.Fatalf("queue = %q", cfg.QueueName)
	}
	if cfg.ConsumerGroup != ShadowKeyPrefix+"-workers" {
		t.Fatalf("group = %q", cfg.ConsumerGroup)
	}
	if cfg.DLQKey != ShadowKeyPrefix+":flow:queue:dlq" {
		t.Fatalf("dlq = %q", cfg.DLQKey)
	}
	if cfg.DispatchLedgerKey != ShadowKeyPrefix+":dispatch:ledger" {
		t.Fatalf("ledger = %q", cfg.DispatchLedgerKey)
	}
}

// TestConfigDefaults: protocol defaults (§1.2).
func TestConfigDefaults(t *testing.T) {
	cfg := configFromEnv(func(string) string { return "" })
	if cfg.RedisURL != "redis://localhost:6379/0" {
		t.Fatalf("redis url = %q", cfg.RedisURL)
	}
	if cfg.QueueName != DefaultKeyPrefix+":flow:queue" {
		t.Fatalf("queue = %q", cfg.QueueName)
	}
	if cfg.ConsumerGroup != "plaita-workers" {
		t.Fatalf("group = %q", cfg.ConsumerGroup)
	}
	if cfg.ClaimMinIdleMS != 60000 || cfg.LeaseTTLSeconds != 120 || cfg.MaxDeliveries != 5 {
		t.Fatalf("queue/lease defaults drifted: %+v", cfg)
	}
	if cfg.DLQKey != cfg.QueueName+":dlq" || cfg.DLQMaxLen != 1000 {
		t.Fatalf("dlq defaults drifted: %q %d", cfg.DLQKey, cfg.DLQMaxLen)
	}
	if cfg.DispatchLedgerKey != DefaultKeyPrefix+":dispatch:ledger" {
		t.Fatalf("dispatch ledger default drifted: %q", cfg.DispatchLedgerKey)
	}
	if cfg.DispatcherID == "" {
		t.Fatal("dispatcher id default must be non-empty")
	}
	if cfg.EventTTLSeconds != 7*86400 || cfg.StateTTLDays != 30 {
		t.Fatalf("ttl defaults drifted: %d %d", cfg.EventTTLSeconds, cfg.StateTTLDays)
	}
	if cfg.HeartbeatInterval != 10*time.Second || cfg.RegistryTTL != 30*time.Second {
		t.Fatalf("heartbeat defaults drifted: %v %v", cfg.HeartbeatInterval, cfg.RegistryTTL)
	}
}

// TestConfigExplicitOverridesWin: explicit queue/group/DLQ envs are honored
// even under a shadow prefix.
func TestConfigExplicitOverridesWin(t *testing.T) {
	env := map[string]string{
		"PLAITA_KEY_PREFIX":     ShadowKeyPrefix,
		"PLAITA_QUEUE_NAME":     "shadow-custom:queue",
		"PLAITA_CONSUMER_GROUP": "custom-group",
		"PLAITA_DLQ_KEY":        "shadow-custom:dead",
	}
	cfg := configFromEnv(func(k string) string { return env[k] })
	if cfg.QueueName != "shadow-custom:queue" || cfg.ConsumerGroup != "custom-group" || cfg.DLQKey != "shadow-custom:dead" {
		t.Fatalf("explicit overrides lost: %+v", cfg)
	}
}

// TestApplyShadow: in-code shadow switch keeps explicit overrides.
func TestApplyShadow(t *testing.T) {
	cfg := configFromEnv(func(string) string { return "" })
	cfg.ApplyShadow()
	if cfg.KeyPrefix != ShadowKeyPrefix || cfg.QueueName != ShadowKeyPrefix+":flow:queue" {
		t.Fatalf("shadow switch incomplete: %+v", cfg)
	}
	if cfg.DispatchLedgerKey != ShadowKeyPrefix+":dispatch:ledger" {
		t.Fatalf("shadow ledger key = %q", cfg.DispatchLedgerKey)
	}
}

// TestConfigEnvInts: env integer overrides parse (§1.2 env surface).
func TestConfigEnvInts(t *testing.T) {
	env := map[string]string{
		"PLAITA_CLAIM_MIN_IDLE_MS":    "1500",
		"PLAITA_LEASE_TTL_SECONDS":    "60",
		"PLAITA_MAX_DELIVERIES":       "9",
		"PLAITA_DLQ_MAX_LEN":          "17",
		"PLAITA_CANCEL_POLL_MS":       "250",
		"PLAITA_WORKER_DRAIN_TIMEOUT": "5.5",
	}
	cfg := configFromEnv(func(k string) string { return env[k] })
	if cfg.ClaimMinIdleMS != 1500 || cfg.LeaseTTLSeconds != 60 || cfg.MaxDeliveries != 9 || cfg.DLQMaxLen != 17 {
		t.Fatalf("int overrides drifted: %+v", cfg)
	}
	if cfg.CancelPollMS != 250 || cfg.DrainTimeout != 5500*time.Millisecond {
		t.Fatalf("poll/drain drifted: %d %v", cfg.CancelPollMS, cfg.DrainTimeout)
	}
}
