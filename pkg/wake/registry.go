package wake

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// ServiceInfo mirrors the worker's registry record (pkg/worker
// registry.go ServiceInfo) field-for-field — one JSON shape across
// flow_worker and wake sources, so consumers (console roll-call,
// runtime_pool scans) read them uniformly.
type ServiceInfo struct {
	InstanceID    string         `json:"instance_id"`
	ServiceType   string         `json:"service_type"`
	Host          string         `json:"host"`
	Status        string         `json:"status"`
	StartTime     string         `json:"start_time"`
	Metadata      map[string]any `json:"metadata"`
	ActiveTasks   int            `json:"active_tasks"`
	LastHeartbeat string         `json:"last_heartbeat"`
}

// Registry statuses (§8.1/§9.3 vocabulary shared with the worker).
const (
	RegStatusStarting = "starting"
	RegStatusRunning  = "running"
	RegStatusStopped  = "stopped"
)

// registryTombstoneTTL mirrors the worker's stopped tombstone (5s, then DEL).
const registryTombstoneTTL = 5 * time.Second

// ServiceRegistry keeps one source's roll-call record alive:
// SETEX {prefix}:registry:{source}:{instance_id} (TTL = cfg.RegistryTTL),
// rewritten whole on every beat (no separate renewal face — same shape as the
// worker's flow_worker registration).
type ServiceRegistry struct {
	client    *redis.Client
	cfg       *Config
	source    string
	startTime string
	logger    *slog.Logger
}

func NewServiceRegistry(client *redis.Client, cfg *Config, source string) *ServiceRegistry {
	return &ServiceRegistry{
		client:    client,
		cfg:       cfg,
		source:    source,
		startTime: time.Now().UTC().Format(time.RFC3339),
		logger:    cfg.Logger,
	}
}

func (r *ServiceRegistry) key() string {
	return r.cfg.KeyPrefix + ":registry:" + r.source + ":" + r.cfg.InstanceID
}

// Beat writes one registration record with the given status.
func (r *ServiceRegistry) Beat(ctx context.Context, status string, activeTasks int) error {
	info := ServiceInfo{
		InstanceID:    r.cfg.InstanceID,
		ServiceType:   r.source,
		Host:          hostname(),
		Status:        status,
		StartTime:     r.startTime,
		Metadata:      map[string]any{"engine": "plaita-go", "key_prefix": r.cfg.KeyPrefix},
		ActiveTasks:   activeTasks,
		LastHeartbeat: time.Now().UTC().Format(time.RFC3339),
	}
	blob, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("wake registry marshal: %w", err)
	}
	return r.client.SetEx(ctx, r.key(), string(blob), r.cfg.RegistryTTL).Err()
}

// Stop writes the stopped tombstone (5s) and deletes the key — the same
// shutdown shape as the worker registry, so a dead instance never lingers as
// a live roll-call entry beyond the TTL.
func (r *ServiceRegistry) Stop(ctx context.Context) {
	ttlCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.client.SetEx(ttlCtx, r.key(), r.tombstone(), registryTombstoneTTL).Err(); err != nil {
		r.logger.Warn("wake registry tombstone failed; TTL will reclaim the key",
			"source", r.source, "error", err)
		return
	}
	if err := r.client.Del(ttlCtx, r.key()).Err(); err != nil {
		r.logger.Warn("wake registry delete failed; tombstone reclaims in 5s",
			"source", r.source, "error", err)
	}
}

func (r *ServiceRegistry) tombstone() string {
	blob, _ := json.Marshal(ServiceInfo{
		InstanceID:    r.cfg.InstanceID,
		ServiceType:   r.source,
		Host:          hostname(),
		Status:        RegStatusStopped,
		StartTime:     r.startTime,
		LastHeartbeat: time.Now().UTC().Format(time.RFC3339),
	})
	return string(blob)
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
