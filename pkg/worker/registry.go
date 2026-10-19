package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Worker-side heartbeats (protocol §8.1). Three layers:
//
//  1. registry heartbeat — SETEX {prefix}:registry:flow_worker:{instance_id}
//     every 10s (TTL 30s) with the ServiceInfo JSON;
//  2. capacity block — metadata.worker_capacity refreshed when the active
//     task count changes (disk probing is not implemented: free_disk_gib
//     stays null, which the capacity schema allows);
//  3. execution progress heartbeat — long-node node_timings merge, lives in
//     worker.go (it writes through the fenced state path).

// ServiceInfo is the registry entry JSON (§8.1, registry.py:122-126,187-251).
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

// Registry status lifecycle (§8.1, §9.3).
const (
	RegStatusStarting = "starting"
	RegStatusRunning  = "running"
	RegStatusDraining = "draining"
	RegStatusStopping = "stopping"
	RegStatusStopped  = "stopped"
)

// Registry writes the worker registry key.
type Registry struct {
	client *redis.Client
	cfg    *Config
	logger Logger

	mu        sync.Mutex
	info      ServiceInfo
	capacity  map[string]any
	startTime time.Time
}

func NewRegistry(client *redis.Client, cfg *Config, logger Logger) *Registry {
	return &Registry{client: client, cfg: cfg, logger: logger, startTime: time.Now()}
}

func (r *Registry) key() string {
	return r.cfg.KeyPrefix + ":registry:flow_worker:" + r.cfg.InstanceID
}

// Start builds the initial ServiceInfo. host/port/db come from the Redis URL
// (password masked, mirroring the Python metadata rules §1.2).
func (r *Registry) Start(redisURL string) {
	meta := map[string]any{
		"queue_name":  r.cfg.QueueName,
		"cache_size":  r.cfg.FlowCacheSize,
		"cache_ttl":   int(r.cfg.FlowCacheTTL.Seconds()),
		"concurrency": 1, // MVP: single consumer loop
	}
	masked := redisURL
	host, port, db := "unknown", "0", 0
	if u, err := url.Parse(redisURL); err == nil {
		if u.User != nil {
			masked = maskURL(u)
		}
		host = u.Hostname()
		if p := u.Port(); p != "" {
			port = p
		}
		if u.Path != "" {
			if n, err := strconv.Atoi(u.Path[1:]); err == nil {
				db = n
			}
		}
	}
	meta["redis_url"] = masked
	meta["host"] = host
	meta["port"] = port
	meta["db"] = db
	r.capacity = map[string]any{
		"schema_version":    1,
		"free_disk_gib":     nil, // disk guard not implemented (phase 2)
		"min_free_disk_gib": 0,
		"disk_blocked":      false,
		"active_tasks":      0,
		"concurrency":       1,
		"sampled_at":        nowISO(),
	}
	meta["worker_capacity"] = r.capacity

	r.mu.Lock()
	r.info = ServiceInfo{
		InstanceID:  r.cfg.InstanceID,
		ServiceType: "flow_worker",
		Host:        host,
		Status:      RegStatusStarting,
		StartTime:   nowISO(),
		Metadata:    meta,
	}
	r.mu.Unlock()
}

// maskURL rebuilds the URL with the password replaced (flow_worker.py:416-449
// masking rules, Go port: keep the username, star out the password).
func maskURL(u *url.URL) string {
	masked := *u
	if u.User != nil {
		pass, hasPass := u.User.Password()
		if hasPass {
			masked.User = url.UserPassword(u.User.Username(), "******")
		} else {
			masked.User = url.User(u.User.Username())
		}
		_ = pass
	}
	return masked.String()
}

// SetStatus flips the lifecycle status and flushes immediately (§9.3: status
// transitions are persisted right away, not on the next heartbeat tick).
func (r *Registry) SetStatus(ctx context.Context, status string) {
	_ = r.SetStatusFlush(ctx, status, 1)
}

// SetStatusFlush sets the status and flushes with up to `attempts` short
// retries — the draining transition must be visible immediately, otherwise
// the dispatcher keeps routing to this instance during the window
// (registry.flush_registry_status, 3 retries).
func (r *Registry) SetStatusFlush(ctx context.Context, status string, attempts int) error {
	r.mu.Lock()
	r.info.Status = status
	r.mu.Unlock()
	if attempts < 1 {
		attempts = 1
	}
	var err error
	for i := 0; i < attempts; i++ {
		if err = r.Flush(ctx); err == nil {
			return nil
		}
		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	r.logger.Warn("registry status flush failed", "status", status, "error", err)
	return err
}

// SetActiveTasks refreshes the capacity block when the in-flight count
// changes (§8.1 layer 2: refresh on change) and flushes.
func (r *Registry) SetActiveTasks(ctx context.Context, n int) {
	r.mu.Lock()
	if r.capacity == nil {
		// ProcessOne without a Start (tests / embedded use): keep the
		// capacity block shape valid.
		r.capacity = map[string]any{
			"schema_version":    1,
			"free_disk_gib":     nil,
			"min_free_disk_gib": 0,
			"disk_blocked":      false,
			"concurrency":       1,
		}
	}
	r.capacity["active_tasks"] = n
	r.capacity["sampled_at"] = nowISO()
	r.info.ActiveTasks = n
	r.mu.Unlock()
	_ = r.Flush(ctx)
}

// Flush writes the current ServiceInfo with the registry TTL (§8.1: 30s).
func (r *Registry) Flush(ctx context.Context) error {
	r.mu.Lock()
	info := r.info
	info.LastHeartbeat = nowISO()
	r.info = info
	r.mu.Unlock()
	blob, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return r.client.SetEx(ctx, r.key(), string(blob), r.cfg.RegistryTTL).Err()
}

// HeartbeatLoop flushes every HeartbeatInterval until ctx is done (§8.1
// layer 1: 10s period).
func (r *Registry) HeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Flush(ctx); err != nil {
				r.logger.Warn("registry heartbeat failed", "error", err)
			}
		}
	}
}

// LookupStatus reads another instance's registry status (§3.5 lease-conflict
// liveness accounting). Returns "" when the key is absent.
func (r *Registry) LookupStatus(ctx context.Context, instanceID string) (string, error) {
	v, err := r.client.Get(ctx, r.cfg.KeyPrefix+":registry:flow_worker:"+instanceID).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var info ServiceInfo
	if err := json.Unmarshal([]byte(v), &info); err != nil {
		return "", fmt.Errorf("registry entry unparsable: %w", err)
	}
	return info.Status, nil
}

// Unregister writes the stopped tombstone then deletes the key (§9.3:
// stopped tombstone SETEX 5s → DEL).
func (r *Registry) Unregister(ctx context.Context) {
	blob, err := json.Marshal(map[string]any{"instance_id": r.cfg.InstanceID, "status": RegStatusStopped})
	if err == nil {
		_ = r.client.SetEx(ctx, r.key(), string(blob), 5*time.Second).Err()
	}
	_ = r.client.Del(ctx, r.key()).Err()
}
