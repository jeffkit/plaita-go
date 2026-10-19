package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// Redis checkpoint persistence (M6, checkpoint v1.5).
//
// Key layout (all under the worker key namespace, so a shadow run stays in
// PLAITA_KEY_PREFIX and never touches production keys):
//
//	{ns}:execution:checkpoint:{execution_id} = Checkpoint JSON
//
// The key is written by the engine at the suspend boundary (CheckpointStore
// wired through workflow.SuspendOptions), read on resume, and deleted when
// the execution reaches a terminal state. No TTL while suspended: a human
// approval can wait arbitrarily long; the terminal delete is what bounds the
// key's life. {ns} = cfg.Namespace(tenant_id), like every other execution
// key (§3.1).
type RedisCheckpointStore struct {
	client *redis.Client
	cfg    *Config
}

// NewRedisCheckpointStore returns the checkpoint store for one worker.
func NewRedisCheckpointStore(client *redis.Client, cfg *Config) *RedisCheckpointStore {
	return &RedisCheckpointStore{client: client, cfg: cfg}
}

func (s *RedisCheckpointStore) checkpointKey(tenantID, executionID string) string {
	return s.cfg.Namespace(tenantID) + ":execution:checkpoint:" + executionID
}

// SaveCheckpoint writes the checkpoint (overwrites a previous boundary).
func (s *RedisCheckpointStore) SaveCheckpoint(ctx context.Context, cp *workflow.Checkpoint) error {
	if cp == nil || cp.ExecutionID == "" {
		return fmt.Errorf("%w: checkpoint has no execution id", ErrStatePersist)
	}
	blob, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("%w: checkpoint marshal: %v", ErrStatePersist, err)
	}
	if err := s.client.Set(ctx, s.checkpointKey(cp.TenantID, cp.ExecutionID), string(blob), 0).Err(); err != nil {
		return fmt.Errorf("%w: checkpoint set: %v", ErrStatePersist, err)
	}
	return nil
}

// LoadCheckpoint reads the checkpoint. Missing → (nil, nil); a corrupt or
// newer-schema checkpoint is an error (never silently "missing" — a resume
// would otherwise start from the wrong place).
func (s *RedisCheckpointStore) LoadCheckpoint(ctx context.Context, tenantID, executionID string) (*workflow.Checkpoint, error) {
	raw, err := s.client.Get(ctx, s.checkpointKey(tenantID, executionID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: checkpoint get: %v", ErrStateLoad, err)
	}
	var cp workflow.Checkpoint
	if err := json.Unmarshal([]byte(raw), &cp); err != nil {
		return nil, fmt.Errorf("%w: corrupt checkpoint json: %v", ErrStateLoad, err)
	}
	if cp.SchemaVersion <= 0 || cp.SchemaVersion > workflow.CheckpointSchemaVersion {
		return nil, fmt.Errorf("%w: checkpoint schema version %d not supported (engine supports 1..%d)",
			ErrStateLoad, cp.SchemaVersion, workflow.CheckpointSchemaVersion)
	}
	return &cp, nil
}

// DeleteCheckpoint removes the checkpoint (idempotent; terminal cleanup).
func (s *RedisCheckpointStore) DeleteCheckpoint(ctx context.Context, tenantID, executionID string) error {
	if executionID == "" {
		return nil
	}
	return s.client.Del(ctx, s.checkpointKey(tenantID, executionID)).Err()
}
