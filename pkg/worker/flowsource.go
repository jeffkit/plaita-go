package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/ir"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// Flow IR source (protocol §7): the IR is NOT in the message payload — it
// lives in the RedisFlowStorage shape written by console engine_sync:
//
//	{ns}:flow:{flow_id}:{version}   = definition JSON (SET, no TTL)
//	{ns}:flow_list                  = SET of flow_id
//	{ns}:flow_versions:{flow_id}    = SET of version
//
// With no explicit version the reader prefers the literal "latest" member,
// then falls back to the numerically-largest version (storage/redis.py:764-782).

// FlowDefinition is a resolved flow: raw deployment JSON + fingerprint + the
// loaded engine workflow.
type FlowDefinition struct {
	FlowID   string
	Version  string
	Raw      []byte
	Hash     string
	HashAlgo string
	Workflow *workflow.Workflow
	cachedAt time.Time
}

// FlowSource reads flow definitions from RedisFlowStorage with the protocol
// TTL cache (§1.2: 100 entries / 300s) and the v2 fingerprint algorithm
// (§7: sha256 over the ORIGINAL definition JSON, sorted keys, compact
// separators, before model_validate).
type FlowSource struct {
	client *redis.Client
	cfg    *Config
	logger Logger

	mu    sync.Mutex
	cache map[string]FlowDefinition
}

func NewFlowSource(client *redis.Client, cfg *Config, logger Logger) *FlowSource {
	return &FlowSource{client: client, cfg: cfg, logger: logger, cache: make(map[string]FlowDefinition)}
}

func (s *FlowSource) flowKey(tenantID, flowID, version string) string {
	return s.cfg.Namespace(tenantID) + ":flow:" + flowID + ":" + version
}

func (s *FlowSource) versionsKey(tenantID, flowID string) string {
	return s.cfg.Namespace(tenantID) + ":flow_versions:" + flowID
}

// GetFlowDefinition resolves flow_id(+version?) to a loaded workflow.
// ErrFlowNotFound (deterministic) when the definition key does not exist;
// ErrFlowLoad (transient) on read or IR-load failure.
func (s *FlowSource) GetFlowDefinition(ctx context.Context, tenantID, flowID, version string) (*FlowDefinition, error) {
	cacheKey := s.cfg.Namespace(tenantID) + ":" + flowID + ":" + version
	if def, ok := s.cacheGet(cacheKey); ok {
		return &def, nil
	}

	resolvedVersion, err := s.resolveVersion(ctx, tenantID, flowID, version)
	if err != nil {
		return nil, err
	}
	raw, err := s.client.Get(ctx, s.flowKey(tenantID, flowID, resolvedVersion)).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("%w: flow %s version %s", ErrFlowNotFound, flowID, resolvedVersion)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFlowLoad, err)
	}

	// Fingerprint over the raw definition, BEFORE engine loading (§7 step 4).
	hash, err := FlowDefinitionHash(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: fingerprint: %v", ErrFlowLoad, err)
	}

	wf, err := ir.Load(raw)
	if err != nil {
		// The IR loader is fail-closed: an unloadable flow is a permanent,
		// deterministic failure for this engine build (AGENTS.md 拒载清单).
		return nil, fmt.Errorf("%w: flow %s:%s not loadable: %v", ErrFlowNotFound, flowID, resolvedVersion, err)
	}
	def := &FlowDefinition{
		FlowID:   flowID,
		Version:  resolvedVersion,
		Raw:      raw,
		Hash:     hash,
		HashAlgo: FlowHashAlgoV2,
		Workflow: wf,
	}
	s.cachePut(cacheKey, *def)
	return def, nil
}

// resolveVersion applies the empty-version rule: prefer "latest", else the
// numerically-largest member of the versions set (§7).
func (s *FlowSource) resolveVersion(ctx context.Context, tenantID, flowID, version string) (string, error) {
	if version != "" {
		return version, nil
	}
	versions, err := s.client.SMembers(ctx, s.versionsKey(tenantID, flowID)).Result()
	if err != nil {
		return "", fmt.Errorf("%w: versions set: %v", ErrFlowLoad, err)
	}
	if len(versions) == 0 {
		return "latest", nil
	}
	if contains(versions, "latest") {
		return "latest", nil
	}
	sort.Slice(versions, func(i, j int) bool {
		return numericVersionLess(versions[i], versions[j])
	})
	return versions[len(versions)-1], nil
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func numericVersionLess(a, b string) bool {
	ai, aerr := strconv.ParseFloat(strings.TrimPrefix(a, "v"), 64)
	bi, berr := strconv.ParseFloat(strings.TrimPrefix(b, "v"), 64)
	if aerr != nil || berr != nil {
		return a < b
	}
	return ai < bi
}

// FlowDefinitionHash is the byte-exact Go counterpart of Python
// compute_flow_definition_hash (§7: json.dumps(sort_keys=True,
// ensure_ascii=False, separators=(",",":")) over the json.loads-parsed
// stored definition → sha256). Canonicalization goes through
// canonicalFlowDefinitionJSON (canonicaljson.go) — NOT json.Marshal, whose
// HTML escaping, U+2028 escaping and json.Number passthrough each flip the
// fingerprint against Python's (#60). Byte equality with the Python engine
// is pinned by flow_hash_golden_test.go against testdata/flow_hash_golden.json.
// Re-export the fixture with:
//
//	cd ~/projects/infra4agent/plaita && python3 \
//	  ~/projects/infra4agent/plaita-go/scripts/export_flow_hash_golden.py
func FlowDefinitionHash(raw []byte) (string, error) {
	canonical, err := canonicalFlowDefinitionJSON(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func (s *FlowSource) cacheGet(key string) (FlowDefinition, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	def, ok := s.cache[key]
	if !ok || time.Since(def.cachedAt) > s.cfg.FlowCacheTTL {
		delete(s.cache, key)
		return FlowDefinition{}, false
	}
	return def, true
}

func (s *FlowSource) cachePut(key string, def FlowDefinition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cache) >= s.cfg.FlowCacheSize {
		// Naive eviction (the Python side uses TTLCache): drop everything
		// expired first, then an arbitrary entry.
		now := time.Now()
		for k, v := range s.cache {
			if now.Sub(v.cachedAt) > s.cfg.FlowCacheTTL {
				delete(s.cache, k)
			}
		}
		for k := range s.cache {
			delete(s.cache, k)
			break
		}
	}
	def.cachedAt = time.Now()
	s.cache[key] = def
}

// PresetFlowDefinition writes a definition into the storage shape — used by
// tests and shadow fixtures (console engine_sync.sync_flow_to_engine is the
// production writer).
func (s *FlowSource) PresetFlowDefinition(ctx context.Context, tenantID, flowID, version string, raw []byte) error {
	pipe := s.client.Pipeline()
	pipe.Set(ctx, s.flowKey(tenantID, flowID, version), raw, 0)
	pipe.SAdd(ctx, s.cfg.Namespace(tenantID)+":flow_list", flowID)
	pipe.SAdd(ctx, s.versionsKey(tenantID, flowID), version)
	_, err := pipe.Exec(ctx)
	return err
}
