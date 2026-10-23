package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
//
// #299 §2: with a console flow source configured (Config.ConsoleFlowSourceURL)
// a Redis miss reads through to the console admin API before declaring
// ErrFlowNotFound — cross-Redis deployments ride out the outbox delivery
// window instead of failing (console_flowsource.go). A deterministic console
// miss is negative-cached for Config.ConsoleFlowSourceNegativeTTL.
type FlowSource struct {
	client  *redis.Client
	cfg     *Config
	logger  Logger
	console *consoleFlowSource

	mu       sync.Mutex
	cache    map[string]FlowDefinition
	negative map[string]time.Time
}

// latestVersion is the empty-version sentinel: engine storage tracks it as a
// literal member of the versions set (storage/redis.py:764-782). On the
// console side the sentinel resolves to the newest published version (#299).
const latestVersion = "latest"

func NewFlowSource(client *redis.Client, cfg *Config, logger Logger) *FlowSource {
	fs := &FlowSource{client: client, cfg: cfg, logger: logger, cache: make(map[string]FlowDefinition)}
	if cfg.ConsoleFlowSourceURL != "" {
		fs.console = newConsoleFlowSource(cfg)
	}
	return fs
}

func (s *FlowSource) flowKey(tenantID, flowID, version string) string {
	return s.cfg.Namespace(tenantID) + ":flow:" + flowID + ":" + version
}

func (s *FlowSource) versionsKey(tenantID, flowID string) string {
	return s.cfg.Namespace(tenantID) + ":flow_versions:" + flowID
}

// GetFlowDefinition resolves flow_id(+version?) to a loaded workflow.
// ErrFlowNotFound (deterministic) when the definition does not exist in engine
// storage nor on the console (or no console flow source is configured);
// ErrFlowLoad (transient) on read, console-outage or IR-load failure.
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
		// #299 §2: before declaring not-found, read through to the console
		// (a no-op when no console flow source is configured).
		return s.pullThrough(ctx, tenantID, flowID, resolvedVersion, cacheKey)
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
		return latestVersion, nil
	}
	if contains(versions, latestVersion) {
		return latestVersion, nil
	}
	// Numeric resolution is Python's口径 (storage/redis.py:964-974): filter
	// to purely-numeric dotted versions FIRST, then take the tuple-order max
	// within that subset — a numeric member must never lose to a
	// lexicographically larger non-numeric one (#61: the old comparator fed
	// dotted versions to ParseFloat, so "1.10" collapsed to 1.1 < "1.9", and
	// three-segment versions fell back to string compare, "9.0.0" > "10.0.0").
	numeric := make([]string, 0, len(versions))
	for _, v := range versions {
		if _, ok := parseDottedVersion(v); ok {
			numeric = append(numeric, v)
		}
	}
	if len(numeric) == 0 {
		// No comparable numeric version: an arbitrary member (Python
		// next(iter(all_versions))) — deliberately NOT an error.
		return versions[0], nil
	}
	sort.Slice(numeric, func(i, j int) bool {
		return numericVersionLess(numeric[i], numeric[j])
	})
	return numeric[len(numeric)-1], nil
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// numericVersionLess orders purely-numeric dotted versions by tuple order:
// segments compare as ints and a shorter prefix sorts first ([1,10] <
// [1,10,0], matching Python's list comparison). Both operands must have been
// accepted by parseDottedVersion (resolveVersion filters before sorting).
func numericVersionLess(a, b string) bool {
	as, _ := parseDottedVersion(a)
	bs, _ := parseDottedVersion(b)
	for i := 0; i < len(as) && i < len(bs); i++ {
		if as[i] != bs[i] {
			return as[i] < bs[i]
		}
	}
	return len(as) < len(bs)
}

// parseDottedVersion accepts exactly ^[0-9]+(\.[0-9]+)*$ — the Go mirror of
// Python's v.replace(".", "").isdigit() filter. A "v" prefix disqualifies
// (v1.2 can only reach the arbitrary-member fallback, Python 严格口径), as do
// empty segments and segments beyond int range (fail-safe: such members just
// stop competing for the max).
func parseDottedVersion(v string) ([]int, bool) {
	if v == "" {
		return nil, false
	}
	parts := strings.Split(v, ".")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			return nil, false
		}
		for i := 0; i < len(p); i++ {
			if p[i] < '0' || p[i] > '9' {
				return nil, false
			}
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
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

// pullThrough is the #299 §2 read-through: the engine storage key is missing,
// so fetch the published definition from the console admin API. With no
// console flow source configured this is exactly the pre-#299 miss behavior
// (deterministic ErrFlowNotFound).
func (s *FlowSource) pullThrough(ctx context.Context, tenantID, flowID, version, cacheKey string) (*FlowDefinition, error) {
	if s.console == nil {
		return nil, fmt.Errorf("%w: flow %s version %s", ErrFlowNotFound, flowID, version)
	}
	if s.negativeGet(cacheKey) {
		return nil, fmt.Errorf("%w: flow %s version %s (console lookup negative-cached)", ErrFlowNotFound, flowID, version)
	}

	tenant := s.consoleTenant(tenantID)
	concrete := version
	if concrete == latestVersion {
		lv, err := s.console.latestPublished(ctx, tenant, flowID)
		if err != nil {
			return nil, s.pullThroughErr(cacheKey, flowID, version, err)
		}
		concrete = lv
	}

	detail, err := s.console.versionDetail(ctx, tenant, flowID, concrete)
	if err != nil {
		return nil, s.pullThroughErr(cacheKey, flowID, concrete, err)
	}
	if detail.Status != "published" {
		s.negativePut(cacheKey)
		return nil, fmt.Errorf("%w: flow %s version %s not published on console (status %q)",
			ErrFlowNotFound, flowID, concrete, detail.Status)
	}

	raw, err := engineFlowPayload(flowID, concrete, detail.Definition)
	if err != nil {
		// Deterministic (#304 口径): a published-but-corrupt definition can
		// never succeed on redelivery — terminalize instead of burning the
		// delivery budget.
		s.negativePut(cacheKey)
		return nil, fmt.Errorf("%w: console flow %s:%s definition unparsable: %v", ErrFlowNotFound, flowID, concrete, err)
	}
	hash, err := FlowDefinitionHash(raw)
	if err != nil {
		s.negativePut(cacheKey)
		return nil, fmt.Errorf("%w: console flow %s:%s fingerprint: %v", ErrFlowNotFound, flowID, concrete, err)
	}
	wf, err := ir.Load(raw)
	if err != nil {
		// The IR loader is fail-closed: an unloadable flow is a permanent,
		// deterministic failure for this engine build (same as the Redis path).
		s.negativePut(cacheKey)
		return nil, fmt.Errorf("%w: flow %s:%s not loadable: %v", ErrFlowNotFound, flowID, concrete, err)
	}

	def := &FlowDefinition{
		FlowID:   flowID,
		Version:  concrete,
		Raw:      raw,
		Hash:     hash,
		HashAlgo: FlowHashAlgoV2,
		Workflow: wf,
	}
	if s.cfg.ConsoleFlowSourceWriteBack {
		// Optional self-heal (#299 §2 可选回写): write the pulled definition
		// into the engine storage shape so peers in the same Redis stop
		// missing too. Best-effort — the run is already resolved; a failed
		// write-back must not fail it. Off by default: an explicit opt-in
		// keeps the pull-through read-only with respect to #303's
		// 「删除后目标侧不复活」 purge semantics.
		if err := s.PresetFlowDefinition(ctx, tenantID, flowID, concrete, raw); err != nil {
			s.logger.Warn("console flow source: write-back failed",
				"flow_id", flowID, "version", concrete, "error", err)
		}
	}
	s.cachePut(cacheKey, *def)
	return def, nil
}

// pullThroughErr maps a console client error onto the worker error taxonomy:
// not-found → deterministic ErrFlowNotFound (+ negative cache); anything
// transient → ErrFlowLoad with NO negative cache (the outbox may deliver the
// definition any moment).
func (s *FlowSource) pullThroughErr(cacheKey, flowID, version string, err error) error {
	if errors.Is(err, errConsoleNotFound) {
		s.negativePut(cacheKey)
		return fmt.Errorf("%w: flow %s version %s (console: does not exist)", ErrFlowNotFound, flowID, version)
	}
	return fmt.Errorf("%w: console flow source: %v", ErrFlowLoad, err)
}

// consoleTenant maps the task tenant onto the X-Tenant-ID header value: the
// unpartitioned/default tenant is spelled explicitly for the console (its
// X-Tenant-ID must reference a real tenant row), mirroring cfg.Namespace's
// default routing.
func (s *FlowSource) consoleTenant(tenantID string) string {
	if tenantID == "" || tenantID == s.cfg.DefaultTenantID {
		return s.cfg.DefaultTenantID
	}
	return tenantID
}

func (s *FlowSource) negativeGet(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts, ok := s.negative[key]
	if !ok {
		return false
	}
	if time.Since(ts) > s.cfg.ConsoleFlowSourceNegativeTTL {
		delete(s.negative, key)
		return false
	}
	return true
}

func (s *FlowSource) negativePut(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.negative == nil {
		s.negative = make(map[string]time.Time)
	}
	s.negative[key] = time.Now()
}
