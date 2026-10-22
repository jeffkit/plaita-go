package worker

// #82 negative control: the worker registry entry must carry the #300
// capability manifest — console's capability gate (plaita server/capability.py
// manifest_from_metadata) reads metadata.capability_manifest as a nested JSON
// object (five MANIFEST_KEYS + engine ∈ {python, plaita-go}) and fails open
// for any pool whose entries lack it. Pre-#82 the Go worker registered with
// no manifest at all, so the gate was permanently fail-open for the go pool.
//
// The assertions mirror the console reader generically (plain map walk over
// the flushed ServiceInfo JSON — the same shape json.loads produces there),
// and pin supported/unsupported to the ir accept-list live values (no second
// handwritten list).

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/ir"
)

// manifestProbeRegistry builds a registry against miniredis, drives it to
// running, flushes, and returns the entry read back from Redis (what a
// console scan would see) plus the live registry handle for heartbeat probes.
func manifestProbeRegistry(t *testing.T, instanceID string) (map[string]any, *Registry, *redis.Client, *Config) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	cfg := DefaultConfig()
	cfg.InstanceID = instanceID
	cfg.QueueName = "plaita:flow:queue:go"
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	reg := NewRegistry(rdb, cfg, logger)
	reg.Start("redis://localhost:6379/0")
	ctx := context.Background()
	reg.SetStatus(ctx, RegStatusRunning)
	if err := reg.Flush(ctx); err != nil {
		t.Fatalf("flush registry: %v", err)
	}

	raw, err := rdb.Get(ctx, cfg.KeyPrefix+":registry:flow_worker:"+instanceID).Result()
	if err != nil {
		t.Fatalf("registry entry missing after flush: %v", err)
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		t.Fatalf("registry entry unparsable: %v", err)
	}
	return entry, reg, rdb, cfg
}

func stringSlice(t *testing.T, v any) []string {
	t.Helper()
	list, ok := v.([]any)
	if !ok {
		t.Fatalf("expected JSON array, got %T", v)
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		s, ok := item.(string)
		if !ok {
			t.Fatalf("expected string element, got %T", item)
		}
		out = append(out, s)
	}
	return out
}

func readRegistryEntry(t *testing.T, rdb *redis.Client, cfg *Config, instanceID string) map[string]any {
	t.Helper()
	raw, err := rdb.Get(context.Background(), cfg.KeyPrefix+":registry:flow_worker:"+instanceID).Result()
	if err != nil {
		t.Fatalf("registry entry missing: %v", err)
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		t.Fatalf("registry entry unparsable: %v", err)
	}
	return entry
}

// assertManifestContract checks the console-visible manifest shape on one
// registry entry (shared by the first-flush and heartbeat-survival probes).
func assertManifestContract(t *testing.T, entry map[string]any) {
	t.Helper()
	meta, ok := entry["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("entry metadata not an object: %T", entry["metadata"])
	}

	if got := meta["engine"]; got != ir.EngineName {
		t.Fatalf("metadata.engine = %v, want %q", got, ir.EngineName)
	}

	// Must be a nested JSON object, not a marshaled string: the console
	// parser (manifest_from_metadata) rejects anything but a dict.
	manifest, ok := meta["capability_manifest"].(map[string]any)
	if !ok {
		t.Fatalf("metadata.capability_manifest is %T, want nested JSON object (console manifest_from_metadata rejects non-dict)", meta["capability_manifest"])
	}
	for _, key := range []string{
		"engine", "ir_contract_version", "supported_node_types",
		"unsupported_node_types", "features",
	} {
		if _, ok := manifest[key]; !ok {
			t.Fatalf("capability_manifest missing console MANIFEST_KEY %q", key)
		}
	}
	if got := manifest["engine"]; got != ir.EngineName {
		t.Fatalf("capability_manifest.engine = %v, want %q", got, ir.EngineName)
	}
	if got := manifest["ir_contract_version"]; got != ir.ContractVersion {
		t.Fatalf("capability_manifest.ir_contract_version = %v, want %q", got, ir.ContractVersion)
	}
	if got, want := stringSlice(t, manifest["supported_node_types"]), ir.SupportedNodeTypes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("capability_manifest.supported_node_types diverges from live accept-list:\n got  %v\n want %v", got, want)
	}
	if got, want := stringSlice(t, manifest["unsupported_node_types"]), ir.UnsupportedNodeTypes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("capability_manifest.unsupported_node_types diverges from live known-unsupported list:\n got  %v\n want %v", got, want)
	}
	if features := stringSlice(t, manifest["features"]); len(features) == 0 {
		t.Fatal("capability_manifest.features empty")
	}

	// #80 phase 3 additive key: deployment dependencies for the five
	// suspended server nodes ride along (consumers of the five keys are
	// unaffected, but the annotation must survive the registry hop).
	deps, ok := manifest["node_dependencies"].(map[string]any)
	if !ok {
		t.Fatalf("capability_manifest.node_dependencies is %T, want object", manifest["node_dependencies"])
	}
	for _, nodeType := range []string{"delay", "approval", "redis_queue", "kafka_queue", "http_callback"} {
		if _, ok := deps[nodeType]; !ok {
			t.Fatalf("node_dependencies missing %q", nodeType)
		}
	}
}

func TestRegistryMetadataCarriesCapabilityManifest(t *testing.T) {
	entry, _, _, _ := manifestProbeRegistry(t, "go-cap-probe-1")
	assertManifestContract(t, entry)
}

func TestRegistryManifestSurvivesHeartbeat(t *testing.T) {
	_, reg, rdb, cfg := manifestProbeRegistry(t, "go-cap-probe-1")
	if err := reg.Flush(context.Background()); err != nil {
		t.Fatalf("heartbeat flush: %v", err)
	}
	assertManifestContract(t, readRegistryEntry(t, rdb, cfg, "go-cap-probe-1"))
}

// TestExportRegistryFixture writes one real registration entry (the exact
// bytes a plaita-go worker SETEXs into {ns}:registry:flow_worker:{id}) to
// $PLAITA_EXPORT_REGISTRY_FIXTURE. The plaita repo's console-side #300
// negative control (test_issue300_runtime_capability_gate.py) consumes the
// committed copy instead of a hand-synthesized manifest. Skipped unless the
// env var names a target path.
func TestExportRegistryFixture(t *testing.T) {
	path := os.Getenv("PLAITA_EXPORT_REGISTRY_FIXTURE")
	if path == "" {
		t.Skip("PLAITA_EXPORT_REGISTRY_FIXTURE not set")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	entry, _, _, _ := manifestProbeRegistry(t, "flow-worker-fixture-1")
	blob, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if err := os.WriteFile(path, append(blob, '\n'), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	t.Logf("registry fixture written to %s", path)
}
