package worker

import (
	"context"
	"errors"
	"testing"
)

// TestFlowDefinitionHash: canonical sorted-keys compact JSON sha256 with the
// v2 algorithm marker (§7).
func TestFlowDefinitionHash(t *testing.T) {
	a := []byte(`{"b":1,"a":"x"}`)
	b := []byte(`{ "a": "x",  "b": 1 }`) // same document, different whitespace/key order
	ha, err := FlowDefinitionHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := FlowDefinitionHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Fatalf("hash mismatch for equivalent documents: %s vs %s", ha, hb)
	}
	c := []byte(`{"b":2,"a":"x"}`)
	hc, _ := FlowDefinitionHash(c)
	if hc == ha {
		t.Fatal("different documents must hash differently")
	}
	if len(ha) != 64 {
		t.Fatalf("hash length = %d, want 64 (sha256 hex)", len(ha))
	}
}

// TestFlowSourceResolve: the storage shape + version resolution rules (§7):
// prefer "latest", else the numerically-largest version.
func TestFlowSourceResolve(t *testing.T) {
	w, rdb, _, _ := newTestWorker(t)
	ctx := context.Background()

	if err := w.flows.PresetFlowDefinition(ctx, "", "hello_world_go", "latest", mustHelloWorld(t)); err != nil {
		t.Fatal(err)
	}
	def, err := w.flows.GetFlowDefinition(ctx, "", "hello_world_go", "")
	if err != nil {
		t.Fatalf("resolve latest: %v", err)
	}
	if def.Version != "latest" {
		t.Fatalf("version = %q, want latest", def.Version)
	}
	if def.HashAlgo != FlowHashAlgoV2 || def.Hash == "" {
		t.Fatalf("fingerprint = %q/%q", def.Hash, def.HashAlgo)
	}
	if def.Workflow == nil || def.Workflow.ID != "hello_world_go" {
		t.Fatalf("workflow = %+v", def.Workflow)
	}

	// Numeric versions: no "latest" member → largest wins.
	if err := w.flows.PresetFlowDefinition(ctx, "", "num_flow", "2", mustHelloWorld(t)); err != nil {
		t.Fatal(err)
	}
	if err := w.flows.PresetFlowDefinition(ctx, "", "num_flow", "11", mustHelloWorld(t)); err != nil {
		t.Fatal(err)
	}
	def, err = w.flows.GetFlowDefinition(ctx, "", "num_flow", "")
	if err != nil {
		t.Fatalf("resolve numeric: %v", err)
	}
	if def.Version != "11" {
		t.Fatalf("numeric resolve = %q, want 11 (numeric, not lexicographic)", def.Version)
	}

	// Explicit version hits that key.
	def, err = w.flows.GetFlowDefinition(ctx, "", "num_flow", "2")
	if err != nil || def.Version != "2" {
		t.Fatalf("explicit version = (%+v,%v)", def, err)
	}

	// Missing flow → deterministic not-found.
	if _, err := w.flows.GetFlowDefinition(ctx, "", "nope", ""); !errors.Is(err, ErrFlowNotFound) {
		t.Fatalf("missing flow = %v, want ErrFlowNotFound", err)
	}

	// Tenant-routed keys: a definition under tenant "acme" is invisible from
	// the default namespace (§3.1/§7 tenant ns).
	if err := w.flows.PresetFlowDefinition(ctx, "acme", "tenant_flow", "latest", mustHelloWorld(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.flows.GetFlowDefinition(ctx, "", "tenant_flow", ""); !errors.Is(err, ErrFlowNotFound) {
		t.Fatal("tenant flow leaked into default namespace")
	}
	if _, err := w.flows.GetFlowDefinition(ctx, "acme", "tenant_flow", ""); err != nil {
		t.Fatalf("tenant-scoped read: %v", err)
	}
	_ = rdb
}

// TestFlowUnloadableIsDeterministic: an IR that fails the fail-closed loader
// maps to ErrFlowNotFound (deterministic, poison-ack family), not a transient
// load error.
func TestFlowUnloadableIsDeterministic(t *testing.T) {
	w, _, _, _ := newTestWorker(t)
	ctx := context.Background()
	if err := w.flows.PresetFlowDefinition(ctx, "", "bad_flow", "latest",
		[]byte(`{"flow_id":"bad_flow","nodes":[{"type":"event","id":"e1"}]}`)); err != nil {
		t.Fatal(err)
	}
	_, err := w.flows.GetFlowDefinition(ctx, "", "bad_flow", "")
	if !errors.Is(err, ErrFlowNotFound) {
		t.Fatalf("unloadable flow = %v, want ErrFlowNotFound", err)
	}
}
