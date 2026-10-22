package worker

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// TestFlowHashDifferentialFuzz replays a large randomized corpus against the
// Python authority (offline differential harness, NOT a CI dependency). The
// corpus is generated from the live plaita engine — for every line:
// canonical text must be byte-identical and the sha256 must match:
//
//	# one-off generation + run (Python side generates, Go verifies):
//	python3 /tmp/fuzzhash/gen_fuzz.py          # → /tmp/fuzzhash/cases.jsonl
//	PLAITA_FLOW_HASH_FUZZ=/tmp/fuzzhash/cases.jsonl \
//	  go test ./pkg/worker -run TestFlowHashDifferentialFuzz -v
//
// Skipped unless PLAITA_FLOW_HASH_FUZZ is set. The committed fixture
// (flow_hash_golden.json) is the CI-visible regression net; this harness is
// for deep changes to canonicaljson.go (e.g. after a Go toolchain upgrade
// touching strconv shortest-repr behavior).
func TestFlowHashDifferentialFuzz(t *testing.T) {
	path := os.Getenv("PLAITA_FLOW_HASH_FUZZ")
	if path == "" {
		t.Skip("PLAITA_FLOW_HASH_FUZZ not set (offline differential harness)")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open fuzz corpus: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<22), 1<<22)
	var n, bad int
	for sc.Scan() {
		var c struct {
			Raw       string `json:"raw"`
			Canonical string `json:"canonical"`
			Hash      string `json:"hash"`
		}
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			t.Fatalf("line %d: decode corpus entry: %v", n+1, err)
		}
		n++
		canonical, err := canonicalFlowDefinitionJSON([]byte(c.Raw))
		if err != nil {
			bad++
			t.Errorf("line %d: canonicalize: %v", n, err)
			continue
		}
		if !bytes.Equal(canonical, []byte(c.Canonical)) {
			bad++
			t.Errorf("line %d canonical mismatch:\nraw: %.120q\n go: %.120q\n py: %.120q", n, c.Raw, canonical, c.Canonical)
			continue
		}
		h, err := FlowDefinitionHash([]byte(c.Raw))
		if err != nil || h != c.Hash {
			bad++
			t.Errorf("line %d hash mismatch: err=%v go=%s py=%s", n, err, h, c.Hash)
		}
		if bad >= 5 {
			t.Fatalf("stopping after %d mismatches of %d cases", bad, n)
		}
	}
	if bad > 0 {
		t.Fatalf("%d mismatches of %d cases", bad, n)
	}
	t.Logf("all %d cases byte-identical to Python", n)
}
