package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// flowHashGolden loads testdata/flow_hash_golden.json — exported from the
// live Python engine by scripts/export_flow_hash_golden.py (see the script
// header for the command). Every case pins the exact raw stored-definition
// text, Python's canonical text and Python's sha256.
func flowHashGolden(t *testing.T) []struct {
	Name            string `json:"name"`
	Raw             string `json:"raw"`
	PythonCanonical string `json:"python_canonical"`
	PythonHash      string `json:"python_hash"`
	GoRejects       bool   `json:"go_rejects"`
} {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "flow_hash_golden.json"))
	if err != nil {
		t.Fatalf("read golden fixture: %v", err)
	}
	var fixture struct {
		Cases []struct {
			Name            string `json:"name"`
			Raw             string `json:"raw"`
			PythonCanonical string `json:"python_canonical"`
			PythonHash      string `json:"python_hash"`
			GoRejects       bool   `json:"go_rejects"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("decode golden fixture: %v", err)
	}
	// Matrix-size guard: the fixture must always carry the full A1 r120
	// ten-family matrix plus the extended families (≥20 non-a1 cases).
	a1, extended := 0, 0
	for _, c := range fixture.Cases {
		if strings.HasPrefix(c.Name, "a1_") {
			a1++
		} else if !c.GoRejects {
			extended++
		}
	}
	if a1 < 10 || extended < 20 {
		t.Fatalf("golden matrix shrunk: a1=%d (want ≥10) extended=%d (want ≥20)", a1, extended)
	}
	return fixture.Cases
}

// TestFlowHashGoldenMatchesPython is the cross-engine pin (#60): for every
// fixture case the Go canonical text must be BYTE-identical to Python's
// json.dumps(sort_keys=True, ensure_ascii=False, separators=(",",":")) output
// and the fingerprint must equal Python's compute_flow_definition_hash.
// The A1 r120 matrix (a1_* families) is the acceptance table of plaita-go#60:
// html_chars / num_1e2 / u2028 diverged pre-fix, the other seven already
// agreed and must stay equal.
func TestFlowHashGoldenMatchesPython(t *testing.T) {
	for _, c := range flowHashGolden(t) {
		c := c
		if c.GoRejects || strings.HasPrefix(c.Name, "err_") {
			continue // covered by TestFlowHashGoldenGoRejects
		}
		t.Run(c.Name, func(t *testing.T) {
			canonical, err := canonicalFlowDefinitionJSON([]byte(c.Raw))
			if err != nil {
				t.Fatalf("canonicalize: %v", err)
			}
			if string(canonical) != c.PythonCanonical {
				t.Fatalf("canonical text diverges from Python:\n go: %q\n py: %q", string(canonical), c.PythonCanonical)
			}
			got, err := FlowDefinitionHash([]byte(c.Raw))
			if err != nil {
				t.Fatalf("hash: %v", err)
			}
			if got != c.PythonHash {
				t.Fatalf("hash diverges from Python: go=%s py=%s", got, c.PythonHash)
			}
		})
	}
}

// TestFlowHashGoldenGoRejects pins the decode-layer rejection surface: Go's
// strict RFC-8259 decoder refuses trailing data (Python agrees) and the
// NaN/Infinity literals (Python's parse_constant is lenient — a recorded
// residual in the fixture; see export_flow_hash_golden.py).
func TestFlowHashGoldenGoRejects(t *testing.T) {
	for _, c := range flowHashGolden(t) {
		c := c
		if !c.GoRejects && !strings.HasPrefix(c.Name, "err_") {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			if _, err := FlowDefinitionHash([]byte(c.Raw)); err == nil {
				t.Fatalf("FlowDefinitionHash accepted %q — must stay rejected", c.Raw)
			}
		})
	}
}

// TestFlowHashAlgoMarkerDistinctFromPython: the Go-stamped algorithm marker
// (#60) must differ from Python's "flow-definition-json-sortkeys-v2" so a
// same-name-different-implementation regression can never hide from
// upgrade_suspected again.
func TestFlowHashAlgoMarkerDistinctFromPython(t *testing.T) {
	if FlowHashAlgoV2 == "flow-definition-json-sortkeys-v2" {
		t.Fatal("Go algo marker equals Python's pre-#60 marker — the rename regressed")
	}
	if !strings.HasPrefix(FlowHashAlgoV2, "flow-definition-json-sortkeys-") {
		t.Fatalf("algo marker %q drifted from the algorithm family", FlowHashAlgoV2)
	}
}

// goldenCase returns one fixture case by name (e2e tests consume the
// Python-computed hash straight from the fixture).
func goldenCase(t *testing.T, name string) (raw, pythonHash string) {
	t.Helper()
	for _, c := range flowHashGolden(t) {
		if c.Name == name {
			return c.Raw, c.PythonHash
		}
	}
	t.Fatalf("golden case %q not found", name)
	return "", ""
}

// sha256Hex is the digest of an arbitrary byte string (used to fabricate a
// genuinely-changed definition hash for the negative control).
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
