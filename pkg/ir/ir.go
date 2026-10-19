// Package ir loads plaita deployment-form flow JSON into an executable
// engine workflow.
//
// The deployment JSON is the single authoritative contract between the
// plaita Python frontend (@flow compile / console publish) and this engine.
// It is produced by plaita's codeflow compiler: top-level
// `flow_id`/`inputType`/`nodes`/`runtime`, camelCase node fields with a few
// snake residues the loader tolerates (`source_line`, `flow_id`,
// `timeout_handler`, ...). Legacy console exports (go-era shape with `id`,
// `required`/`default`/`children` property keys) are normalized too.
//
// The loader is a strict accept-list by design (fail-closed): plaita node
// types that this engine has not ported (decision, collection variants,
// python code, ...) are rejected at load with an explicit message instead of
// being silently skipped. Contract changes must keep the round-trip with
// plaita's ir_validate rules; see AGENTS.md at the repo root.
package ir

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/jeffkit/plaita-go/pkg/workflow"
	// register the builtin node types the loader accepts
	_ "github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

// Load parses plaita deployment-form flow JSON and builds the engine
// workflow. Structural problems (missing start, dangling refs, unreachable
// nodes, unsupported node types) are reported as errors before execution.
func Load(data []byte) (*workflow.Workflow, error) {
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("ir: parse flow json: %w", err)
	}
	if err := normalizeDoc(doc, scopeRoot); err != nil {
		return nil, err
	}
	if err := validateDoc(doc, scopeRoot); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("ir: re-encode flow: %w", err)
	}
	wf := new(workflow.Workflow)
	if err := json.Unmarshal(raw, wf); err != nil {
		return nil, fmt.Errorf("ir: build workflow: %w", err)
	}
	return wf, nil
}

// scope distinguishes the root flow from embedded flows (loop/child bodies,
// parallel branches). Embedded flows must additionally reach an end node,
// mirroring plaita's check_childflow_reachable_end.
type scope bool

const (
	scopeRoot  scope = false
	scopeChild scope = true
)

// supportedNodeTypes is the strict accept-list of plaita node types this
// engine implements.
var supportedNodeTypes = map[string]bool{
	"start":      true,
	"end":        true,
	"assignment": true,
	"switch":     true,
	"parallel":   true,
	"loop":       true,
	"child":      true,
	"http":       true,
	"redis":      true,
	"code":       true,
}

// knownUnsupported types carry a targeted hint; anything else unknown gets a
// generic message listing what is available.
var knownUnsupported = map[string]string{
	"decision":  "structured decision node (plaita-nodes) is not ported yet",
	"map":       "collection variant, not ported yet (base loop is supported)",
	"filter":    "collection variant, not ported yet (base loop is supported)",
	"find":      "collection variant, not ported yet (base loop is supported)",
	"reduce":    "collection variant, not ported yet (base loop is supported)",
	"while":     "condition loop variant, not ported yet (base loop is supported)",
	"if":        "bool branch node, use switch",
	"case":      "legacy switch dialect, use switch",
	"calculate": "calculate node is not ported yet",
	"event":     "suspending event node is not ported yet",
	"mock":      "mock node is not ported yet",
	"reference": "provider-based child flow reference is not ported yet (use inline childFlow)",
	"sleep":     "sleep node is not part of the plaita dialect",
	"trpc":      "internal RPC node was removed from this engine",
	"mdb":       "internal database node was removed from this engine",
	"readmdb":   "internal database node was removed from this engine",
	"writemdb":  "internal database node was removed from this engine",
}
