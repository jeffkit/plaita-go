package workflow

// NodeSpan — the node execution instance identity (plaita#344 对应物,
// plaita-go#90). Python semantics: one span per node execution instance —
// loop/map rounds and concurrent frames of the same node_id get distinct
// spans, so per-round timings stop overwriting each other and concurrent
// start/end pairs stop mis-differencing. Recorders pair by span_id; the
// node_id aggregate stays backward compatible.
//
// Wiring (the engine stamps; composite nodes need zero changes):
//
//   - Workflow.executeNode builds the span BEFORE the retry loop (one span
//     covers all attempts of that node execution instance) and stamps it
//     into the ctx handed to the listeners — listeners read it via
//     SpanFromContext (Go's positional listener signatures have no kwarg
//     channel; the ctx stamp IS the span= kwarg's counterpart, and Python
//     stamps the same fields into its execution context).
//   - frame_path / iteration come from the runtime context's frame state
//     (FramePath extended + FrameKey set at every frame-creation site:
//     loop/while/collection iterations, parallel branches, child/reference
//     calls).
//
// Boundary: the resume leg (Workflow.Resume / ResumeSuspension closing a
// suspended node's timing without a same-process start) has no span —
// recorders fall back to the un-paired aggregate, matching Python.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

// NodeSpan identifies one node execution instance.
type NodeSpan struct {
	SpanID       string
	ParentSpanID string   // empty = root frame
	FramePath    []string // frame node ids root→leaf
	Iteration    any      // int (loop family) / string (parallel branch) / nil
	NodeID       string
}

// ToDict renders the span for timeline/observation payloads (Python
// NodeSpan.to_dict shape: frame_path as a list).
func (s *NodeSpan) ToDict() map[string]any {
	if s == nil {
		return nil
	}
	framePath := make([]any, 0, len(s.FramePath))
	for _, id := range s.FramePath {
		framePath = append(framePath, id)
	}
	return map[string]any{
		"span_id":        s.SpanID,
		"parent_span_id": s.ParentSpanID,
		"frame_path":     framePath,
		"iteration":      s.Iteration,
		"node_id":        s.NodeID,
	}
}

type spanCtxKey struct{}

// SpanFromContext returns the current node execution span, or nil on the
// resume leg / legacy hosts (observation never blocks execution).
func SpanFromContext(ctx context.Context) *NodeSpan {
	if ctx == nil {
		return nil
	}
	span, _ := ctx.Value(spanCtxKey{}).(*NodeSpan)
	return span
}

// WithSpan stamps the span into ctx.
func WithSpan(ctx context.Context, span *NodeSpan) context.Context {
	return context.WithValue(ctx, spanCtxKey{}, span)
}

// BuildNodeSpan reads the frame state + parent span from ctx and mints the
// instance-unique span: span_id = node_id-random12 (Python build_node_span).
func BuildNodeSpan(ctx context.Context, node Node) *NodeSpan {
	nodeID := string(node.GetID())
	parent := SpanFromContext(ctx)
	framePath, iteration := frameState(ctx)
	var parentID string
	if parent != nil {
		parentID = parent.SpanID
	}
	return &NodeSpan{
		SpanID:       fmt.Sprintf("%s-%s", nodeID, randomHex12()),
		ParentSpanID: parentID,
		FramePath:    framePath,
		Iteration:    iteration,
		NodeID:       nodeID,
	}
}

// frameState reads frame_path/iteration from the runtime context (any
// absence = root frame — duck-read like Python).
func frameState(ctx context.Context) ([]string, any) {
	rc := runtime.WorkflowRuntimeContext(ctx)
	if rc == nil {
		return nil, nil
	}
	return rc.FramePath, rc.FrameKey
}

// ExtendFramePath appends the frame node's id to the current frame path
// (the frame entrypoints' shared helper — span 模型的帧路径延长).
func ExtendFramePath(ctx context.Context, frameNodeID string) []string {
	rc := runtime.WorkflowRuntimeContext(ctx)
	var parent []string
	if rc != nil {
		parent = rc.FramePath
	}
	out := make([]string, 0, len(parent)+1)
	out = append(out, parent...)
	return append(out, frameNodeID)
}

func randomHex12() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing is catastrophic anyway; a constant keeps the
		// span well-formed (observation never blocks execution).
		return "000000000000"
	}
	return hex.EncodeToString(b)
}
