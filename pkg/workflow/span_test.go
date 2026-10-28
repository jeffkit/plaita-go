package workflow_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jeffkit/plaita-go/pkg/ir"
	"github.com/jeffkit/plaita-go/pkg/workflow"
	_ "github.com/jeffkit/plaita-go/pkg/workflow/nodes" // node registry (init)
)

// spanRecorder collects the spans seen by node_end listeners.
type spanRecorder struct {
	mu    sync.Mutex
	spans []*workflow.NodeSpan
}

func (r *spanRecorder) register() {
	workflow.OnNodeEnd(func(ctx context.Context, flow *workflow.Workflow, node workflow.Node, result any, err error, args map[string]interface{}) {
		if span := workflow.SpanFromContext(ctx); span != nil {
			r.mu.Lock()
			r.spans = append(r.spans, span)
			r.mu.Unlock()
		}
	})
}

func (r *spanRecorder) forNode(id string) []*workflow.NodeSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*workflow.NodeSpan
	for _, s := range r.spans {
		if s.NodeID == id {
			out = append(out, s)
		}
	}
	return out
}

func flowFromNodes(t *testing.T, nodes string) *workflow.Workflow {
	t.Helper()
	wf, err := ir.Load([]byte(nodes))
	if err != nil {
		t.Fatal(err)
	}
	return wf
}

func TestSpanLoopIterationsDistinct(t *testing.T) {
	rec := &spanRecorder{}
	rec.register()
	wf := flowFromNodes(t, `{"id":"span-loop","nodes":[
	 {"id":"start","type":"start","next":"loop"},
	 {"id":"loop","type":"loop","collection":"[1,2,3]","childFlow":{"id":"body","nodes":[
	   {"id":"b1","type":"start","next":"b2"},
	   {"id":"b2","type":"end","resultType":"success"}]},"next":"end"},
	 {"id":"end","type":"end","resultType":"success"}]}`)
	if _, err := wf.Execute(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	spans := rec.forNode("b1")
	if len(spans) != 3 {
		t.Fatalf("b1 must record 3 spans (one per round), got %d", len(spans))
	}
	seen := map[string]bool{}
	for i, s := range spans {
		if s.SpanID == "" || seen[s.SpanID] {
			t.Fatalf("round %d span_id must be unique and non-empty: %q", i, s.SpanID)
		}
		seen[s.SpanID] = true
		if s.Iteration != i {
			t.Fatalf("round %d iteration=%v want %d", i, s.Iteration, i)
		}
		if len(s.FramePath) != 1 || s.FramePath[0] != "loop" {
			t.Fatalf("frame_path=%v want [loop]", s.FramePath)
		}
	}
}

func TestSpanNestedLoopFramePath(t *testing.T) {
	rec := &spanRecorder{}
	rec.register()
	wf := flowFromNodes(t, `{"id":"span-nested","nodes":[
	 {"id":"start","type":"start","next":"outer"},
	 {"id":"outer","type":"loop","collection":"[1]","childFlow":{"id":"ob","nodes":[
	   {"id":"o1","type":"start","next":"inner"},
	   {"id":"inner","type":"loop","collection":"[1,2]","childFlow":{"id":"ib","nodes":[
	     {"id":"i1","type":"start","next":"i2"},
	     {"id":"i2","type":"end","resultType":"success"}]},"next":"o3"},
	   {"id":"o3","type":"end","resultType":"success"}]},"next":"end"},
	 {"id":"end","type":"end","resultType":"success"}]}`)
	if _, err := wf.Execute(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	spans := rec.forNode("i1")
	if len(spans) != 2 {
		t.Fatalf("i1 must record 2 spans, got %d", len(spans))
	}
	for i, s := range spans {
		if len(s.FramePath) != 2 || s.FramePath[0] != "outer" || s.FramePath[1] != "inner" {
			t.Fatalf("round %d frame_path=%v want [outer inner]", i, s.FramePath)
		}
	}
	// The inner loop NODE itself carries iteration under the outer frame.
	outer := rec.forNode("inner")
	if len(outer) != 1 || outer[0].Iteration != 0 || len(outer[0].FramePath) != 1 || outer[0].FramePath[0] != "outer" {
		t.Fatalf("inner loop node span wrong: %+v", outer)
	}
}

func TestSpanParentChain(t *testing.T) {
	rec := &spanRecorder{}
	rec.register()
	wf := flowFromNodes(t, `{"id":"span-parent","nodes":[
	 {"id":"start","type":"start","next":"loop"},
	 {"id":"loop","type":"loop","collection":"[1]","childFlow":{"id":"b","nodes":[
	   {"id":"b1","type":"start","next":"b2"},
	   {"id":"b2","type":"end","resultType":"success"}]},"next":"end"},
	 {"id":"end","type":"end","resultType":"success"}]}`)
	if _, err := wf.Execute(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	loopSpans := rec.forNode("loop")
	childSpans := rec.forNode("b1")
	if len(loopSpans) != 1 || len(childSpans) != 1 {
		t.Fatalf("expected 1 loop span + 1 child span, got %d/%d", len(loopSpans), len(childSpans))
	}
	if loopSpans[0].ParentSpanID != "" {
		t.Fatalf("root-frame loop span must have empty parent: %q", loopSpans[0].ParentSpanID)
	}
	if childSpans[0].ParentSpanID != loopSpans[0].SpanID {
		t.Fatalf("child span parent=%q want loop span %q", childSpans[0].ParentSpanID, loopSpans[0].SpanID)
	}
}

func TestSpanParallelBranchIteration(t *testing.T) {
	rec := &spanRecorder{}
	rec.register()
	wf := flowFromNodes(t, `{"id":"span-par","nodes":[
	 {"id":"start","type":"start","next":"par"},
	 {"id":"par","type":"parallel","joinBranches":["alpha","beta"],"branches":[
	   {"name":"alpha","flow":{"id":"fa","nodes":[{"id":"a1","type":"start","next":"a2"},{"id":"a2","type":"end","resultType":"success"}]}},
	   {"name":"beta","flow":{"id":"fb","nodes":[{"id":"b1","type":"start","next":"b2"},{"id":"b2","type":"end","resultType":"success"}]}}],
	  "next":"end"},
	 {"id":"end","type":"end","resultType":"success"}]}`)
	if _, err := wf.Execute(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	a := rec.forNode("a1")
	b := rec.forNode("b1")
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("expected 1 span per branch, got %d/%d", len(a), len(b))
	}
	if a[0].Iteration != "alpha" || len(a[0].FramePath) != 1 || a[0].FramePath[0] != "par" {
		t.Fatalf("alpha span wrong: %+v", a[0])
	}
	if b[0].Iteration != "beta" {
		t.Fatalf("beta span iteration=%v want beta", b[0].Iteration)
	}
}

func TestSpanChildFramePath(t *testing.T) {
	rec := &spanRecorder{}
	rec.register()
	wf := flowFromNodes(t, `{"id":"span-child","nodes":[
	 {"id":"start","type":"start","next":"child"},
	 {"id":"child","type":"child","childFlow":{"id":"sub","nodes":[
	   {"id":"s1","type":"start","next":"s2"},
	   {"id":"s2","type":"end","resultType":"success"}]},"next":"end"},
	 {"id":"end","type":"end","resultType":"success"}]}`)
	if _, err := wf.Execute(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	spans := rec.forNode("s1")
	if len(spans) != 1 {
		t.Fatalf("s1 must record 1 span, got %d", len(spans))
	}
	if len(spans[0].FramePath) != 1 || spans[0].FramePath[0] != "child" {
		t.Fatalf("child frame_path=%v want [child]", spans[0].FramePath)
	}
	if spans[0].Iteration != nil {
		t.Fatalf("child frame iteration must be nil, got %v", spans[0].Iteration)
	}
}

func TestSpanToDictShape(t *testing.T) {
	s := &workflow.NodeSpan{SpanID: "n1-abc", ParentSpanID: "loop-xyz", FramePath: []string{"loop"}, Iteration: 2, NodeID: "n1"}
	d := s.ToDict()
	if d["span_id"] != "n1-abc" || d["iteration"] != 2 {
		t.Fatalf("dict wrong: %+v", d)
	}
	if fp, ok := d["frame_path"].([]any); !ok || len(fp) != 1 || fp[0] != "loop" {
		t.Fatalf("frame_path must render as a list: %+v", d["frame_path"])
	}
	if (*workflow.NodeSpan)(nil).ToDict() != nil {
		t.Fatal("nil span dict must be nil")
	}
}
