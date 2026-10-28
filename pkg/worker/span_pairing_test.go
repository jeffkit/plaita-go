package worker

import (
	"testing"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// span 模型配对（plaita#344 对应物）：同一 node_id 的多轮/并发帧按 span_id
// 独立成 occurrence，聚合面（attempts/duration 语义）保持向后兼容。

func spanFor(id, nodeID string, iteration any) *workflow.NodeSpan {
	return &workflow.NodeSpan{
		SpanID:    id,
		NodeID:    nodeID,
		FramePath: []string{"loop"},
		Iteration: iteration,
	}
}

func TestExecutionRunSpanOccurrences(t *testing.T) {
	r := &executionRun{nodeTimings: map[string]map[string]any{}}

	r.beginNode("n1", "end", "", spanFor("n1-aaa", "n1", 0))
	r.endNode("n1", "end", "", false, spanFor("n1-aaa", "n1", 0))
	r.beginNode("n1", "end", "", spanFor("n1-bbb", "n1", 1))
	r.endNode("n1", "end", "", false, spanFor("n1-bbb", "n1", 1))
	r.beginNode("n1", "end", "", spanFor("n1-ccc", "n1", 2))
	r.endNode("n1", "end", "", false, spanFor("n1-ccc", "n1", 2))

	entry := r.nodeTimings["n1"]
	if got := toInt(entry["attempts"]); got != 3 {
		t.Fatalf("aggregate attempts=%d want 3 (backward compatible)", got)
	}
	occs, ok := entry["occurrences"].([]map[string]any)
	if !ok || len(occs) != 3 {
		t.Fatalf("occurrences=%#v want 3", entry["occurrences"])
	}
	for i, occ := range occs {
		if occ["ended_at"] == "" {
			t.Fatalf("occurrence %d must be closed", i)
		}
		if occ["duration_ms"] == nil {
			t.Fatalf("occurrence %d must carry its own duration", i)
		}
		if occ["iteration"] != i {
			t.Fatalf("occurrence %d iteration=%v", i, occ["iteration"])
		}
	}
}

func TestExecutionRunNilSpanKeepsAggregateOnly(t *testing.T) {
	r := &executionRun{nodeTimings: map[string]map[string]any{}}
	// resume 收尾腿：无 start 可配对，维持无 span 回退。
	r.beginNode("n1", "end", "", nil)
	r.endNode("n1", "end", "", false, nil)
	entry := r.nodeTimings["n1"]
	if _, has := entry["occurrences"]; has {
		t.Fatalf("nil span must not create occurrences")
	}
	if toInt(entry["attempts"]) != 1 {
		t.Fatalf("aggregate still counts: attempts=%d", toInt(entry["attempts"]))
	}
}

func TestExecutionRunSpanTimelineCarriesIdentity(t *testing.T) {
	r := &executionRun{nodeTimings: map[string]map[string]any{}}
	ev := r.beginNode("n1", "end", "", spanFor("n1-xyz", "n1", "branchA"))
	if ev["span_id"] != "n1-xyz" || ev["iteration"] != "branchA" {
		t.Fatalf("timeline event missing span identity: %+v", ev)
	}
	fp, ok := ev["frame_path"].([]any)
	if !ok || len(fp) != 1 || fp[0] != "loop" {
		t.Fatalf("timeline frame_path must render as list: %+v", ev["frame_path"])
	}
}
