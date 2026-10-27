package nodes

// 嵌套挂起 v1 的静态分支闸（rs flow_contains_suspending 同形）。

import (
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// SuspendNodeRef identifies one suspending node found by a static scan
// (inline entry gates report which nodes need a managed host — Python
// 「报缺不报档」: name the nodes, not the tier vocabulary).
type SuspendNodeRef struct {
	NodeID   string
	NodeType string
}

// FlowSuspendingNodes statically scans a flow graph (including nested
// child/reference flows, collection bodies and parallel branches) and returns
// every suspending node in deterministic graph order. Same traversal as
// flowContainsSuspending, exposed for entry gates that must name the nodes
// (cmd/plaita inline run) instead of a boolean.
func FlowSuspendingNodes(f *workflow.Workflow) []SuspendNodeRef {
	if f == nil {
		return nil
	}
	var out []SuspendNodeRef
	for _, n := range f.Nodes {
		if s, ok := n.(workflow.Suspender); ok && s.IsSuspending() {
			out = append(out, SuspendNodeRef{NodeID: string(n.GetID()), NodeType: string(n.GetType())})
		}
		switch t := n.(type) {
		case *Child:
			out = append(out, FlowSuspendingNodes(t.ChildFlow)...)
		case *Reference:
			if t.ChildFlow != nil {
				out = append(out, FlowSuspendingNodes(t.ChildFlow)...)
			}
		case *Loop:
			out = append(out, FlowSuspendingNodes(t.ChildFlow)...)
		case *Map:
			out = append(out, FlowSuspendingNodes(t.ChildFlow)...)
		case *Filter:
			out = append(out, FlowSuspendingNodes(t.ChildFlow)...)
		case *Find:
			out = append(out, FlowSuspendingNodes(t.ChildFlow)...)
		case *Reduce:
			out = append(out, FlowSuspendingNodes(t.ChildFlow)...)
		case *While:
			out = append(out, FlowSuspendingNodes(t.ChildFlow)...)
		case *Parallel:
			for _, b := range t.Branches {
				out = append(out, FlowSuspendingNodes(b.Flow)...)
			}
		}
	}
	return out
}

// flowContainsSuspending is the v1 parallel-branch static gate: a branch
// graph must not contain a suspending node at all (see FlowSuspendingNodes).
func flowContainsSuspending(f *workflow.Workflow) bool {
	return len(FlowSuspendingNodes(f)) > 0
}
