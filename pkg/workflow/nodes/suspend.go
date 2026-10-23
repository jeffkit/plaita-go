package nodes

// 嵌套挂起 v1 的静态分支闸（rs flow_contains_suspending 同形）。

import (
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// flowContainsSuspending statically scans a flow graph (including nested
// child/reference flows, collection bodies and parallel branches) for a
// suspending node. The parallel node runs it before dispatching branches: a
// background branch's dynamic scope-gate rejection is asynchronous and cannot
// surface synchronously, so v1 semantics = a branch graph must not contain a
// suspending node at all (a deterministic, synchronous named rejection).
func flowContainsSuspending(f *workflow.Workflow) bool {
	if f == nil {
		return false
	}
	for _, n := range f.Nodes {
		if s, ok := n.(workflow.Suspender); ok && s.IsSuspending() {
			return true
		}
		switch t := n.(type) {
		case *Child:
			if flowContainsSuspending(t.ChildFlow) {
				return true
			}
		case *Reference:
			if t.ChildFlow != nil && flowContainsSuspending(t.ChildFlow) {
				return true
			}
		case *Loop:
			if flowContainsSuspending(t.ChildFlow) {
				return true
			}
		case *Map:
			if flowContainsSuspending(t.ChildFlow) {
				return true
			}
		case *Filter:
			if flowContainsSuspending(t.ChildFlow) {
				return true
			}
		case *Find:
			if flowContainsSuspending(t.ChildFlow) {
				return true
			}
		case *Reduce:
			if flowContainsSuspending(t.ChildFlow) {
				return true
			}
		case *While:
			if flowContainsSuspending(t.ChildFlow) {
				return true
			}
		case *Parallel:
			for _, b := range t.Branches {
				if flowContainsSuspending(b.Flow) {
					return true
				}
			}
		}
	}
	return false
}
