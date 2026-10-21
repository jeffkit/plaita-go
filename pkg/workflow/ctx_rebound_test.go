package workflow_test

// #48 ctx-rebound negative control (engine level): the sleep node used a bare
// time.Sleep — ctx-blind — so one oversized Duration occupied the engine past
// any cancellation and the caller's deadline terminal (worker-side
// execution_deadline_exceeded) never became reachable. Post-fix the node wakes
// on ctx.Done and names the cancellation. The node is engine-reachable only:
// the IR rejects "sleep" as not part of the plaita dialect, so worker-deployed
// flows cannot carry it (the worker-level control for the other ctx-blind
// node — js code — lives in pkg/worker/ctx_rebound_test.go).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/jeffkit/plaita-go/pkg/workflow/nodes"
)

func TestSleepNodeReboundsContextCancellation(t *testing.T) {
	wf := &workflow.Workflow{
		ID:         "ctx-rebound-sleep",
		Runtime:    workflow.RuntimeGo,
		Version:    "v1.0",
		InputType:  workflow.Property{DataType: workflow.Integer},
		OutputType: workflow.Property{DataType: workflow.String},
		Nodes: workflow.Nodes{
			&nodes.Start{BaseNode: nodes.BaseNode{ID: "start", Next: "nap"}},
			// The issue's config shape: one absurd Duration pinning the
			// consumer (10 years).
			&nodes.Sleep{BaseNode: nodes.BaseNode{ID: "nap", Next: "end"}, Duration: 87600 * time.Hour},
			&nodes.End{
				BaseNode: nodes.BaseNode{
					ID:         "end",
					Output:     "awake",
					OutputType: workflow.Property{DataType: workflow.String},
				},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	began := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := wf.Execute(ctx, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled (named cancellation; a plain cancel must not be absorbed as a node timeout)", err)
		}
		if el := time.Since(began); el > 3*time.Second {
			t.Fatalf("Execute returned after %s — the wake is not prompt", el)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sleep 87600h still occupying the engine 5s after cancel: ctx-blind node (pre-#48 negative control)")
	}
}
