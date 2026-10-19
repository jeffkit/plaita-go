package nodes

import (
	"context"
	"fmt"
	"sync"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/iter"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeParallel,
		func(ctx context.Context) workflow.Node { return new(Parallel) },
	)
}

// ParallelModel represents node logic execute model for the parallel node
type ParallelModel string

// Parallel branches execute models
const (
	ArtificialParallelModel ParallelModel = "artificial"
	CoroutineParallelModel  ParallelModel = "coroutine"
	ProcessParallelModel    ParallelModel = "process"
	ThreadParallelModel     ParallelModel = "thread"
)

var supportedParallelModels = []ParallelModel{
	ArtificialParallelModel,
	CoroutineParallelModel,
	ProcessParallelModel,
	ThreadParallelModel,
}

// ParallelBranch represents a branch of a parallel node
type ParallelBranch struct {
	Branch
	Flow  workflow.Workflow `json:"flow"`
	Input map[string]any    `json:"input,omitempty"`
}

// Validate validates the parallel branch
func (pb *ParallelBranch) Validate() error {
	return validation.ValidateStruct(pb,
		validation.Field(&pb.Branch),
		validation.Field(&pb.Flow, validation.Required))
}

// Parallel represents a parallel node
type Parallel struct {
	BaseNode
	Branches      []ParallelBranch `json:"branches"`
	IsConditional bool             `json:"isConditional"`
	Mode          ParallelModel    `json:"mode,omitempty"`
	JoinBranches  []string         `json:"joinBranches,omitempty"`
}

// Validate validates the parallel node
func (p *Parallel) Validate() error {
	return validation.ValidateStruct(p,
		validation.Field(&p.Branches, validation.Required),
		validation.Field(&p.Mode, validation.In(lo.ToAnySlice(supportedParallelModels)...)),
		validation.Field(&p.BaseNode),
		validation.Field(&p.Branches, validation.Each()))
}

// GetType returns the parallel node type
func (p *Parallel) GetType() workflow.NodeType {
	return types.NodeTypeParallel
}

// Execute executes the parallel node
func (p *Parallel) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	branches, err := p.matchConditionBranches(ctx)
	if err != nil {
		return nil, err
	}

	p.backgroundExecBranches(ctx, lo.Filter(branches, func(item ParallelBranch, _ int) bool {
		return !lo.Contains(p.JoinBranches, item.Name)
	}))

	joinBranches := lo.Filter(branches, func(item ParallelBranch, _ int) bool {
		if len(p.JoinBranches) == 0 {
			return true
		}
		return lo.Contains(p.JoinBranches, item.Name)
	})
	results, err := p.execJoinBranches(ctx, joinBranches)
	return &workflow.ExecuteResult{Result: results, NextNode: p.Next}, err
}

func (p *Parallel) execJoinBranches(ctx context.Context, joinBranches []ParallelBranch) (map[string]any, error) {
	results, err := iter.MapErr(joinBranches, func(pb *ParallelBranch) (any, error) {
		branchCtx := runtime.CloneContext(ctx)
		if pb.Flow.Timeout.IsZero() {
			pb.Flow.Timeout = p.Timeout
		}

		// plaita semantics: a branch without an explicit input inherits the
		// parent workflow input ($INPUT stays visible inside the branch).
		var subWorkflowInput any
		if pb.Input != nil {
			var err error
			subWorkflowInput, err = expression.Eval(branchCtx, pb.Input)
			if err != nil {
				return nil, err
			}
		}
		if subWorkflowInput == nil {
			subWorkflowInput = runtime.WorkflowRuntimeContext(ctx).WorkflowInput
		}
		return pb.Flow.Execute(branchCtx, subWorkflowInput)
	})

	execResult := make(map[string]any)
	for i, r := range results {
		if r == nil {
			continue
		}
		execResult[joinBranches[i].Name] = r
	}
	return execResult, err
}

type execFuture struct {
	mutex sync.Mutex

	results map[string]any
	errs    map[string]error
}

func newExecFuture() *execFuture {
	return &execFuture{
		results: make(map[string]any),
		errs:    make(map[string]error),
	}
}

func (e *execFuture) set(key string, v any, err error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if v != nil {
		e.results[key] = v
	}
	if err != nil {
		e.errs[key] = err
	}
}

func (p *Parallel) backgroundExecBranches(ctx context.Context, branches []ParallelBranch) *execFuture {
	future := newExecFuture()
	go func() {
		iter.ForEach(branches, func(pb *ParallelBranch) {
			ctx = runtime.CloneContext(ctx)
			subWorkflowInput, err := expression.Eval(ctx, pb.Input)
			if err != nil {
				future.set(pb.Name, nil, err)
				return
			}

			result, err := pb.Flow.Execute(ctx, subWorkflowInput)
			future.set(pb.Name, result, err)
		})
	}()
	return future
}

func (p *Parallel) matchConditionBranches(ctx context.Context) ([]ParallelBranch, error) {
	if !p.IsConditional {
		return p.Branches, nil
	}

	var bs []ParallelBranch
	for _, b := range p.Branches {
		match, _, err := b.MatchContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("parallel node %q branch %q match condition error: %w", p.Name, b.Name, err)
		}

		if match {
			bs = append(bs, b)
		}
	}
	return bs, nil
}
