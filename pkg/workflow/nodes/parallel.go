package nodes

import (
	"context"
	"fmt"
	"log"
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
	Flow  *workflow.Workflow `json:"flow"`
	Input map[string]any     `json:"input,omitempty"`
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
	Branches      []*ParallelBranch `json:"branches"`
	IsConditional bool              `json:"isConditional"`
	Mode          ParallelModel     `json:"mode,omitempty"`
	JoinBranches  []string          `json:"joinBranches,omitempty"`
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

	// plaita 语义（concurrent.py _split_branches）：只有显式列进 JoinBranches
	// 的分支阻塞等待；JoinBranches 为空 = 全部后台 fire-and-forget，节点立即
	// 返回空结果 {}——与"空列表=全部 join"的旧语义正好相反（issue #7）。
	joinBranches, backgroundBranches := p.splitBranches(branches)

	p.backgroundExecBranches(ctx, backgroundBranches)

	results := p.execJoinBranches(ctx, joinBranches)
	return &workflow.ExecuteResult{Result: results, NextNode: p.Next}, nil
}

// splitBranches ports concurrent.py Parallel._split_branches: membership in
// JoinBranches decides the split; an empty JoinBranches sends every branch to
// background and joins nothing.
func (p *Parallel) splitBranches(branches []*ParallelBranch) (join, background []*ParallelBranch) {
	for _, b := range branches {
		if lo.Contains(p.JoinBranches, b.Name) {
			join = append(join, b)
		} else {
			background = append(background, b)
		}
	}
	return join, background
}

// branchOutcome 是单个 join 分支的执行产物（结果或错误）。
type branchOutcome struct {
	result any
	err    error
}

// execJoinBranches 并发执行 join 分支并把每个分支的产物收进结果 map。分支
// 失败不让整个节点失败（concurrent.py _process_future_result）：该分支的槽位
// 写入哨兵 {"__parallel_error__": <msg>, "__branch__": <name>}，下游一眼可
// 识别（不像 nil 那样与合法返回混淆）；是否整体失败交由节点/流程 errorHandler
// 决定。
func (p *Parallel) execJoinBranches(ctx context.Context, joinBranches []*ParallelBranch) map[string]any {
	execResult := make(map[string]any, len(joinBranches))
	if len(joinBranches) == 0 {
		return execResult
	}

	outcomes := make([]branchOutcome, len(joinBranches))
	iter.ForEachIdx(joinBranches, func(i int, pbRef **ParallelBranch) {
		result, err := p.execBranchFlow(ctx, *pbRef)
		outcomes[i] = branchOutcome{result: result, err: err}
	})

	for i, br := range joinBranches {
		if outcomes[i].err != nil {
			log.Printf("parallel 节点 %q join 分支 %q 失败，结果置为错误哨兵: %v", p.GetID(), br.Name, outcomes[i].err)
			execResult[br.Name] = map[string]any{
				"__parallel_error__": outcomes[i].err.Error(),
				"__branch__":         br.Name,
			}
			continue
		}
		execResult[br.Name] = outcomes[i].result
	}
	return execResult
}

// execBranchFlow 解析分支输入并执行分支子流程，join 与 background 共用。
//
// plaita 语义（concurrent.py _resolve_branch_input）：分支未显式声明 input 时
// 继承父流程 $INPUT，使分支子流程能自然地用 INPUT.x 访问父流程输入字段；
// 显式声明了 input 的分支按表达式求值，不受影响。继承值取 CloneContext 的
// 深拷贝快照（对齐 plaita#69 的边界物化——分支内就地改写不写穿父运行态）。
//
// issue #17：不在共享子 Workflow 上改写 Timeout——每次执行对 Workflow 结构做
// 浅拷贝（节点列表共享只读），超时兜底落在拷贝上，避免并发 run 互相改写。
func (p *Parallel) execBranchFlow(ctx context.Context, br *ParallelBranch) (any, error) {
	branchCtx := runtime.CloneContext(ctx)

	var subWorkflowInput any
	var err error
	if br.Input != nil {
		subWorkflowInput, err = expression.Eval(branchCtx, br.Input)
		if err != nil {
			return nil, fmt.Errorf("parallel branch %q resolve input error: %w", br.Name, err)
		}
	}
	if subWorkflowInput == nil {
		subWorkflowInput = runtime.WorkflowRuntimeContext(branchCtx).WorkflowInput
	}

	subFlow := p.branchFlowView(br)
	return subFlow.Execute(branchCtx, subWorkflowInput)
}

// branchFlowView 返回本次分支执行用的子 Workflow 副本：Timeout 兜底写在副本
// 上，不改写共享的子 Workflow（issue #17）。Workflow.Context 内含 sync 锁、
// 不能整结构浅拷贝（go vet），这里逐字段复制；Global 取执行时快照（经原
// Context 的读锁拷出）——分支看到的全局变量即 parallel 节点时刻的状态。
// Nodes 列表共享只读：switch/parallel 均已保证不在执行期改写节点状态。
func (p *Parallel) branchFlowView(br *ParallelBranch) *workflow.Workflow {
	subFlow := &workflow.Workflow{
		ID:                  br.Flow.ID,
		Name:                br.Flow.Name,
		Desc:                br.Flow.Desc,
		Runtime:             br.Flow.Runtime,
		Version:             br.Flow.Version,
		Author:              br.Flow.Author,
		Nodes:               br.Flow.Nodes,
		InputType:           br.Flow.InputType,
		OutputType:          br.Flow.OutputType,
		ErrorHandlePolicy:   br.Flow.ErrorHandlePolicy,
		TimeoutHandlePolicy: br.Flow.TimeoutHandlePolicy,
		Metadata:            br.Flow.Metadata,
		Context:             workflow.Context{Global: br.Flow.Context.GetGlobalVariables()},
	}
	if subFlow.Timeout.IsZero() {
		subFlow.Timeout = p.Timeout
	}
	return subFlow
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

// backgroundExecBranches fire-and-forget 地后台执行分支：不阻塞父流程，但
// 失败不再静默——记日志（对齐 Python _execute_background_branches 的
// done_callback）并写入返回的 execFuture（供测试/宿主观测）。
//
// 分支基础上下文在父 goroutine、派发之前克隆（对齐 plaita#72：子执行创建即
// 钉住 $PARENT/$INPUT 快照）——既保证后台分支读到的父状态停在 parallel 节点
// 时刻，也避免后台 goroutine 克隆时与父流程的继续写入构成数据竞争。
func (p *Parallel) backgroundExecBranches(ctx context.Context, branches []*ParallelBranch) *execFuture {
	future := newExecFuture()
	if len(branches) == 0 {
		return future
	}
	baseCtx := runtime.CloneContext(ctx)
	go func() {
		iter.ForEach(branches, func(pbRef **ParallelBranch) {
			pb := *pbRef
			result, err := p.execBranchFlow(baseCtx, pb)
			if err != nil {
				log.Printf("parallel 节点 %q 后台分支 %q 失败: %v", p.GetID(), pb.Name, err)
			}
			future.set(pb.Name, result, err)
		})
	}()
	return future
}

func (p *Parallel) matchConditionBranches(ctx context.Context) ([]*ParallelBranch, error) {
	if !p.IsConditional {
		return p.Branches, nil
	}

	var bs []*ParallelBranch
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
