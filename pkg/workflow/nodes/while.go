package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		nodeTypeWhile,
		func(ctx context.Context) workflow.Node { return new(While) },
	)
}

const nodeTypeWhile workflow.NodeType = "while"

// whileDefaultMaxIterations is plaita While.max_iterations' model default.
const whileDefaultMaxIterations = 1000

// While 转正 plaita 的 while 条件循环节点（plaita/node/loop.py While，#80
// phase 1）。先判断后执行：条件满足才继续下一轮；条件上下文可见 $LOOP-ITEM
// （上一轮子流程结果，首轮 None）与 $LOOP-INDEX（当前轮次，从 0 起）——
// Python While **不**注入 $LOOP-RESULT（那是 Loop 专属）。每轮子流程输入
// {item: 上一轮结果, index: 轮次}。condition 缺省/null → Python
// condition=None：只执行一轮。max_iterations 缺省 1000，触顶仅告警不报错
// （Python 同，_logger.warning）；节点输出 = 最后一轮子流程结果（零轮 → None）。
type While struct {
	Child
	// Condition / MaxIterations carry json tags for the ir loader's
	// known-keys reflection (nodeKnownKeys); decoding itself runs through the
	// custom UnmarshalJSON below, which also accepts the camel maxIterations.
	Condition     Condition `json:"condition"`
	MaxIterations *int      `json:"max_iterations"`
	// singleRound marks a condition-less while (JSON condition absent/null):
	// Python `condition is None → return index == 0` runs exactly one round.
	singleRound bool
}

// GetType returns the while node type.
func (w *While) GetType() workflow.NodeType {
	return nodeTypeWhile
}

// UnmarshalJSON unmarshals json to the while node. Both max_iterations
// spellings are accepted (compiler emits snake, console canonical emits
// camel — plaita#89 dual-read); condition presence must be detected on the
// raw JSON to distinguish "absent → one round" from a zero-value condition.
func (w *While) UnmarshalJSON(data []byte) error {
	t := &struct {
		Child
		ConditionRaw       json.RawMessage `json:"condition"`
		MaxIterations      *int            `json:"max_iterations"`
		MaxIterationsCamel *int            `json:"maxIterations"`
	}{}
	if err := json.Unmarshal(data, t); err != nil {
		return err
	}
	w.Child = t.Child
	w.MaxIterations = t.MaxIterations
	if w.MaxIterations == nil {
		w.MaxIterations = t.MaxIterationsCamel
	}
	if len(t.ConditionRaw) == 0 || string(t.ConditionRaw) == "null" {
		w.singleRound = true
	} else if err := json.Unmarshal(t.ConditionRaw, &w.Condition); err != nil {
		return err
	}
	return nil
}

// Execute executes the while node.
func (w *While) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	maxIterations := whileDefaultMaxIterations
	if w.MaxIterations != nil {
		maxIterations = *w.MaxIterations
	}

	runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
	index := 0
	var result any
	for index < maxIterations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if w.singleRound && index > 0 {
			break
		}
		// condition 在父侧求值：注入 $LOOP-ITEM/$LOOP-INDEX（与 loop.go 同一
		// CurrentNodeContext 通道）。item = 上一轮子流程结果，首轮为 None。
		runtimeCtx.WithCurrentNodeContext(loopItemKey, result)
		runtimeCtx.WithCurrentNodeContext(loopIndexKey, index)
		matched, _, err := w.Condition.MatchContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("while condition error: %v", err)
		}
		if !matched {
			break
		}
		result, err = runCollectionChild(ctx, w.ChildFlow, map[string]any{"item": result, "index": index})
		if err != nil {
			return nil, err
		}
		index++
	}
	// plaita: hitting the cap is a warning, never an error; only a loop that
	// actually ran rounds up to the cap reports it (max_iterations=0 runs zero
	// rounds and stays silent).
	if index >= maxIterations && index > 0 {
		log.Printf("while 节点 %s 达到 max_iterations=%d 上限，已强制停止", w.ID, maxIterations)
	}

	return collectionExecuteResult(ctx, &w.BaseNode, result)
}
