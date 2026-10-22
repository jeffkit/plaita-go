package nodes

// #80 phase 1：map/filter/find/reduce/while 五个集合/循环节点的共享件。
// 权威语义 = plaita/node/loop.py 的 BaseCollectionNode 家族：循环体是独立子
// 流程（$INPUT 只含注入的 item/index 或 first/second，另有 $PARENT/$GLOBAL），
// 节点内部零 try/except——子流程错误直接冒泡交给引擎按 errorHandler 分流。

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// runCollectionChild executes one collection-node iteration: the child flow
// sees exactly the injected value as $INPUT (plaita run_compatible kwargs →
// {item, index} / {first, second} / positional array) and the calling flow as
// $PARENT. Mirrors loop.go's child seeding; input validation is skipped the
// same way (loop injects synthetic shapes the child's inputType doesn't name).
func runCollectionChild(ctx context.Context, childFlow *workflow.Workflow, input any) (any, error) {
	childCtx := runtime.WithWorkflowRuntimeContext(ctx, &runtime.Context{
		WorkflowInput:         input,
		ParentWorkflowContext: ctx,
	})
	return childFlow.Execute(childCtx, input)
}

// collectionExecuteResult is the shared output tail: without an explicit
// `output` expression the node result is the computed collection value
// ($NODE.<id> downstream), otherwise the expression is evaluated and
// validated against outputType — same as loop.go.
func collectionExecuteResult(ctx context.Context, base *BaseNode, result any) (*workflow.ExecuteResult, error) {
	if base.Output == nil {
		return &workflow.ExecuteResult{Result: result, NextNode: base.NextNode()}, nil
	}
	out, err := expression.Eval(ctx, base.Output)
	if err != nil {
		return nil, err
	}
	if err := base.OutputType.ValidateValue(out); err != nil {
		return nil, err
	}
	return &workflow.ExecuteResult{Result: out, NextNode: base.NextNode()}, nil
}

// checkCollectionItem mirrors loop.go's item_type validation: only enforced
// when the collection is a literal array AND an itemType schema is declared
// (the compiler always emits collection as an expression, so this is a
// hand-written-flow guard, uniform across the collection family).
func checkCollectionItem(c loopCollection, itemType workflow.Property, nodeType string, item any) error {
	if c.IsArray() && itemType.ValidateValue(item) != nil {
		return fmt.Errorf("invalid %s item value: %v", nodeType, item)
	}
	return nil
}

// unmarshalCollectionField decodes the polymorphic `collection` field:
// expression string, literal array, or absent/null (empty collection —
// plaita evaluates None → _coerce_collection → []).
func unmarshalCollectionField(raw json.RawMessage) (loopCollection, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return loopCollection{}, nil
	}
	var c any
	if err := json.Unmarshal(raw, &c); err != nil {
		return loopCollection{}, err
	}
	var lc loopCollection
	switch t := c.(type) {
	case []any:
		lc.array = t
	case string:
		lc.expr = t
	default:
		return loopCollection{}, fmt.Errorf("collection must be an expression string or an array, got %T", c)
	}
	return lc, nil
}
