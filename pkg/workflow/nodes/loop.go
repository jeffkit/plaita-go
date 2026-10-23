package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeLoop,
		func(ctx context.Context) workflow.Node { return new(Loop) },
	)
}

const (
	loopItemKey   = "LOOP-ITEM"
	loopIndexKey  = "LOOP-INDEX"
	loopResultKey = "LOOP-RESULT"
)

// Loop node iterates over a collection and executes its child flow for each item.
type Loop struct {
	Child
	Collection loopCollection    `json:"collection"`
	Condition  Condition         `json:"condition"`
	ItemType   workflow.Property `json:"itemType,omitempty"`
}

type loopCollection struct {
	expr  string
	array []any
}

// Elements returns the elements of the collection.
func (c loopCollection) Elements(ctx context.Context) ([]any, error) {
	if len(c.array) > 0 {
		return c.array, nil
	}

	if len(c.expr) == 0 {
		return nil, nil
	}

	v, err := expression.Eval(ctx, c.expr)
	if err != nil {
		return nil, err
	}

	return coerceCollectionLabel(v, "loop")
}

// collectionElements is the shared entry for the map/filter/find/reduce
// family (#80 phase 1): same coercion, error/warning labels carry the owning
// node type instead of a hardcoded "loop".
func collectionElements(ctx context.Context, nodeType string, c loopCollection) ([]any, error) {
	if len(c.array) > 0 {
		return c.array, nil
	}
	if len(c.expr) == 0 {
		return nil, nil
	}
	v, err := expression.Eval(ctx, c.expr)
	if err != nil {
		return nil, err
	}
	return coerceCollectionLabel(v, nodeType)
}

// coerceLoopCollection ports plaita loop._coerce_collection（issue #27）with
// the loop label; the map/filter/find/reduce family goes through
// collectionElements (#80 phase 1).
//
// coerceCollectionLabel 的归一规则（五节点共用）：
//   - 字符串先按 JSON 数组字面量解析（"[1,2,3]" → 3 个元素）；
//   - 不是 JSON 数组的普通字符串按单元素集合处理（不逐字符迭代），打 warning；
//   - nil → 空集合；slice/array → 逐元素；map → Python list(dict) 语义取键
//     （Go map 无序，与 Python dict 插入序的分歧沿用 loop 既有行为）；
//   - 其余不可迭代标量对齐 Python 的 TypeError，返回错误。
func coerceLoopCollection(v any) ([]any, error) {
	return coerceCollectionLabel(v, "loop")
}

func coerceCollectionLabel(v any, label string) ([]any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		stripped := strings.TrimSpace(t)
		if strings.HasPrefix(stripped, "[") {
			var parsed any
			if err := json.Unmarshal([]byte(stripped), &parsed); err == nil {
				if arr, ok := parsed.([]any); ok {
					return arr, nil
				}
			}
		}
		logged := t
		if len(logged) >= 80 {
			logged = logged[:77] + "..."
		}
		log.Printf("%s collection 表达式求值为普通字符串，按单元素集合处理（不逐字符迭代）: %q", label, logged)
		return []any{t}, nil
	case []any:
		return t, nil
	case map[string]any:
		keys := make([]any, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		return keys, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = rv.Index(i).Interface()
		}
		return out, nil
	case reflect.Map:
		keys := make([]any, 0, rv.Len())
		for _, key := range rv.MapKeys() {
			keys = append(keys, key.Interface())
		}
		return keys, nil
	default:
		return nil, fmt.Errorf("%s collection: %T is not iterable", label, v)
	}
}

// IsArray returns true if the collection is an array.
func (c loopCollection) IsArray() bool {
	return len(c.array) > 0
}

// UnmarshalJSON unmarshal json to loop node.
func (l *Loop) UnmarshalJSON(data []byte) error {
	t := &struct {
		Child
		CollectionRaw json.RawMessage   `json:"collection"`
		Condition     Condition         `json:"condition"`
		ItemType      workflow.Property `json:"itemType,omitempty"`
	}{}
	if err := json.Unmarshal(data, t); err != nil {
		return err
	}

	var c any
	if err := json.Unmarshal(t.CollectionRaw, &c); err != nil {
		return err
	}

	var lc loopCollection
	switch t := c.(type) {
	case []any:
		lc.array = t
	case string:
		lc.expr = t
	}

	l.Child = t.Child
	l.Collection = lc
	l.Condition = t.Condition
	l.ItemType = t.ItemType
	return nil
}

// GetType returns the loop node type.
func (l *Loop) GetType() workflow.NodeType {
	return types.NodeTypeLoop
}

// Execute executes the loop node.
func (l *Loop) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	collection, err := l.Collection.Elements(ctx)
	if err != nil {
		return nil, fmt.Errorf("load loop collection elements error: %v", err)
	}

	// Loop through the collection
	var lastResult any
	for index, item := range collection {
		if l.Collection.IsArray() && l.ItemType.ValidateValue(item) != nil {
			return nil, fmt.Errorf("invalid loop item value: %v", item)
		}

		// Set the loop item and index in the current node context
		runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
		runtimeCtx.WithCurrentNodeContext(loopIndexKey, index)
		runtimeCtx.WithCurrentNodeContext(loopItemKey, item)

		// Run the child flow with the current item and index
		input := map[string]any{"item": item, "index": index}
		childResult, execErr := l.ChildFlow.Execute(runtime.WithWorkflowRuntimeContext(ctx, &runtime.Context{
			WorkflowInput:         input,
			ParentWorkflowContext: ctx,
			// 嵌套挂起 v1：迭代帧标注（runCollectionChild 同款）。
			FrameKind: runtime.FrameKindLoopIteration,
		}), input)
		if execErr != nil {
			// 迭代帧边界（rs run_collection_child 同款）：挂起载体无条件具名
			// 拒绝并注销订阅，不进普通错误链；闸拒绝原样透传。
			if susp := workflow.AsSuspend(execErr); susp != nil {
				return nil, workflow.RejectSuspendedCarrier(ctx, susp, runtime.FrameKindLoopIteration)
			}
			return nil, execErr
		}

		// Store the result of the child flow
		runtimeCtx.WithCurrentNodeContext(loopResultKey, childResult)
		lastResult = childResult
		// Check the loop condition
		matched, _, execErr := l.Condition.MatchContext(ctx)
		if execErr != nil {
			return nil, fmt.Errorf("loop condition error: %v", execErr)
		}
		if !matched {
			break
		}
	}

	if l.Output == nil {
		// plaita semantics: the loop node's output is the last child result
		// ($NODE.<loop>), nil when the collection was empty.
		return &workflow.ExecuteResult{Result: lastResult, NextNode: l.NextNode()}, nil
	}

	// Evaluate and validate the output
	output, err := expression.Eval(ctx, l.Output)
	if err != nil {
		return nil, err
	}

	if err := l.OutputType.ValidateValue(output); err != nil {
		return nil, err
	}

	return &workflow.ExecuteResult{Result: output, NextNode: l.NextNode()}, nil
}
