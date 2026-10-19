package nodes

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cast"

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

	return cast.ToSliceE(v)
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
		}), input)
		if execErr != nil {
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
