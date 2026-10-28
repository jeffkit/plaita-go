package nodes

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		nodeTypeFilter,
		func(ctx context.Context) workflow.Node { return new(Filter) },
	)
}

const nodeTypeFilter workflow.NodeType = "filter"

// Filter 转正 plaita 的 filter 集合节点（plaita/node/loop.py Filter，#80
// phase 1）。对集合每个元素执行子流程，返回值为**原子元素的子集**：判定是裸
// Python 真值（`if result:`）——非 bool 返回值（""/0/[]/None/{}）按 falsy
// 剔除；空集合 → []；错误直接冒泡。子流程输入 {item, index}。
type Filter struct {
	Child
	Collection loopCollection    `json:"collection"`
	ItemType   workflow.Property `json:"itemType,omitempty"`
}

// GetType returns the filter node type.
func (f *Filter) GetType() workflow.NodeType {
	return nodeTypeFilter
}

// UnmarshalJSON unmarshals json to the filter node (collection is polymorphic).
func (f *Filter) UnmarshalJSON(data []byte) error {
	t := &struct {
		Child
		CollectionRaw json.RawMessage   `json:"collection"`
		ItemType      workflow.Property `json:"itemType,omitempty"`
	}{}
	if err := json.Unmarshal(data, t); err != nil {
		return err
	}
	lc, err := unmarshalCollectionField(t.CollectionRaw)
	if err != nil {
		return err
	}
	f.Child = t.Child
	f.Collection = lc
	f.ItemType = t.ItemType
	return nil
}

// Execute executes the filter node.
func (f *Filter) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	collection, err := collectionElements(ctx, "filter", f.Collection)
	if err != nil {
		return nil, fmt.Errorf("load filter collection elements error: %v", err)
	}

	results := make([]any, 0, len(collection))
	for index, item := range collection {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := checkCollectionItem(f.Collection, f.ItemType, "filter", item); err != nil {
			return nil, err
		}
		res, execErr := runCollectionChild(ctx, f.ChildFlow, map[string]any{"item": item, "index": index}, string(f.GetID()), index)
		if execErr != nil {
			return nil, execErr
		}
		// plaita semantics: bare Python truthiness on the child result.
		if isTruthy(res) {
			results = append(results, item)
		}
	}

	return collectionExecuteResult(ctx, &f.BaseNode, results)
}
