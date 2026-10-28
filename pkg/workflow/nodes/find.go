package nodes

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		nodeTypeFind,
		func(ctx context.Context) workflow.Node { return new(Find) },
	)
}

const nodeTypeFind workflow.NodeType = "find"

// Find 转正 plaita 的 find 集合节点（plaita/node/loop.py Find，#80 phase 1）。
// 返回集合中第一个使子流程返回真值的**元素**（短路：找到即停，后续元素不再
// 执行）；未找到/空集合 → None（不是错误）。子流程输入 {item, index}。
type Find struct {
	Child
	Collection loopCollection    `json:"collection"`
	ItemType   workflow.Property `json:"itemType,omitempty"`
}

// GetType returns the find node type.
func (f *Find) GetType() workflow.NodeType {
	return nodeTypeFind
}

// UnmarshalJSON unmarshals json to the find node (collection is polymorphic).
func (f *Find) UnmarshalJSON(data []byte) error {
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

// Execute executes the find node.
func (f *Find) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	collection, err := collectionElements(ctx, "find", f.Collection)
	if err != nil {
		return nil, fmt.Errorf("load find collection elements error: %v", err)
	}

	for index, item := range collection {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := checkCollectionItem(f.Collection, f.ItemType, "find", item); err != nil {
			return nil, err
		}
		res, execErr := runCollectionChild(ctx, f.ChildFlow, map[string]any{"item": item, "index": index}, string(f.GetID()), index)
		if execErr != nil {
			return nil, execErr
		}
		if isTruthy(res) {
			// short-circuit: plaita returns the ELEMENT, not the child result.
			return collectionExecuteResult(ctx, &f.BaseNode, item)
		}
	}

	// not found: plaita returns None (a normal result, not an error).
	return collectionExecuteResult(ctx, &f.BaseNode, nil)
}
