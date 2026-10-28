package nodes

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		nodeTypeReduce,
		func(ctx context.Context) workflow.Node { return new(Reduce) },
	)
}

const nodeTypeReduce workflow.NodeType = "reduce"

// Reduce 转正 plaita 的 reduce 归纳节点（plaita/node/loop.py Reduce，#80
// phase 1）。accumulator 链：initial 缺省 → 首元素作初值、从第二个元素开始；
// 空集合且无 initial → Python collection[0] 的 IndexError（确定性错误，不重
// 试）；空集合但有 initial → 返回 initial。子流程输入两形态（plaita
// _child_is_array_input）：子流程 inputType.dataType == "array" → 位置参数
// [acc, item]（$INPUT[0]/$INPUT[1]，@flow 编译器固定产此形态）；否则 object
// {first, second}（$INPUT.first/$INPUT.second）。不注入 index。
type Reduce struct {
	Child
	Collection loopCollection    `json:"collection"`
	Initial    any               `json:"initial,omitempty"`
	ItemType   workflow.Property `json:"itemType,omitempty"`
}

// GetType returns the reduce node type.
func (r *Reduce) GetType() workflow.NodeType {
	return nodeTypeReduce
}

// UnmarshalJSON unmarshals json to the reduce node (collection is polymorphic).
func (r *Reduce) UnmarshalJSON(data []byte) error {
	t := &struct {
		Child
		CollectionRaw json.RawMessage   `json:"collection"`
		Initial       any               `json:"initial"`
		ItemType      workflow.Property `json:"itemType,omitempty"`
	}{}
	if err := json.Unmarshal(data, t); err != nil {
		return err
	}
	lc, err := unmarshalCollectionField(t.CollectionRaw)
	if err != nil {
		return err
	}
	r.Child = t.Child
	r.Collection = lc
	r.Initial = t.Initial
	r.ItemType = t.ItemType
	return nil
}

// Execute executes the reduce node.
func (r *Reduce) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	collection, err := collectionElements(ctx, "reduce", r.Collection)
	if err != nil {
		return nil, fmt.Errorf("load reduce collection elements error: %v", err)
	}

	items := collection
	var acc any
	if r.Initial != nil {
		// plaita: initial 表达式在父侧求值；显式求值为 None 时 items 仍为全集
		//（与 self.initial is not None 的 Python 判据一致——JSON null 视同缺省）。
		if acc, err = expression.Eval(ctx, r.Initial); err != nil {
			// %w keeps the wrap chain intact for the deterministic bridge (#56);
			// rendered text identical to the historical %v form.
			return nil, fmt.Errorf("eval reduce initial error: %w", err)
		}
	} else {
		if len(collection) == 0 {
			// plaita: collection[0] on an empty list raises IndexError; the
			// error is deterministic (shape of the flow, not transient state).
			return nil, workflow.NonRetryable(fmt.Errorf(
				"reduce on empty collection without initial: IndexError: list index out of range"))
		}
		acc = collection[0]
		items = collection[1:]
	}

	arrayInput := r.ChildFlow != nil && r.ChildFlow.InputType.DataType == workflow.Array
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := checkCollectionItem(r.Collection, r.ItemType, "reduce", item); err != nil {
			return nil, err
		}
		var input any = map[string]any{"first": acc, "second": item}
		if arrayInput {
			input = []any{acc, item}
		}
		if acc, err = runCollectionChild(ctx, r.ChildFlow, input, string(r.GetID()), nil); err != nil {
			return nil, err
		}
	}

	return collectionExecuteResult(ctx, &r.BaseNode, acc)
}
