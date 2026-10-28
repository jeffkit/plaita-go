package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		nodeTypeMap,
		func(ctx context.Context) workflow.Node { return new(Map) },
	)
}

const nodeTypeMap workflow.NodeType = "map"

// Map 转正 plaita 的 map 集合节点（plaita/node/loop.py Map，#80 phase 1）。
// 对集合每个元素执行子流程并按集合顺序返回全部结果 list；空集合 → []；
// 子流程输入 {item, index}；错误直接冒泡（已产出的部分结果丢弃，同 Python）。
// concurrent=true（历史别名 async，IR 层折叠）并发执行，maxConcurrent gate
// （Python `or None` 语义：缺省/0 = 不设限）。
type Map struct {
	Child
	Collection    loopCollection    `json:"collection"`
	ItemType      workflow.Property `json:"itemType,omitempty"`
	Concurrent    bool              `json:"concurrent,omitempty"`
	MaxConcurrent *int              `json:"maxConcurrent,omitempty"`
}

// GetType returns the map node type.
func (m *Map) GetType() workflow.NodeType {
	return nodeTypeMap
}

// UnmarshalJSON unmarshals json to the map node (collection is polymorphic).
func (m *Map) UnmarshalJSON(data []byte) error {
	t := &struct {
		Child
		CollectionRaw json.RawMessage   `json:"collection"`
		ItemType      workflow.Property `json:"itemType,omitempty"`
		Concurrent    bool              `json:"concurrent"`
		MaxConcurrent *int              `json:"maxConcurrent"`
	}{}
	if err := json.Unmarshal(data, t); err != nil {
		return err
	}
	lc, err := unmarshalCollectionField(t.CollectionRaw)
	if err != nil {
		return err
	}
	m.Child = t.Child
	m.Collection = lc
	m.ItemType = t.ItemType
	m.Concurrent = t.Concurrent
	m.MaxConcurrent = t.MaxConcurrent
	return nil
}

// Execute executes the map node.
func (m *Map) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	collection, err := collectionElements(ctx, "map", m.Collection)
	if err != nil {
		return nil, fmt.Errorf("load map collection elements error: %v", err)
	}

	results := make([]any, len(collection))
	if m.Concurrent && len(collection) > 0 {
		if err := m.executeConcurrent(ctx, collection, results); err != nil {
			return nil, err
		}
	} else {
		for index, item := range collection {
			// fail fast between iterations (#48 ctx 反弹矩阵约定)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := checkCollectionItem(m.Collection, m.ItemType, "map", item); err != nil {
				return nil, err
			}
			res, execErr := runCollectionChild(ctx, m.ChildFlow, map[string]any{"item": item, "index": index}, string(m.GetID()), index)
			if execErr != nil {
				return nil, execErr
			}
			results[index] = res
		}
	}

	return collectionExecuteResult(ctx, &m.BaseNode, results)
}

// childFlowView returns a fresh Workflow struct per concurrent element:
// Workflow.running is an instance-level reentry guard, so launching the same
// childFlow pointer N times concurrently would trip ErrWorkflowReentry. Mirrors
// parallel.branchFlowView: field-by-field copy (atomic.Bool and the Context's
// RWMutex must not be copied), Nodes shared — node instances are read-only
// during execution, per-run state lives in runtime.Context.
//
// Timeout is copied verbatim (#81): the sequential path executes the original
// pointer, so its flow-level budget is f.Timeout alone — a parallel-style
// fallback to the map node's timeout here would make the concurrent path
// diverge from the sequential one in the opposite direction.
func childFlowView(f *workflow.Workflow) *workflow.Workflow {
	return &workflow.Workflow{
		ID:                  f.ID,
		Name:                f.Name,
		Desc:                f.Desc,
		Runtime:             f.Runtime,
		Version:             f.Version,
		Author:              f.Author,
		Timeout:             f.Timeout,
		Nodes:               f.Nodes,
		InputType:           f.InputType,
		OutputType:          f.OutputType,
		ErrorHandlePolicy:   f.ErrorHandlePolicy,
		TimeoutHandlePolicy: f.TimeoutHandlePolicy,
		Metadata:            f.Metadata,
		Context:             workflow.Context{Global: f.Context.GetGlobalVariables()},
		ExposeEnv:           f.ExposeEnv,
	}
}

// executeConcurrent 是 Python ThreadParallelExecutor 的 Go 对应物：每元素一个
// goroutine（CloneContext 隔离，同 parallel 分支先例），maxConcurrent 信号量
// gate。结果按集合顺序落位；错误取最小失败下标——Python ThreadPoolExecutor.map
// 按序产出结果，最早失败下标先冒泡。
func (m *Map) executeConcurrent(ctx context.Context, collection []any, results []any) error {
	var sem chan struct{}
	if m.MaxConcurrent != nil && *m.MaxConcurrent > 0 {
		sem = make(chan struct{}, *m.MaxConcurrent)
	}
	errs := make([]error, len(collection))
	var wg sync.WaitGroup
	for index, item := range collection {
		wg.Add(1)
		go func(i int, it any, flow *workflow.Workflow) {
			defer wg.Done()
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}
			if err := ctx.Err(); err != nil {
				errs[i] = err
				return
			}
			if err := checkCollectionItem(m.Collection, m.ItemType, "map", it); err != nil {
				errs[i] = err
				return
			}
			res, err := runCollectionChild(runtime.CloneContext(ctx), flow, map[string]any{"item": it, "index": i}, string(m.GetID()), i)
			if err != nil {
				errs[i] = err
				return
			}
			results[i] = res
		}(index, item, childFlowView(m.ChildFlow))
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
