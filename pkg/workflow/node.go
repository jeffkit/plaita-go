package workflow

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/duration"
)

// NodeType represents type for workflow node
type NodeType = types.NodeType

// NodeID node id
type NodeID = types.NodeID

// Node workflow logic node interface
type Node interface {
	GetID() NodeID
	GetType() NodeType
	GetName() string
	GetTimeout() duration.Duration
	GetErrorHandlePolicy() RecoverableErrorHandlePolicy
	GetTimeoutHandlePolicy() TimeoutHandlePolicy
	NextNode() NodeID
	Execute(ctx context.Context) (*ExecuteResult, error)
	GetMetadata() map[string]string
	MoreInfoAfterExecute(ctx context.Context) map[string]interface{}
}

// ExecuteResult 节点处理结果
type ExecuteResult struct {
	Result   any
	NextNode NodeID
}

// childOutputTyper is satisfied by nodes exposing their declared output
// schema (nodes.BaseNode.GetOutputType) — the nested-suspend return step
// validates the child's value against it (rs validate_child_return_output
// 同形). Duck-typed so engine-level node implementations without a schema
// keep working.
type childOutputTyper interface {
	GetOutputType() Property
}

// Nodes represents slice of Node
type Nodes []Node

// UnmarshalJSON unmarshal nodes from json.
func (ns *Nodes) UnmarshalJSON(data []byte) error {
	var rawNodes []json.RawMessage
	if err := json.Unmarshal(data, &rawNodes); err != nil {
		return err
	}

	for _, rawMessage := range rawNodes {
		nodeType := NodeType(gjson.GetBytes(rawMessage, "type").String())
		factory := GetNodeFactory(nodeType)
		if factory == nil {
			return fmt.Errorf("node type %q not registered", nodeType)
		}

		node := factory(context.Background())
		if err := json.Unmarshal(rawMessage, node); err != nil {
			return err
		}
		*ns = append(*ns, node)
	}
	return nil
}

// MarshalJSON marshal nodes to json.
func (ns *Nodes) MarshalJSON() ([]byte, error) {
	return json.Marshal(ns)
}

func isEndNode(node Node) bool {
	return node.GetType() == types.NodeTypeEnd
}
