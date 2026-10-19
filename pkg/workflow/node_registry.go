package workflow

import (
	"context"
	"fmt"
	"sync"
)

// NodeFactory is a factory that creates a new node.
type NodeFactory func(ctx context.Context) Node

// NodeRegistry is used to register node types and unmarshal nodes from JSON.
type NodeRegistry struct {
	mutex sync.Mutex
	nodes map[NodeType]NodeFactory
}

// NewRegistry creates a new NodeRegistry.
func NewRegistry() *NodeRegistry {
	return &NodeRegistry{
		nodes: make(map[NodeType]NodeFactory),
	}
}

// Register registers a node type.
func (r *NodeRegistry) Register(nodeType NodeType, nodeFactory NodeFactory) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, ok := r.nodes[nodeType]; ok {
		panic(fmt.Errorf("node type already registered: %s", nodeType))
	}
	r.nodes[nodeType] = nodeFactory
}

// GetNodeFactory returns a node factory for a given node type.
func (r *NodeRegistry) GetNodeFactory(nodeType NodeType) NodeFactory {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.nodes[nodeType]
}

// GlobalNodeRegistry is the global node registry.
var GlobalNodeRegistry = NewRegistry()

// RegisterNodeType registers a node type.
func RegisterNodeType(nodeType NodeType, nodeFactory NodeFactory) {
	GlobalNodeRegistry.Register(nodeType, nodeFactory)
}

// GetNodeFactory returns a node factory for a given node type.
func GetNodeFactory(nodeType NodeType) NodeFactory {
	return GlobalNodeRegistry.GetNodeFactory(nodeType)
}
