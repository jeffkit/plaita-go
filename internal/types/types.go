// Package types defines engine types
package types

// NodeID is a unique identifier for a node in a workflow.
type NodeID string

// NodeType is the type of node.
type NodeType string

// builtin node types
const (
	NodeTypeStart      NodeType = "start"
	NodeTypeEnd        NodeType = "end"
	NodeTypeSleep      NodeType = "sleep"
	NodeTypeAssignment NodeType = "assignment"
	NodeTypeChild      NodeType = "child"
	NodeTypeLoop       NodeType = "loop"
	NodeTypeParallel   NodeType = "parallel"
	NodeTypeSwitch     NodeType = "switch"
	NodeTypeCase       NodeType = "case"
	NodeTypeMock       NodeType = "mock"
	NodeTypeReference  NodeType = "reference"
	NodeTypeHTTP       NodeType = "http"
	NodeTypeCode       NodeType = "code"
	NodeTypeRedis      NodeType = "redis"
	NodeTypeAgentRun   NodeType = "agentrun"
	NodeTypeMDB        NodeType = "mdb"
	NodeTypeReadMDB    NodeType = "readmdb"
	NodeTypeWriteMDB   NodeType = "writemdb"
)
