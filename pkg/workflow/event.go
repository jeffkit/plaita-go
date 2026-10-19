package workflow

import (
	"context"
	"sync"
)

type EventType string

const (
	FlowStart EventType = "flow_start"
	FlowEnd   EventType = "flow_end"
	NodeStart EventType = "node_start"
	NodeEnd   EventType = "node_end"
)

type FlowStartFunc func(ctx context.Context, flow *Workflow)
type FlowEndFunc func(ctx context.Context, flow *Workflow, result any, err error)
type NodeStartFunc func(ctx context.Context, flow *Workflow, node Node)
type NodeEndFunc func(ctx context.Context,
	flow *Workflow, node Node, result any, err error, args map[string]interface{})

const initListenerSize = 1 << 6

var (
	flowStartListeners = make([]FlowStartFunc, 0, initListenerSize)
	fsMux              sync.RWMutex
	flowEndListeners   = make([]FlowEndFunc, 0, initListenerSize)
	feMux              sync.RWMutex

	nodeStartListeners = make([]NodeStartFunc, 0, initListenerSize)
	nsMux              sync.RWMutex
	nodeEndListeners   = make([]NodeEndFunc, 0, initListenerSize)
	neMux              sync.RWMutex
)

// OnFlowStart register flow start listener
func OnFlowStart(listener FlowStartFunc) {
	fsMux.Lock()
	defer fsMux.Unlock()
	flowStartListeners = append(flowStartListeners, listener)
}

// OnFlowEnd register flow end listener
func OnFlowEnd(listener FlowEndFunc) {
	feMux.Lock()
	defer feMux.Unlock()
	flowEndListeners = append(flowEndListeners, listener)
}

// OnNodeStart register node start listener
func OnNodeStart(listener NodeStartFunc) {
	nsMux.Lock()
	defer nsMux.Unlock()
	nodeStartListeners = append(nodeStartListeners, listener)
}

// OnNodeEnd register node end listener
func OnNodeEnd(listener NodeEndFunc) {
	neMux.Lock()
	defer neMux.Unlock()
	nodeEndListeners = append(nodeEndListeners, listener)
}

func triggerFlowStart(ctx context.Context, flow *Workflow) {
	fsMux.RLock()
	defer fsMux.RUnlock()
	for _, listener := range flowStartListeners {
		listener(ctx, flow)
	}
}

func triggerFlowEnd(ctx context.Context, flow *Workflow, result any, err error) {
	feMux.RLock()
	defer feMux.RUnlock()
	for _, listener := range flowEndListeners {
		listener(ctx, flow, result, err)
	}
}

func triggerNodeStart(ctx context.Context, flow *Workflow, node Node) {
	nsMux.RLock()
	defer nsMux.RUnlock()
	for _, listener := range nodeStartListeners {
		listener(ctx, flow, node)
	}
}

func triggerNodeEnd(ctx context.Context,
	flow *Workflow, node Node, result any, err error, args map[string]interface{}) {
	neMux.RLock()
	defer neMux.RUnlock()
	for _, listener := range nodeEndListeners {
		listener(ctx, flow, node, result, err, args)
	}
}
