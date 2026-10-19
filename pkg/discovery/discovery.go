// Package discovery provides a way to discover address for the http node
package discovery

import (
	"context"
	"encoding/json"
	"net/url"
	"sync"
	"time"
)

// DefaultSelector is the noop selector
var DefaultSelector = &noopSelector{}

// AddressingName define http addressing name
type AddressingName string

// Addressing define http addressing method
type Addressing struct {
	Name   AddressingName  `json:"name,omitempty"`   // default: domain
	Params json.RawMessage `json:"params,omitempty"` // addressing target
}

// RegistryNode is the information of address node
type RegistryNode struct {
	URL      *url.URL       // store the url of Addr
	MetaData map[string]any // store the metadata of Addr
}

const (
	// DomainAddr represents the domain address
	DomainAddr AddressingName = "domain"
)

// Selector is an interface to select address for the http node
type Selector interface {
	// Addr returns schema and host(or host:port)
	Addr(ctx context.Context, url *url.URL, params json.RawMessage) (*RegistryNode, error)
	// Report reports the request status
	Report(node *RegistryNode, cost time.Duration, err error) error
}

var (
	mu        sync.RWMutex
	selectors = make(map[AddressingName]Selector)
)

// RegisterSelector registers a selector
func RegisterSelector(name AddressingName, selector Selector) {
	mu.Lock()
	defer mu.Unlock()
	selectors[name] = selector
}

// GetSelector returns a selector for a given name
func GetSelector(name AddressingName) Selector {
	mu.RLock()
	defer mu.RUnlock()
	return selectors[name]
}

type noopSelector struct {
}

// Addr noop selector always return original url
func (n *noopSelector) Addr(ctx context.Context, url *url.URL, params json.RawMessage) (*RegistryNode, error) {
	return &RegistryNode{URL: url}, nil
}

// Report noop selector do nothing
func (n *noopSelector) Report(node *RegistryNode, cost time.Duration, err error) error {
	return nil
}
