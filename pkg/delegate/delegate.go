// Package delegate define a delegate way to call http request
package delegate

import (
	"encoding/json"
	"net/http"
	"sync"
)

// HttpDelegator define a http delegator
type HttpDelegator interface {
	Do(request *http.Request, params json.RawMessage) (*http.Response, error)
}

var (
	mu         sync.RWMutex
	delegators = make(map[string]HttpDelegator)
)

// RegisterHTTPDelegator registers a http delegator
func RegisterHTTPDelegator(name string, delegator HttpDelegator) {
	mu.Lock()
	defer mu.Unlock()
	delegators[name] = delegator
}

// GetHTTPDelegator returns a delegator for a given name
func GetHTTPDelegator(name string) HttpDelegator {
	mu.RLock()
	defer mu.RUnlock()
	return delegators[name]
}
