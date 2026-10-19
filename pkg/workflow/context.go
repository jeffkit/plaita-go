package workflow

import "sync"

// Context represents the context config of workflow execution.
type Context struct {
	Global map[string]any `json:"global"`
	mu     sync.RWMutex
}

const (
	envConfKey = "__ENVCONF__"
	envNameKey = "__ENVNAME__"
)

// ParseGlobal parse global variables
func (c *Context) ParseGlobal() map[string]any {
	envConfValue, ok1 := c.GetGlobalVariable(envConfKey)
	envNameValue, ok2 := c.GetGlobalVariable(envNameKey)
	if !ok1 || !ok2 {
		return c.GetGlobalVariables()
	}

	envConf, ok1 := envConfValue.(map[string]any)
	envName, ok2 := envNameValue.(string)
	if !ok1 || !ok2 {
		return c.GetGlobalVariables()
	}

	targetConf, ok := envConf[envName]
	if !ok {
		return c.GetGlobalVariables()
	}
	confValues, ok := targetConf.(map[string]any)
	if !ok {
		return c.GetGlobalVariables()
	}

	result := make(map[string]any)
	// set global variable default value
	for k, v := range c.GetGlobalVariables() {
		if k == envConfKey || k == envNameKey {
			continue
		}
		result[k] = v
	}
	// override by env conf values
	for k, v := range confValues {
		result[k] = v
	}
	return result
}

// SetEnvName set env name
func (c *Context) SetEnvName(envName string) {
	c.SetGlobalVariable(envNameKey, envName)
}

// SetGlobalVariable set global variable
func (c *Context) SetGlobalVariable(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Global == nil {
		c.Global = make(map[string]any)
	}
	c.Global[key] = value
}

// SetGlobalVariables set global variables
func (c *Context) SetGlobalVariables(variables map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Global == nil {
		c.Global = make(map[string]any)
	}
	// merge global variables
	for k, v := range variables {
		c.Global[k] = v
	}
}

// GetGlobalVariables get global variables
func (c *Context) GetGlobalVariables() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]any)
	for k, v := range c.Global {
		result[k] = v
	}
	return result
}

// GetGlobalVariable get global variable
func (c *Context) GetGlobalVariable(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.Global[key]
	return value, ok
}
