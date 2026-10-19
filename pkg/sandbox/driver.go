package sandbox

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ExecOptions describes one command inside a sandbox. Secret environment
// values do NOT travel in Env: they go through EnvFile, which the driver
// injects (0600) and the caller burns. Env carries non-secret knobs only.
type ExecOptions struct {
	// Cmd is argv, executed without an implicit shell unless the driver must
	// wrap it to source EnvFile.
	Cmd []string
	// Env is the small non-secret environment (exec API env).
	Env map[string]string
	// WorkDir is the container working directory ("" = image default).
	WorkDir string
	// EnvFile is a container path to source before exec (0600, caller-owned).
	EnvFile string
	// StdinFile is a container path redirected into the command's stdin.
	StdinFile string
	// Timeout bounds the whole exec (0 = caller's context only). On expiry
	// the driver kills the container and returns a context deadline error.
	Timeout time.Duration
}

// ExecResult is the assembled outcome of one exec.
type ExecResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

// ReleaseOptions controls cleanup. Successful turns stop+remove the
// container but keep the data layer (the working copy is the durable half).
// Failed turns additionally write a retention marker in the data directory so
// the scene survives for inspection.
type ReleaseOptions struct {
	KeepData      bool
	Failed        bool
	FailureReason string
}

// Driver is the sandbox execution contract. Docker (HTTP API) is the only
// implementation in this build; tests substitute a fake through the same
// interface.
type Driver interface {
	// Name is the registry key ("docker").
	Name() string
	// Ensure idempotently creates/attaches the workspace and provisions it.
	Ensure(ctx context.Context, spec *WorkspaceSpec, executionID, wsKey string) (*WorkspaceHandle, error)
	// Exec runs argv inside the workspace.
	Exec(ctx context.Context, handle *WorkspaceHandle, opts ExecOptions) (*ExecResult, error)
	// InjectFile writes a file into the container through a tar stream.
	InjectFile(ctx context.Context, handle *WorkspaceHandle, containerPath string, data []byte, mode int64) error
	// RemoveFile removes a file inside the container (best effort callers).
	RemoveFile(ctx context.Context, handle *WorkspaceHandle, containerPath string) error
	// Enforce is the final kill switch: it terminates the workspace's
	// processes/container after a timeout, cancellation or failed exec.
	Enforce(ctx context.Context, handle *WorkspaceHandle) error
	// Release stops and removes the compute layer; keep_data controls the
	// data layer.
	Release(ctx context.Context, handle *WorkspaceHandle, opts ReleaseOptions) error
}

// DriverFactory constructs a driver instance. Factories resolve their
// environment (socket, data root) at construction.
type DriverFactory func() Driver

var (
	driversMu sync.RWMutex
	drivers   = map[string]DriverFactory{}
)

// RegisterDriver adds a driver factory (init-time in practice).
func RegisterDriver(name string, factory DriverFactory) {
	driversMu.Lock()
	defer driversMu.Unlock()
	drivers[name] = factory
}

// GetDriver resolves a registered driver by name.
func GetDriver(name string) (Driver, error) {
	driversMu.RLock()
	factory, ok := drivers[name]
	driversMu.RUnlock()
	if !ok || factory == nil {
		return nil, fmt.Errorf("%w: %q", ErrDriverMissing, name)
	}
	return factory(), nil
}

// DriverNames lists registered drivers (error messages / diagnostics).
func DriverNames() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	names := make([]string, 0, len(drivers))
	for name := range drivers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
