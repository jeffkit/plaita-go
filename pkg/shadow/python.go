package shadow

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

//go:embed pybridge.py
var pythonBridgeSource []byte

// DefaultPythonRepo is where the reference plaita implementation lives on the
// shadow workstation. Callers should override it with PLAITA_PY_REPO or the
// -python-repo flag.
const DefaultPythonRepo = "/Users/kong/projects/infra4agent/plaita"

// PythonRunner executes flows in the reference Python engine through a
// subprocess sidecar.
type PythonRunner struct {
	// Command is the interpreter (default "python3").
	Command string
	// RepoDir is the plaita repository root containing the plaita package.
	RepoDir string
	// Timeout bounds one flow execution (default 120s).
	Timeout time.Duration
}

// NewPythonRunner returns a runner with defaults applied.
func NewPythonRunner(command, repoDir string) *PythonRunner {
	if command == "" {
		command = "python3"
	}
	if repoDir == "" {
		repoDir = DefaultPythonRepo
	}
	return &PythonRunner{Command: command, RepoDir: repoDir, Timeout: 120 * time.Second}
}

var (
	bridgeFileOnce sync.Once
	bridgeFilePath string
	bridgeFileErr  error
)

func ensureBridgeFile() (string, error) {
	bridgeFileOnce.Do(func() {
		dir, err := os.MkdirTemp("", "plaita-shadow-bridge-")
		if err != nil {
			bridgeFileErr = err
			return
		}
		path := filepath.Join(dir, "pybridge.py")
		if err := os.WriteFile(path, pythonBridgeSource, 0o600); err != nil {
			bridgeFileErr = err
			return
		}
		bridgeFilePath = path
	})
	return bridgeFilePath, bridgeFileErr
}

type pythonRequest struct {
	Flow          json.RawMessage `json:"flow"`
	Input         any             `json:"input"`
	Driver        string          `json:"driver,omitempty"`
	DriverOptions map[string]any  `json:"driverOptions,omitempty"`
}

// Run executes one flow in the Python engine and returns its outcome.
func (r *PythonRunner) Run(ctx context.Context, flowJSON []byte, input any) *Outcome {
	return r.RunDrive(ctx, flowJSON, input, "", nil)
}

// RunDrive executes one flow with the given driver ("" / "fresh" = single
// execute; see pybridge.py for the resume drivers).
func (r *PythonRunner) RunDrive(ctx context.Context, flowJSON []byte, input any,
	driver string, driverOptions map[string]any,
) *Outcome {
	out := &Outcome{Engine: "python"}
	bridgePath, err := ensureBridgeFile()
	if err != nil {
		return failedPython(out, fmt.Errorf("prepare python bridge: %w", err))
	}

	payload, err := json.Marshal(pythonRequest{
		Flow:          flowJSON,
		Input:         input,
		Driver:        driver,
		DriverOptions: driverOptions,
	})
	if err != nil {
		return failedPython(out, fmt.Errorf("encode python request: %w", err))
	}

	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, r.Command, bridgePath)
	cmd.Dir = r.RepoDir
	cmd.Env = pythonEnv(r.RepoDir)
	cmd.Stdin = bytes.NewReader(payload)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()

	decoded, decodeErr := decodePythonResponse(stdout.Bytes())
	if decodeErr != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = strings.TrimSpace(stdout.String())
		}
		if runErr != nil {
			return failedPython(out, fmt.Errorf("python bridge failed: %v: %s", runErr, tail(detail, 2000)))
		}
		return failedPython(out, fmt.Errorf("decode python response: %v: %s", decodeErr, tail(detail, 2000)))
	}
	decoded.Engine = "python"
	return decoded
}

// decodePythonResponse parses the sidecar JSON, tolerating a non-zero exit
// status (the sidecar returns 1 when the flow itself failed and still writes
// the structured response).
func decodePythonResponse(raw []byte) (*Outcome, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errors.New("empty stdout")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	out := new(Outcome)
	if err := dec.Decode(out); err != nil {
		return nil, err
	}
	return out, nil
}

func failedPython(out *Outcome, err error) *Outcome {
	out.OK = false
	out.Result = nil
	out.Error = err.Error()
	out.ErrorType = "python_bridge"
	return out
}

func pythonEnv(repoDir string) []string {
	env := os.Environ()
	env = append(env, "PYTHONIOENCODING=utf-8", "PYTHONDONTWRITEBYTECODE=1")
	if repoDir != "" {
		path := repoDir
		if existing := os.Getenv("PYTHONPATH"); existing != "" {
			path += string(os.PathListSeparator) + existing
		}
		env = append(env, "PYTHONPATH="+path)
	}
	return env
}

func tail(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return "..." + s[len(s)-limit:]
}
