// Package sandbox implements the execution-side sandbox abstraction for the
// Go engine. It mirrors plaita-nodes' sandbox.py + sandbox_docker.py contract
// (docs/sandbox-drivers-design.md v2.1) with one deliberate difference: the
// Docker driver talks the Docker Engine HTTP API over a Unix socket instead
// of shelling out to the docker CLI, so the runtime needs no new dependency.
//
// Layering:
//
//   - .plaita/sandboxes.json is the infra registry (definition authority).
//     Flows only reference a workspace name; an unregistered name is a
//     fail-closed error and is never created on the fly.
//   - WorkspaceHandle ids derive deterministically from
//     (execution_id, ws_key), so a retried execution re-attaches instead of
//     cloning a second working copy.
//   - A workspace-level lease serializes execs against one handle; the
//     execution lease only renews between nodes and is far too short to
//     cover a long agent turn (design §6.5).
//   - Credentials for the sandbox travel only through a 0600 envfile that is
//     injected into the container and burned afterwards; injected values are
//     also canary-redacted from everything returned to the caller.
package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"sigs.k8s.io/yaml"
)

// Default resource naming follows the Python driver so operator tooling sees
// one family of names. Shadow deployments can isolate themselves with
// PLAITA_SANDBOX_PREFIX (e.g. plaita-go-shadow-ws) when they share a Docker
// daemon with the Python runtime.
const (
	DefaultNamePrefix = "plaita-ws"
	// SandboxPath is the workspace path inside the container; it is stable
	// across rebuilds (agent session/cwd semantics depend on it).
	SandboxPath = "/work"
)

// Error classes. Callers distinguish configuration mistakes (never retried)
// from transient infrastructure failures.
var (
	ErrConfig        = errors.New("sandbox config error")
	ErrPin           = errors.New("sandbox image not pinned")
	ErrNotRegistered = errors.New("sandbox workspace not registered")
	ErrDriverMissing = errors.New("sandbox driver not registered")
	ErrLeaseHeld     = errors.New("sandbox workspace lease held")
	ErrImageMissing  = errors.New("sandbox image missing")
)

// WorkspaceSpec is one entry of the infra registry.
type WorkspaceSpec struct {
	Name          string
	Driver        string
	Image         string
	Template      string
	Provision     map[string]any
	Resources     map[string]any
	Env           map[string]any
	Egress        string // bridge | none
	AllowUnpinned bool
}

// GitRepo returns provision.git.repo ("" when unset).
func (s *WorkspaceSpec) GitRepo() string {
	git, _ := s.Provision["git"].(map[string]any)
	if git == nil {
		return ""
	}
	repo, _ := git["repo"].(string)
	return repo
}

// GitBranch returns provision.git.branch ("" when unset).
func (s *WorkspaceSpec) GitBranch() string {
	git, _ := s.Provision["git"].(map[string]any)
	if git == nil {
		return ""
	}
	branch, _ := git["branch"].(string)
	return branch
}

// WorkspaceHandle is the product of Ensure. ID/path are stable for the
// lifetime of an execution; Data is driver-private.
type WorkspaceHandle struct {
	Driver      string
	ID          string
	Path        string
	Key         string
	ExecutionID string
	Data        map[string]string
}

// Snapshot is the small, checkpoint-safe observation attached to the node
// output (design §4/§6.7): env names only, never values.
func (h *WorkspaceHandle) Snapshot(envNames []string) map[string]any {
	names := append([]string(nil), envNames...)
	sort.Strings(names)
	return map[string]any{
		"ws_key":    h.Key,
		"driver":    h.Driver,
		"id":        h.ID,
		"path":      h.Path,
		"env_names": names,
	}
}

// DefaultHome is the plaita config home: PLAITA_HOME wins, else ~/.plaita.
// The same variable roots agentproc's agents.json/providers.json; the shadow
// deployment overrides it as a whole.
func DefaultHome() string {
	if home := os.Getenv("PLAITA_HOME"); home != "" {
		return home
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".plaita"
	}
	return filepath.Join(home, ".plaita")
}

var dockerNameRe = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

// SanitizeWSKey maps a workspace key onto Docker's name alphabet.
func SanitizeWSKey(wsKey string) string {
	cleaned := strings.Trim(dockerNameRe.ReplaceAllString(wsKey, "-"), "-.")
	if cleaned == "" {
		return "default"
	}
	return cleaned
}

// HandleID is the stable workspace identity: {execution_id}:{ws_key}.
func HandleID(executionID, wsKey string) string {
	return executionID + ":" + wsKey
}

// NamePrefix is the container/data resource prefix. PLAITA_SANDBOX_PREFIX
// overrides it; otherwise a deployment that already isolates itself with
// PLAITA_KEY_PREFIX (the shadow worker) gets a matching resource prefix so it
// cannot collide with production Python sandboxes on a shared daemon.
func NamePrefix() string {
	if prefix := strings.TrimSpace(os.Getenv("PLAITA_SANDBOX_PREFIX")); prefix != "" {
		return prefix
	}
	if prefix := strings.TrimSpace(os.Getenv("PLAITA_KEY_PREFIX")); prefix != "" {
		return strings.Trim(SanitizeWSKey(prefix), "-.") + "-ws"
	}
	return DefaultNamePrefix
}

// ResourceName derives a deterministic docker resource name (container or
// data directory), capped to Docker's 255-char name limit.
func ResourceName(executionID, wsKey, kind string) string {
	suffix := SanitizeWSKey(executionID + "-" + wsKey)
	if len(suffix) > 180 {
		suffix = suffix[:180]
	}
	name := NamePrefix() + "-" + kind + "-" + suffix
	if len(name) > 220 {
		name = name[:220]
	}
	return name
}

var envInterpolationRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// LoadSandboxes loads the infra registry. Search order is
// PLAITA_HOME (~/.plaita) then <repo>/.plaita; later files deep-merge over
// earlier ones (per-workspace fields merge, scalars replace). ${VAR}
// interpolation is fail-fast: a missing variable is a configuration error,
// never an empty string.
func LoadSandboxes(repo string) (map[string]*WorkspaceSpec, error) {
	dirs := []string{DefaultHome()}
	if repo != "" {
		repoDir := filepath.Join(repo, ".plaita")
		if repoDir != dirs[0] {
			dirs = append(dirs, repoDir)
		}
	}

	merged := map[string]any{}
	for _, dir := range dirs {
		doc, found, err := readConfigDir(dir)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		merged = deepMerge(merged, doc)
	}
	raw := merged
	if child, ok := merged["sandboxes"].(map[string]any); ok {
		raw = child
	} else if _, present := merged["sandboxes"]; present {
		return nil, fmt.Errorf("%w: sandboxes must be an object", ErrConfig)
	}

	interpolated, err := interpolateValue(raw)
	if err != nil {
		return nil, err
	}
	raw, _ = interpolated.(map[string]any)

	out := make(map[string]*WorkspaceSpec, len(raw))
	for name, body := range raw {
		spec, err := parseSpec(name, body)
		if err != nil {
			return nil, err
		}
		out[name] = spec
	}
	return out, nil
}

func readConfigDir(dir string) (map[string]any, bool, error) {
	for _, suffix := range []string{".json", ".yaml", ".yml"} {
		path := filepath.Join(dir, "sandboxes"+suffix)
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, false, fmt.Errorf("%w: read %s: %v", ErrConfig, path, err)
		}
		var doc map[string]any
		if suffix == ".json" {
			if err := json.Unmarshal(data, &doc); err != nil {
				return nil, false, fmt.Errorf("%w: %s: %v", ErrConfig, path, err)
			}
		} else {
			jsonData, err := yaml.YAMLToJSON(data)
			if err != nil {
				return nil, false, fmt.Errorf("%w: %s: %v", ErrConfig, path, err)
			}
			if err := json.Unmarshal(jsonData, &doc); err != nil {
				return nil, false, fmt.Errorf("%w: %s: %v", ErrConfig, path, err)
			}
		}
		if doc == nil {
			doc = map[string]any{}
		}
		return doc, true, nil
	}
	return nil, false, nil
}

func parseSpec(name string, body any) (*WorkspaceSpec, error) {
	obj, ok := body.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: sandbox %q must be an object", ErrConfig, name)
	}
	driver := stringField(obj, "driver")
	if driver == "" {
		driver = "docker"
	}
	image := stringField(obj, "image")
	template := stringField(obj, "template")
	if image == "" && template == "" && driver != "ssh" {
		return nil, fmt.Errorf("%w: sandbox %q needs image/template", ErrConfig, name)
	}
	provision := map[string]any{}
	if rawProvision, present := obj["provision"]; present && rawProvision != nil {
		parsed, ok := rawProvision.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: sandbox %q provision must be an object", ErrConfig, name)
		}
		provision = parsed
	}
	if rawGit, present := provision["git"]; present {
		git, ok := rawGit.(map[string]any)
		if !ok || stringField(git, "repo") == "" {
			return nil, fmt.Errorf("%w: sandbox %q provision.git needs repo", ErrConfig, name)
		}
	}
	env, err := optionalObject(obj, "env", name)
	if err != nil {
		return nil, err
	}
	resources, err := optionalObject(obj, "resources", name)
	if err != nil {
		return nil, err
	}
	egress := stringField(obj, "egress")
	if egress == "" {
		egress = "bridge"
	}
	allowUnpinned, _ := obj["allow_unpinned"].(bool)

	return &WorkspaceSpec{
		Name:          name,
		Driver:        driver,
		Image:         image,
		Template:      template,
		Provision:     provision,
		Resources:     resources,
		Env:           env,
		Egress:        egress,
		AllowUnpinned: allowUnpinned,
	}, nil
}

func optionalObject(obj map[string]any, key, name string) (map[string]any, error) {
	raw, present := obj[key]
	if !present || raw == nil {
		return map[string]any{}, nil
	}
	child, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: sandbox %q %s must be an object", ErrConfig, name, key)
	}
	return child, nil
}

func stringField(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return strings.TrimSpace(v)
}

func deepMerge(base, override map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		if child, ok := v.(map[string]any); ok {
			if prev, ok := out[k].(map[string]any); ok {
				out[k] = deepMerge(prev, child)
				continue
			}
		}
		out[k] = v
	}
	return out
}

func interpolateValue(value any) (any, error) {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, child := range v {
			next, err := interpolateValue(child)
			if err != nil {
				return nil, err
			}
			out[key] = next
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, child := range v {
			next, err := interpolateValue(child)
			if err != nil {
				return nil, err
			}
			out[i] = next
		}
		return out, nil
	case string:
		var missing string
		replaced := envInterpolationRe.ReplaceAllStringFunc(v, func(match string) string {
			name := envInterpolationRe.FindStringSubmatch(match)[1]
			val, ok := os.LookupEnv(name)
			if !ok {
				missing = name
				return match
			}
			return val
		})
		if missing != "" {
			return nil, fmt.Errorf("%w: sandboxes config references missing environment variable ${%s}",
				ErrConfig, missing)
		}
		return replaced, nil
	default:
		return value, nil
	}
}

// EnsureImagePinned enforces the supply-chain rule (design §7.5): an image
// must carry an @sha256: digest unless the registry entry explicitly opts out.
// Drivers without images (ssh) are not subject to the check.
func EnsureImagePinned(spec *WorkspaceSpec) error {
	if spec.Image == "" || spec.AllowUnpinned || strings.Contains(spec.Image, "@sha256:") {
		return nil
	}
	return fmt.Errorf("%w: sandbox %q image must be digest-pinned "+
		"(add @sha256:… or allow_unpinned=true): %s", ErrPin, spec.Name, spec.Image)
}

// IsDynamicExpression reports whether a workspace value is evaluated per
// execution; such values cannot be checked against the registry at load time.
func IsDynamicExpression(value string) bool {
	v := strings.TrimSpace(value)
	return strings.HasPrefix(v, "$") || strings.Contains(v, "{%")
}

// --- execution scope -----------------------------------------------------

type executionIDContextKey struct{}

// WithExecutionID attaches the worker's execution id to ctx. The engine
// runtime does not expose it to nodes, so the worker (or a test harness) can
// inject it here; agentrun otherwise falls back to DeriveExecutionID.
func WithExecutionID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, executionIDContextKey{}, id)
}

// ExecutionIDFromContext resolves an execution id in priority order:
// explicit context value → PLAITA_EXECUTION_ID env → runtime globals
// (execution_id / $EXECUTION_ID / PLAITA_EXECUTION_ID).
func ExecutionIDFromContext(ctx context.Context) string {
	if ctx != nil {
		if v, _ := ctx.Value(executionIDContextKey{}).(string); strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	if v := strings.TrimSpace(os.Getenv("PLAITA_EXECUTION_ID")); v != "" {
		return v
	}
	rc := runtime.WorkflowRuntimeContext(ctx)
	for _, key := range []string{"execution_id", "$EXECUTION_ID", "PLAITA_EXECUTION_ID"} {
		if v, ok := rc.GetGlobalVariable(key).(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// DeriveExecutionID is the fallback scope when no execution id is available
// to the node (the worker does not thread one into the engine context yet).
// It hashes the node id and workflow input, so a retried carrier with the same
// input re-attaches to the same sandbox. Distinct runs with identical inputs
// therefore share a workspace; the workspace lease still serializes them, and
// a release keeps the data layer. Documented deviation from the Python
// driver, which requires execution.execution_id.
func DeriveExecutionID(ctx context.Context, nodeID string) string {
	seed := struct {
		Node  string `json:"node"`
		Input any    `json:"input"`
	}{
		Node:  nodeID,
		Input: runtime.WorkflowRuntimeContext(ctx).GetWorkflowInput(),
	}
	blob, err := json.Marshal(seed)
	if err != nil {
		blob = []byte(fmt.Sprintf("%s:%v", nodeID, seed.Input))
	}
	sum := sha256.Sum256(blob)
	return "local-" + hex.EncodeToString(sum[:16])
}
