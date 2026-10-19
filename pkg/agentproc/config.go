// Package agentproc implements the host side of the agentproc protocol
// (spec/protocol.md v0.4 in the agentproc repository): spawn an agent
// process from a registry entry, deliver exactly one turn object on stdin,
// and assemble the reply from the NDJSON event stream on stdout.
//
// The spawn path only — CLI adaptation stays in the agentproc hub bridges
// (the same bridge.py / bridge.js the Python runtime uses), so the two
// runtimes share one adapter implementation. In-process executors are a
// per-SDK registry by spec; if one is ever needed in Go it slots in ahead
// of the spawn per the spec's four-case rule.
package agentproc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DefaultConfigDir is the plaita config home (~/.plaita), overridable via
// the PLAITA_HOME environment variable.
func DefaultConfigDir() string {
	if home := os.Getenv("PLAITA_HOME"); home != "" {
		return home
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".plaita"
	}
	return filepath.Join(home, ".plaita")
}

// AgentSpec is a resolved agent registry entry ready to spawn.
type AgentSpec struct {
	Name     string
	Executor string
	Model    string
	Command  string
	Args     []string
	Env      map[string]string
}

// Registry resolves agent names against the plaita config home
// (agents.json / providers.json — the same files the Python runtime reads).
type Registry struct {
	Dir string
}

// NewRegistry returns a registry rooted at the default config dir.
func NewRegistry() *Registry { return &Registry{Dir: DefaultConfigDir()} }

// ResolveAgent resolves an agent name into a spawnable spec.
//
// Env composition mirrors the Python runtime: provider bundle → executor
// translator (recursive / claude), then the entry's own env on top, then
// ${VAR} interpolation against the current process environment. Entries
// MUST carry an explicit `command` (+ optional `args` / `extraArgs`) — the
// Go engine drives the agentproc spawn path, so a bridge command is part of
// the agent's deployment configuration and its absence is a config error,
// never a fallback.
func (r *Registry) ResolveAgent(name string) (*AgentSpec, error) {
	raw, err := r.agentEntry(name)
	if err != nil {
		return nil, err
	}

	spec := &AgentSpec{Name: name, Env: map[string]string{}}
	spec.Executor, _ = raw["executor"].(string)
	if spec.Executor == "" {
		return nil, fmt.Errorf("agent %q: missing executor field", name)
	}

	// provider → executor env translation. A locked CLI (no translator)
	// must not declare a provider, mirroring the Python rule.
	if providerName, _ := raw["provider"].(string); providerName != "" {
		bundle, err := r.resolveProvider(providerName)
		if err != nil {
			return nil, fmt.Errorf("agent %q: %v", name, err)
		}
		translate, ok := envTranslators[spec.Executor]
		if !ok {
			return nil, fmt.Errorf("agent %q: executor %q is a locked CLI and cannot take provider %q",
				name, spec.Executor, providerName)
		}
		for k, v := range translate(bundle, stringField(raw, "model")) {
			spec.Env[k] = v
		}
		if spec.Model == "" {
			spec.Model = bundle.Model
		}
	}
	if spec.Model == "" {
		spec.Model = stringField(raw, "model")
	}
	for k, v := range mapField(raw, "env") {
		spec.Env[k] = v
	}

	spec.Command, _ = raw["command"].(string)
	if spec.Command == "" {
		return nil, fmt.Errorf("agent %q: missing command (the Go engine spawns the agentproc bridge; "+
			"add command/args to the agents.json entry, e.g. python3 <hub>/<executor>/bridge.py)", name)
	}
	for _, a := range listField(raw, "args") {
		spec.Args = append(spec.Args, a)
	}
	for _, a := range listField(raw, "extraArgs") {
		spec.Args = append(spec.Args, a)
	}

	// ${VAR} interpolation last, against the current process environment.
	spec.Command = expandVars(spec.Command)
	for i, a := range spec.Args {
		spec.Args[i] = expandVars(a)
	}
	for k, v := range spec.Env {
		spec.Env[k] = expandVars(v)
	}
	return spec, nil
}

func (r *Registry) agentEntry(name string) (map[string]any, error) {
	agentsDoc, err := readJSONFile(filepath.Join(r.Dir, "agents.json"))
	if err != nil {
		return nil, fmt.Errorf("load agents.json: %w", err)
	}
	agents := mapOrChild(agentsDoc, "agents")
	raw, ok := agents[name].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("agent %q not found (check agents.json)", name)
	}
	return raw, nil
}

// providerBundle is the resolved providers.json entry.
type providerBundle struct {
	Type    string
	APIBase string
	Model   string
	APIKey  string
}

func (r *Registry) resolveProvider(name string) (*providerBundle, error) {
	providersDoc, err := readJSONFile(filepath.Join(r.Dir, "providers.json"))
	if err != nil {
		return nil, fmt.Errorf("load providers.json: %w", err)
	}
	providers := mapOrChild(providersDoc, "providers")
	raw, ok := providers[name].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("provider %q not found (check providers.json)", name)
	}
	bundle := &providerBundle{
		Type:    stringField(raw, "type"),
		APIBase: stringField(raw, "apiBase"),
		Model:   stringField(raw, "model"),
		APIKey:  stringField(raw, "apiKey"),
	}
	if bundle.Type == "" {
		bundle.Type = "openai"
	}
	if bundle.APIBase == "" {
		bundle.APIBase = stringField(raw, "base")
	}
	if bundle.APIKey == "" {
		if keyEnv := stringField(raw, "keyEnv"); keyEnv != "" {
			v := os.Getenv(keyEnv)
			if v == "" {
				return nil, fmt.Errorf("provider %q declares keyEnv %q but it is not set", name, keyEnv)
			}
			bundle.APIKey = v
		}
	}
	return bundle, nil
}

// envTranslators maps an executor to its provider env translation — the
// same keys the Python runtime's _ENV_TRANSLATORS produce.
var envTranslators = map[string]func(*providerBundle, string) map[string]string{
	"recursive": func(b *providerBundle, model string) map[string]string {
		env := map[string]string{
			"RECURSIVE_PROVIDER_TYPE": b.Type,
			"RECURSIVE_API_BASE":      b.APIBase,
			"RECURSIVE_API_KEY":       b.APIKey,
		}
		if m := firstNonEmpty(model, b.Model); m != "" {
			env["RECURSIVE_MODEL"] = m
		}
		return env
	},
	"claude": func(b *providerBundle, model string) map[string]string {
		env := map[string]string{
			"ANTHROPIC_BASE_URL":   b.APIBase,
			"ANTHROPIC_AUTH_TOKEN": b.APIKey,
		}
		if m := firstNonEmpty(model, b.Model); m != "" {
			env["CLAUDE_MODEL"] = m
		}
		return env
	},
}

// expandVars substitutes ${VAR} from the process environment. Unknown
// variables expand to "" (Python interpolate convention).
func expandVars(s string) string {
	return os.Expand(s, func(key string) string { return os.Getenv(key) })
}

// --- small JSON helpers ---

func readJSONFile(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	return doc, nil
}

// mapOrChild supports both {agents: {...}} and a bare top-level map.
func mapOrChild(doc map[string]any, key string) map[string]any {
	if child, ok := doc[key].(map[string]any); ok {
		return child
	}
	return doc
}

func stringField(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func mapField(m map[string]any, key string) map[string]string {
	out := map[string]string{}
	raw, ok := m[key].(map[string]any)
	if !ok {
		return out
	}
	for k, v := range raw {
		out[k] = fmt.Sprint(v)
	}
	return out
}

func listField(m map[string]any, key string) []string {
	raw, ok := m[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		out = append(out, fmt.Sprint(v))
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
