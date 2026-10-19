// Package llm is the LLM provider chain for the plaita-go engine: a
// chat-completions client speaking both the OpenAI-compatible and the
// Anthropic wire protocols (net/http only, zero new dependencies), plus the
// credential resolution chain shared by the llm / decision nodes.
//
// Credential red line: API keys never enter the repository. They arrive via
// ${VAR} interpolation (Python config.py semantics: missing variable is a
// fail-fast error) or from the providers.json bundle read out of the plaita
// config home (PLAITA_HOME, default ~/.plaita) — a file that lives outside
// any repository.
//
// Endpoint resolution priority mirrors plaita-nodes llm.py:51-73
// (resolve_llm_endpoint): explicit field values > provider bundle >
// LLM_API_BASE / LLM_API_KEY / LLM_MODEL environment variables.
package llm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Protocol selects the wire protocol of a completions endpoint.
type Protocol string

const (
	// ProtocolOpenAI is the OpenAI-compatible /chat/completions protocol —
	// the only protocol plaita-nodes speaks (llm.py, decision.py).
	ProtocolOpenAI Protocol = "openai"
	// ProtocolAnthropic is the Anthropic /v1/messages protocol (Go engine
	// extension; providers.json "type": "anthropic" selects it).
	ProtocolAnthropic Protocol = "anthropic"
)

// ConfigDir is the plaita config home (~/.plaita), overridable via the
// PLAITA_HOME environment variable.
func ConfigDir() string {
	if home := os.Getenv("PLAITA_HOME"); home != "" {
		return home
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".plaita"
	}
	return filepath.Join(home, ".plaita")
}

// Endpoint is a resolved (apiBase, apiKey, model) triple; APIBase carries no
// trailing slash (llm.py:73 rstrip("/")), Protocol defaults to OpenAI.
type Endpoint struct {
	APIBase  string
	APIKey   string
	Model    string
	Protocol Protocol
}

// ResolveEndpoint ports resolve_llm_endpoint (llm.py:51-73): the provider
// bundle (when name is given) fills the gaps left by explicit values, the
// LLM_* environment variables fill the rest, and an incomplete triple is a
// deterministic error. Explicit apiBase/apiKey go through ${VAR}
// interpolation (owner decision: credentials enter via env interpolation).
func ResolveEndpoint(apiBase, apiKey, model, provider string) (*Endpoint, error) {
	protocol := ProtocolOpenAI
	if provider != "" {
		bundle, err := ResolveProvider(provider)
		if err != nil {
			return nil, detErr("provider %q 解析失败: %v", provider, err)
		}
		if apiBase == "" {
			apiBase = bundle.APIBase
		}
		if apiKey == "" {
			apiKey = bundle.APIKey
		}
		if model == "" {
			model = bundle.Model
		}
		if bundle.Type == string(ProtocolAnthropic) {
			protocol = ProtocolAnthropic
		}
	}
	// ${VAR} interpolation on the credential fields (fail-fast on a missing
	// variable — plaita-nodes config.interpolate semantics).
	var err error
	if apiBase, err = interpolateString(apiBase); err != nil {
		return nil, err
	}
	if apiKey, err = interpolateString(apiKey); err != nil {
		return nil, err
	}
	if apiBase == "" {
		apiBase = os.Getenv("LLM_API_BASE")
	}
	if apiKey == "" {
		apiKey = os.Getenv("LLM_API_KEY")
	}
	if model == "" {
		model = os.Getenv("LLM_MODEL")
	}
	if apiBase == "" || apiKey == "" || model == "" {
		return nil, detErr("LLM 端点不完整：需要 api_base/api_key/model（字段、provider 或 " +
			"LLM_API_BASE/LLM_API_KEY/LLM_MODEL 环境变量）")
	}
	return &Endpoint{
		APIBase:  strings.TrimRight(apiBase, "/"),
		APIKey:   apiKey,
		Model:    model,
		Protocol: protocol,
	}, nil
}

// ProviderBundle is a resolved providers.json entry
// {name, type, apiBase, model, apiKey} (plaita-nodes config.py:95-122).
type ProviderBundle struct {
	Name    string
	Type    string // "openai" (default) / "anthropic" / …
	APIBase string
	Model   string
	APIKey  string
}

// ResolveProvider loads providers.json from the plaita config home and
// resolves one entry: ${VAR} interpolation over the raw bundle (fail-fast),
// keyEnv fallback for the API key, apiBase|base aliasing, type defaulting to
// "openai".
func ResolveProvider(name string) (*ProviderBundle, error) {
	providers, err := LoadProviders()
	if err != nil {
		return nil, err
	}
	raw, ok := providers[name]
	if !ok || raw == nil {
		return nil, detErr("provider %q 不存在（检查 providers.json）", name)
	}
	rawMap, ok := raw.(map[string]any)
	if !ok {
		return nil, detErr("provider %q 配置需为对象，得到 %T", name, raw)
	}
	interpolated, err := Interpolate(rawMap)
	if err != nil {
		return nil, err
	}
	bundle := interpolated.(map[string]any)

	apiKey := stringField(bundle, "apiKey")
	if apiKey == "" {
		if keyEnv := stringField(bundle, "keyEnv"); keyEnv != "" {
			v, ok := os.LookupEnv(keyEnv)
			if !ok {
				return nil, detErr("provider %q 声明的 keyEnv %q 未设置（环境变量缺失，apiKey 无法解析）", name, keyEnv)
			}
			if v == "" {
				return nil, detErr("provider %q 的 keyEnv %q 为空（apiKey 无法解析）", name, keyEnv)
			}
			apiKey = v
		}
	}
	btype := stringField(bundle, "type")
	if btype == "" {
		btype = "openai"
	}
	apiBase := stringField(bundle, "apiBase")
	if apiBase == "" {
		apiBase = stringField(bundle, "base")
	}
	return &ProviderBundle{
		Name:    name,
		Type:    btype,
		APIBase: apiBase,
		Model:   stringField(bundle, "model"),
		APIKey:  apiKey,
	}, nil
}

// LoadProviders reads providers.json from the plaita config home. Both the
// wrapped {"providers": {...}} shape and a bare top-level name→entry map are
// accepted (config.py:97 `(providers or {}).get("providers", providers)`).
func LoadProviders() (map[string]any, error) {
	path := filepath.Join(ConfigDir(), "providers.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load providers.json: %w", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("providers.json: %w", err)
	}
	if child, ok := doc["providers"].(map[string]any); ok {
		return child, nil
	}
	return doc, nil
}

// envPattern matches ${VAR} with a valid identifier name
// (config.py _ENV_PATTERN).
var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Interpolate expands ${VAR} recursively over strings, lists and maps. A
// missing variable is a fail-fast deterministic error — the plaita-nodes
// config.interpolate convention, so a half-resolved credential can never
// reach an endpoint silently.
func Interpolate(v any) (any, error) {
	switch t := v.(type) {
	case string:
		return interpolateString(t)
	case []any:
		out := make([]any, len(t))
		for i, el := range t {
			ev, err := Interpolate(el)
			if err != nil {
				return nil, err
			}
			out[i] = ev
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, el := range t {
			ev, err := Interpolate(el)
			if err != nil {
				return nil, err
			}
			out[k] = ev
		}
		return out, nil
	default:
		return v, nil
	}
}

func interpolateString(s string) (string, error) {
	if !strings.Contains(s, "${") {
		return s, nil
	}
	var missing string
	out := envPattern.ReplaceAllStringFunc(s, func(match string) string {
		name := envPattern.FindStringSubmatch(match)[1]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		if missing == "" {
			missing = name
		}
		return ""
	})
	if missing != "" {
		return "", detErr("环境变量 %s 未设置（配置里的 ${%s} 无法展开）", missing, missing)
	}
	return out, nil
}

func stringField(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}
