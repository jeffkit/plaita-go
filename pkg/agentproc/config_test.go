package agentproc

import (
	"os"
	"path/filepath"
	stdruntime "runtime"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, agents, providers string) *Registry {
	t.Helper()
	dir := t.TempDir()
	if agents != "" {
		if err := os.WriteFile(filepath.Join(dir, "agents.json"), []byte(agents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if providers != "" {
		if err := os.WriteFile(filepath.Join(dir, "providers.json"), []byte(providers), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return &Registry{Dir: dir}
}

const testProviders = `{"providers": {
  "glm": {"type": "anthropic", "apiBase": "https://open.bigmodel.cn/api/anthropic", "model": "glm-5.3-flash", "apiKey": "key-123"},
  "viaEnv": {"type": "anthropic", "apiBase": "https://x", "keyEnv": "TEST_PROVIDER_KEY"}
}}`

func TestResolveRecursiveWithProvider(t *testing.T) {
	r := writeConfig(t, `{"agents": {
    "glm53": {"executor": "recursive", "provider": "glm", "model": "glm-5.3-flash",
      "env": {"RECURSIVE_MAX_TOKENS": "65536"}}
  }}`, testProviders)

	spec, err := r.ResolveAgent("glm53")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	// recursive is aliased to the native recursive-direct executor: no
	// command needed, argv built per turn
	if spec.Mode != ModeRecursiveDirect {
		t.Errorf("mode = %q, want recursive-direct", spec.Mode)
	}
	for _, want := range [][2]string{
		{"RECURSIVE_PROVIDER_TYPE", "anthropic"},
		{"RECURSIVE_API_BASE", "https://open.bigmodel.cn/api/anthropic"},
		{"RECURSIVE_API_KEY", "key-123"},
		{"RECURSIVE_MODEL", "glm-5.3-flash"},
		{"RECURSIVE_MAX_TOKENS", "65536"},
	} {
		if got := spec.Env[want[0]]; got != want[1] {
			t.Errorf("env[%s] = %q, want %q", want[0], got, want[1])
		}
	}
	if spec.Model != "glm-5.3-flash" {
		t.Errorf("model = %q", spec.Model)
	}
}

func TestResolveCommandPassthrough(t *testing.T) {
	r := writeConfig(t, `{"agents": {
    "cc": {"executor": "claude", "provider": "glm", "command": "python3",
      "args": ["/hub/claude-code/bridge.py"], "extraArgs": ["--flag"]}
  }}`, testProviders)
	spec, err := r.ResolveAgent("cc")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if spec.Command != "python3" || len(spec.Args) != 2 || spec.Args[0] != "/hub/claude-code/bridge.py" || spec.Args[1] != "--flag" {
		t.Errorf("spawn config = %s %v", spec.Command, spec.Args)
	}
}

func TestResolveClaudeTranslator(t *testing.T) {
	r := writeConfig(t, `{"agents": {
    "cc": {"executor": "claude", "provider": "glm", "command": "x", "args": []}
  }}`, testProviders)
	spec, err := r.ResolveAgent("cc")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if spec.Env["ANTHROPIC_BASE_URL"] == "" || spec.Env["ANTHROPIC_AUTH_TOKEN"] != "key-123" {
		t.Errorf("env = %v", spec.Env)
	}
	if spec.Env["CLAUDE_MODEL"] != "glm-5.3-flash" {
		t.Errorf("model env = %q", spec.Env["CLAUDE_MODEL"])
	}
}

func TestResolveLockedExecutorRejectsProvider(t *testing.T) {
	r := writeConfig(t, `{"agents": {
    "cur": {"executor": "cursor", "provider": "glm", "command": "x"}
  }}`, testProviders)
	_, err := r.ResolveAgent("cur")
	if err == nil || !strings.Contains(err.Error(), "locked CLI") {
		t.Fatalf("err = %v, want locked-CLI rejection", err)
	}
}

func TestResolveMissingCommandFailsClosed(t *testing.T) {
	// protocol-path executors need an explicit spawn command; recursive is
	// aliased to the native recursive-direct executor and needs none
	r := writeConfig(t, `{"agents": {"a": {"executor": "cursor"}}}`, "")
	_, err := r.ResolveAgent("a")
	if err == nil || !strings.Contains(err.Error(), "missing command") {
		t.Fatalf("err = %v, want missing command", err)
	}
}

func TestResolveKeyEnv(t *testing.T) {
	t.Setenv("TEST_PROVIDER_KEY", "from-env-999")
	r := writeConfig(t, `{"agents": {
    "a": {"executor": "claude", "provider": "viaEnv", "command": "x"}
  }}`, testProviders)
	spec, err := r.ResolveAgent("a")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if spec.Env["ANTHROPIC_AUTH_TOKEN"] != "from-env-999" {
		t.Errorf("auth token = %q", spec.Env["ANTHROPIC_AUTH_TOKEN"])
	}
}

func TestResolveVarExpansion(t *testing.T) {
	t.Setenv("TEST_HUB_DIR", "/opt/hub")
	r := writeConfig(t, `{"agents": {
    "a": {"executor": "cursor", "command": "python3", "args": ["${TEST_HUB_DIR}/bridge.py"], "env": {"W": "${TEST_HUB_DIR}"}}
  }}`, "")
	spec, err := r.ResolveAgent("a")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if spec.Args[0] != "/opt/hub/bridge.py" || spec.Env["W"] != "/opt/hub" {
		t.Errorf("args=%v env=%v", spec.Args, spec.Env)
	}
}

func TestResolveUnknownAgent(t *testing.T) {
	r := writeConfig(t, `{"agents": {}}`, "")
	_, err := r.ResolveAgent("nope")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v", err)
	}
}

// TestConfigFilesRequirePrivatePermissions pins A62-05: agents.json and
// providers.json carry plaintext credentials, so any group/other bit is a
// fail-loud load error (no warn-and-continue).
func TestConfigFilesRequirePrivatePermissions(t *testing.T) {
	if stdruntime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not enforced on Windows")
	}
	for _, name := range []string{"agents.json", "providers.json"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			if err := os.WriteFile(path, []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := readJSONFile(path)
			if err == nil {
				t.Fatalf("%s with mode 0644 must be refused", name)
			}
			if !strings.Contains(err.Error(), "chmod 600") {
				t.Errorf("error should name the fix, got: %v", err)
			}
			if err := os.Chmod(path, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readJSONFile(path); err != nil {
				t.Fatalf("%s with mode 0600 must load: %v", name, err)
			}
		})
	}
}

// TestResolveAgentRefusesLooseConfigModes proves the check is wired into the
// real resolution paths (agents.json and providers.json), not just the helper.
func TestResolveAgentRefusesLooseConfigModes(t *testing.T) {
	if stdruntime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not enforced on Windows")
	}

	t.Run("agents.json", func(t *testing.T) {
		r := writeConfig(t, `{"agents": {"a": {"executor": "cursor", "command": "x"}}}`, "")
		if err := os.Chmod(filepath.Join(r.Dir, "agents.json"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := r.ResolveAgent("a"); err == nil || !strings.Contains(err.Error(), "chmod 600") {
			t.Fatalf("err = %v, want a permission refusal", err)
		}
	})

	t.Run("providers.json", func(t *testing.T) {
		r := writeConfig(t, `{"agents": {"a": {"executor": "claude", "provider": "glm", "command": "x"}}}`, testProviders)
		if err := os.Chmod(filepath.Join(r.Dir, "providers.json"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := r.ResolveAgent("a"); err == nil || !strings.Contains(err.Error(), "chmod 600") {
			t.Fatalf("err = %v, want a permission refusal (plaintext provider key)", err)
		}
	})
}
