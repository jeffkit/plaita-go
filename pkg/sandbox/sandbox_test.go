package sandbox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadSandboxesMergesHomeAndRepo(t *testing.T) {
	home := t.TempDir()
	repo := t.TempDir()
	t.Setenv("PLAITA_HOME", home)
	t.Setenv("SANDBOX_TEST_REF", "ref-value")

	writeFile(t, filepath.Join(home, "sandboxes.json"), `{
		"sandboxes": {
			"a": {"driver": "docker", "image": "img@sha256:abc",
			      "env": {"X": "1", "REF": "${SANDBOX_TEST_REF}"},
			      "resources": {"cpus": 1, "memory": "512m"}},
			"b": {"driver": "docker", "image": "img-b", "allow_unpinned": true}
		}
	}`)
	writeFile(t, filepath.Join(repo, ".plaita", "sandboxes.json"), `{
		"sandboxes": {
			"a": {"env": {"Y": "2"}, "resources": {"cpus": 2}, "egress": "none"}
		}
	}`)

	specs, err := LoadSandboxes(repo)
	if err != nil {
		t.Fatalf("LoadSandboxes: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("specs = %v", specs)
	}
	a := specs["a"]
	if a.Image != "img@sha256:abc" || a.Driver != "docker" {
		t.Errorf("a = %+v", a)
	}
	if a.Env["X"] != "1" || a.Env["REF"] != "ref-value" || a.Env["Y"] != "2" {
		t.Errorf("env = %v", a.Env)
	}
	if a.Resources["cpus"] != float64(2) || a.Resources["memory"] != "512m" {
		t.Errorf("resources = %v", a.Resources)
	}
	if a.Egress != "none" {
		t.Errorf("egress = %q", a.Egress)
	}
	if b := specs["b"]; b == nil || !b.AllowUnpinned {
		t.Errorf("b = %+v", b)
	}
}

func TestLoadSandboxesYAMLAndBareTopLevel(t *testing.T) {
	home := t.TempDir()
	t.Setenv("PLAITA_HOME", home)
	writeFile(t, filepath.Join(home, "sandboxes.yaml"), `
yaml-sbx:
  driver: docker
  image: img@sha256:abc
  provision:
    git:
      repo: https://example.com/r.git
  resources:
    memory: 1g
`)
	specs, err := LoadSandboxes("")
	if err != nil {
		t.Fatalf("LoadSandboxes: %v", err)
	}
	spec := specs["yaml-sbx"]
	if spec == nil {
		t.Fatalf("specs = %v", specs)
	}
	if spec.GitRepo() != "https://example.com/r.git" {
		t.Errorf("git repo = %q", spec.GitRepo())
	}
}

func TestLoadSandboxesMissingInterpolationVariable(t *testing.T) {
	home := t.TempDir()
	t.Setenv("PLAITA_HOME", home)
	writeFile(t, filepath.Join(home, "sandboxes.json"),
		`{"sandboxes":{"a":{"image":"${SANDBOX_TEST_MISSING_XYZ}"}}}`)
	_, err := LoadSandboxes("")
	if err == nil || !errors.Is(err, ErrConfig) {
		t.Fatalf("err = %v, want ErrConfig", err)
	}
	if !strings.Contains(err.Error(), "SANDBOX_TEST_MISSING_XYZ") {
		t.Errorf("err = %v", err)
	}
}

func TestLoadSandboxesSpecValidation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("PLAITA_HOME", home)
	cases := map[string]string{
		"missing image":        `{"sandboxes":{"a":{"driver":"docker"}}}`,
		"git without repo":     `{"sandboxes":{"a":{"image":"img","provision":{"git":{}}}}}`,
		"env not object":       `{"sandboxes":{"a":{"image":"img","env":[]}}}`,
		"sandboxes not object": `{"sandboxes":[]}`,
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			writeFile(t, filepath.Join(home, "sandboxes.json"), doc)
			if _, err := LoadSandboxes(""); err == nil || !errors.Is(err, ErrConfig) {
				t.Fatalf("err = %v, want ErrConfig", err)
			}
		})
	}
}

func TestEnsureImagePinned(t *testing.T) {
	if err := EnsureImagePinned(&WorkspaceSpec{Name: "a", Image: "img@sha256:abc"}); err != nil {
		t.Errorf("pinned image: %v", err)
	}
	if err := EnsureImagePinned(&WorkspaceSpec{Name: "a", Image: "img:latest", AllowUnpinned: true}); err != nil {
		t.Errorf("allow_unpinned: %v", err)
	}
	if err := EnsureImagePinned(&WorkspaceSpec{Name: "a", Image: "img:latest"}); !errors.Is(err, ErrPin) {
		t.Errorf("err = %v, want ErrPin", err)
	}
	if err := EnsureImagePinned(&WorkspaceSpec{Name: "ssh", Driver: "ssh"}); err != nil {
		t.Errorf("no image: %v", err)
	}
}

func TestNaming(t *testing.T) {
	t.Setenv("PLAITA_SANDBOX_PREFIX", "shadow-ws")
	if got := SanitizeWSKey("task/one:two"); got != "task-one-two" {
		t.Errorf("SanitizeWSKey = %q", got)
	}
	if got := SanitizeWSKey("---"); got != "default" {
		t.Errorf("SanitizeWSKey fallback = %q", got)
	}
	if got := HandleID("e1", "w1"); got != "e1:w1" {
		t.Errorf("HandleID = %q", got)
	}
	name := ResourceName("e1", "w1", "ctr")
	if !strings.HasPrefix(name, "shadow-ws-ctr-") || !strings.Contains(name, "e1-w1") {
		t.Errorf("ResourceName = %q", name)
	}
	long := ResourceName(strings.Repeat("x", 500), strings.Repeat("y", 500), "data")
	if len(long) > 220 {
		t.Errorf("long name not capped: %d", len(long))
	}

	// A shadow worker's key prefix isolates Docker resources automatically.
	t.Setenv("PLAITA_SANDBOX_PREFIX", "")
	t.Setenv("PLAITA_KEY_PREFIX", "plaita-go-shadow")
	if got := NamePrefix(); got != "plaita-go-shadow-ws" {
		t.Errorf("derived prefix = %q", got)
	}
}

func TestExecutionScopeResolution(t *testing.T) {
	t.Setenv("PLAITA_EXECUTION_ID", "")
	ctx := WithExecutionID(context.Background(), "ctx-id")
	if got := ExecutionIDFromContext(ctx); got != "ctx-id" {
		t.Errorf("context id = %q", got)
	}

	t.Setenv("PLAITA_EXECUTION_ID", "env-id")
	if got := ExecutionIDFromContext(context.Background()); got != "env-id" {
		t.Errorf("env id = %q", got)
	}
	t.Setenv("PLAITA_EXECUTION_ID", "")

	runtimeCtx := runtime.NewContext()
	runtimeCtx.WithGlobalVariable("execution_id", "global-id")
	withGlobal := runtime.WithWorkflowRuntimeContext(context.Background(), runtimeCtx)
	if got := ExecutionIDFromContext(withGlobal); got != "global-id" {
		t.Errorf("global id = %q", got)
	}

	makeCtx := func(input map[string]any) context.Context {
		rc := runtime.NewContext()
		rc.WithWorkflowInput(input)
		return runtime.WithWorkflowRuntimeContext(context.Background(), rc)
	}
	first := DeriveExecutionID(makeCtx(map[string]any{"x": 1}), "node")
	second := DeriveExecutionID(makeCtx(map[string]any{"x": 1}), "node")
	third := DeriveExecutionID(makeCtx(map[string]any{"x": 2}), "node")
	if first == "" || first != second {
		t.Errorf("derived ids = %q %q", first, second)
	}
	if first == third {
		t.Errorf("derived id ignores the input: %q", first)
	}
}

func TestEnvFileAndRedactor(t *testing.T) {
	content, err := EnvFileContent(map[string]string{"B": "2", "A": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "A=1\nB=2\n" {
		t.Errorf("content = %q", content)
	}
	if err := ValidateEnv(map[string]string{"bad-name": "x"}); !errors.Is(err, ErrConfig) {
		t.Errorf("bad name err = %v", err)
	}
	if err := ValidateEnv(map[string]string{"A": "line\nbreak"}); !errors.Is(err, ErrConfig) {
		t.Errorf("newline err = %v", err)
	}

	path, err := WriteEnvFile(map[string]string{"TOKEN": "secret-value"})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("mode = %o", mode)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "TOKEN=secret-value\n" {
		t.Errorf("data = %q", data)
	}
	BurnEnvFile(path)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("envfile not burned: %v", err)
	}

	redactor := NewRedactor(map[string]string{"SHORT": "abc", "LONG": "abcdefgh"})
	if got := redactor.Redact("abcdefgh abc"); got != "[REDACTED:LONG] abc" {
		t.Errorf("redact = %q", got)
	}
}
