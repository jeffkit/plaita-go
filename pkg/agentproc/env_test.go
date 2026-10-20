package agentproc

import (
	"context"
	"strings"
	"testing"
	"time"
)

// envMap turns a composeEnv result into a map for assertions.
func envMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, kv := range env {
		if i := strings.IndexByte(kv, '='); i > 0 {
			out[kv[:i]] = kv[i+1:]
		}
	}
	return out
}

// TestComposeEnvUsesAllowlist pins the plaita subprocess_env contract: the
// host environment is rebuilt from the allowlist, never copied wholesale, so
// worker credentials (PLAITA_CREDENTIALS_KEY, provider keys) cannot reach the
// agent process except through its explicit spec.Env.
func TestComposeEnvUsesAllowlist(t *testing.T) {
	t.Setenv("PROBE_SECRET", "host-secret")
	t.Setenv("PLAITA_CREDENTIALS_KEY", "fernet-key")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "host-provider-key")

	env := envMap(composeEnv(map[string]string{"RECURSIVE_API_KEY": "provider-key"}))

	for _, leaked := range []string{"PROBE_SECRET", "PLAITA_CREDENTIALS_KEY", "ANTHROPIC_AUTH_TOKEN"} {
		if _, present := env[leaked]; present {
			t.Errorf("host variable %q leaked into the agent environment", leaked)
		}
	}
	if _, present := env["PATH"]; !present {
		t.Error("PATH missing: the allowlist must keep the shell's lookup path")
	}
	if env["RECURSIVE_API_KEY"] != "provider-key" {
		t.Errorf("explicit env lost: RECURSIVE_API_KEY=%q", env["RECURSIVE_API_KEY"])
	}
}

// TestComposeEnvDeploymentExtra pins the SUBPROCESS_ENV_EXTRA analogue: a
// deployment can add host variables explicitly, and spec env still wins.
func TestComposeEnvDeploymentExtra(t *testing.T) {
	previous := HostEnvExtra
	HostEnvExtra = map[string]string{"HTTPS_PROXY": "http://proxy:8080", "WIN": "from-extra"}
	t.Cleanup(func() { HostEnvExtra = previous })

	env := envMap(composeEnv(map[string]string{"WIN": "from-spec"}))
	if env["HTTPS_PROXY"] != "http://proxy:8080" {
		t.Errorf("deployment extra missing: %v", env)
	}
	if env["WIN"] != "from-spec" {
		t.Errorf("spec env must override the deployment extra, got %q", env["WIN"])
	}
}

// TestRunChildEnvDoesNotInheritHost is the spawn-level version of the
// composeEnv test: a real child process must not observe the host secret
// while still receiving its declared env.
func TestRunChildEnvDoesNotInheritHost(t *testing.T) {
	t.Setenv("PROBE_SECRET", "host-secret")
	spec := &AgentSpec{
		Name:    "fake",
		Command: "/bin/sh",
		Args: []string{"-c",
			`IFS= read -r _ || true; printf '{"type":"result","text":"[%s][%s]"}\n' "$PROBE_SECRET" "$EXPLICIT"`},
		Env: map[string]string{"EXPLICIT": "declared"},
	}
	res, err := Run(context.Background(), spec, RunOptions{
		Message: "hi", Streaming: true, Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "[][declared]" {
		t.Fatalf("child env = %q, want host secret absent and declared env present", res.Text)
	}
}
