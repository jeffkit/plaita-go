package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const helloFlow = "../../examples/hello_world.json"

func runFlow(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	rc := runCLI(args, &stdout, &stderr)
	return rc, stdout.String(), stderr.String()
}

func writeFlow(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "flow.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunInlineHappyPath(t *testing.T) {
	rc, stdout, stderr := runFlow(t, "run", "-flow", helloFlow)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr)
	}
	if !strings.Contains(stdout, "Hello, world!") {
		t.Fatalf("stdout missing result: %q", stdout)
	}
}

func TestValidateOutput(t *testing.T) {
	rc, stdout, stderr := runFlow(t, "validate", "-flow", helloFlow)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr)
	}
	for _, want := range []string{`"ok":true`, `"flow_id":"hello-world-flow"`, `"nodes":2`} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %s: %q", want, stdout)
		}
	}
}

const gateDelayFlow = `{"id":"inline-gate","nodes":[
 {"id":"start","type":"start","next":"wait"},
 {"id":"wait","type":"delay","delay_seconds":1,"delay_unit":"seconds","event_type":"delay_trigger","next":"end"},
 {"id":"end","type":"end","resultType":"success"}]}`

func TestRunRefusesSuspendingFlow(t *testing.T) {
	rc, stdout, stderr := runFlow(t, "run", "-flow", writeFlow(t, gateDelayFlow))
	if rc != 1 {
		t.Fatalf("rc=%d stdout=%s", rc, stdout)
	}
	for _, want := range []string{"suspend_not_executable", `"stage":"entry"`, `"type":"delay"`, `"id":"wait"`} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr missing %s: %q", want, stderr)
		}
	}
}

func TestRunRefusesSuspendingNestedInChild(t *testing.T) {
	flow := `{"id":"inline-gate-nested","nodes":[
	 {"id":"start","type":"start","next":"child"},
	 {"id":"child","type":"child","childFlow":{"id":"sub","nodes":[
	   {"id":"s1","type":"start","next":"s2"},
	   {"id":"s2","type":"delay","delay_seconds":1,"delay_unit":"seconds","event_type":"delay_trigger","next":"s3"},
	   {"id":"s3","type":"end","resultType":"success"}]},"next":"end"},
	 {"id":"end","type":"end","resultType":"success"}]}`
	rc, _, stderr := runFlow(t, "run", "-flow", writeFlow(t, flow))
	if rc != 1 {
		t.Fatalf("rc=%d (nested suspending node must be refused)", rc)
	}
	if !strings.Contains(stderr, `"id":"s2"`) || !strings.Contains(stderr, "suspend_not_executable") {
		t.Fatalf("stderr missing nested node anchor: %q", stderr)
	}
}

func TestRunDeadlineExceeded(t *testing.T) {
	t.Setenv("PLAITA_EXECUTION_DEADLINE_SECONDS", "1")
	flow := `{"id":"inline-deadline","nodes":[
	 {"id":"start","type":"start","next":"spin"},
	 {"id":"spin","type":"code","language":"javascript","code":"while(true){}","next":"end"},
	 {"id":"end","type":"end","resultType":"success"}]}`
	start := time.Now()
	rc, stdout, stderr := runFlow(t, "run", "-flow", writeFlow(t, flow))
	if rc != 1 {
		t.Fatalf("rc=%d stdout=%s", rc, stdout)
	}
	if !strings.Contains(stderr, "execution_deadline_exceeded") {
		t.Fatalf("stderr missing deadline anchor: %q", stderr)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("deadline enforcement took %s — cancel not propagated", elapsed)
	}
}

func TestExecutionDeadlineInvalidEnvFallsBackWithWarning(t *testing.T) {
	t.Setenv("PLAITA_EXECUTION_DEADLINE_SECONDS", "abc")
	rc, stdout, stderr := runFlow(t, "run", "-flow", helloFlow)
	if rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, stderr)
	}
	if !strings.Contains(stdout, "Hello, world!") {
		t.Fatalf("flow did not run: %q", stdout)
	}
	if !strings.Contains(stderr, "falling back to default") {
		t.Fatalf("stderr missing fallback warning: %q", stderr)
	}
}

func TestInputMutuallyExclusive(t *testing.T) {
	rc, _, stderr := runFlow(t, "run", "-flow", helloFlow, "-input", "{}", "-input-file", helloFlow)
	if rc != 1 {
		t.Fatalf("rc=%d", rc)
	}
	if !strings.Contains(stderr, "mutually exclusive") {
		t.Fatalf("stderr missing exclusivity error: %q", stderr)
	}
}

func TestUsageAndUnknownCommand(t *testing.T) {
	if rc, _, _ := runFlow(t); rc != 2 {
		t.Fatalf("bare rc=%d", rc)
	}
	if rc, _, stderr := runFlow(t, "bogus"); rc != 2 || !strings.Contains(stderr, "unknown command") {
		t.Fatalf("bogus rc=%d stderr=%s", rc, stderr)
	}
}
