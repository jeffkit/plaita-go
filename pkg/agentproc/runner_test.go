package agentproc

import (
	"context"
	"strings"
	"testing"
	"time"
)

// fake bridges are /bin/sh scripts that emit canned protocol lines; the
// spec's own conformance suite exercises the real bridges, these pin the
// host-side state machine.

func runScript(t *testing.T, script string, opts RunOptions, timeout time.Duration) (*RunResult, error) {
	t.Helper()
	spec := &AgentSpec{Name: "fake", Command: "/bin/sh", Args: []string{"-c", script}}
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	opts.Timeout = timeout
	return Run(context.Background(), spec, opts)
}

func TestRunResultOnly(t *testing.T) {
	res, err := runScript(t, `printf '%s\n' '{"type":"result","text":"hello world"}'`, RunOptions{Message: "hi", Streaming: true}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "hello world" {
		t.Errorf("text = %q", res.Text)
	}
}

func TestRunStreamingAssembly(t *testing.T) {
	script := `printf '%s\n' '{"type":"partial","text":"hel"}' '{"type":"partial","text":"lo "}' '{"type":"result","text":"hello world FULL"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi", Streaming: true}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// partials carry the body; result.text must NOT be appended again
	if res.Text != "hello " {
		t.Errorf("text = %q, want partial concatenation", res.Text)
	}
}

func TestRunNonStreamingUsesResultText(t *testing.T) {
	script := `printf '%s\n' '{"type":"partial","text":"ignored"}' '{"type":"result","text":"final"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi", Streaming: false}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "final" {
		t.Errorf("text = %q, want result.text", res.Text)
	}
}

func TestRunErrorEventIsTerminalEvenWithExitZero(t *testing.T) {
	script := `printf '%s\n' '{"type":"partial","text":"half "}' '{"type":"error","message":"upstream rate limited"}'; exit 0`
	res, err := runScript(t, script, RunOptions{Message: "hi", Streaming: true}, 0)
	if err == nil {
		t.Fatal("expected failure on error event")
	}
	if !strings.Contains(err.Error(), "upstream rate limited") {
		t.Errorf("error = %v", err)
	}
	if res == nil || !strings.Contains(res.Text, "half") {
		t.Errorf("delivered partials should survive: %+v", res)
	}
}

func TestRunNoResultExitZeroSucceeds(t *testing.T) {
	res, err := runScript(t, `exit 0`, RunOptions{Message: "hi", Streaming: true}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "" {
		t.Errorf("text = %q, want empty", res.Text)
	}
}

func TestRunNonZeroExitWithoutErrorEvent(t *testing.T) {
	_, err := runScript(t, `exit 3`, RunOptions{Message: "hi"}, 0)
	if err == nil || !strings.Contains(err.Error(), "code 3") {
		t.Fatalf("err = %v, want exit code 3", err)
	}
}

func TestRunSessionFirstNonEmptyWins(t *testing.T) {
	script := `printf '%s\n' '{"type":"partial","text":"a","session_id":"sess-1"}' '{"type":"result","text":"x","session_id":"sess-2"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.SessionID != "sess-1" {
		t.Errorf("session = %q, want first non-empty sess-1", res.SessionID)
	}
}

func TestRunSessionInboundPreservedWhenEventsSilent(t *testing.T) {
	res, err := runScript(t, `exit 0`, RunOptions{Message: "hi", SessionID: "inbound-1"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.SessionID != "inbound-1" {
		t.Errorf("session = %q, want inbound-1", res.SessionID)
	}
}

func TestRunInvalidSessionDropped(t *testing.T) {
	script := `printf '%s\n' '{"type":"result","text":"x","session_id":"a/b"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.SessionID != "" {
		t.Errorf("session = %q, want dropped (path separator)", res.SessionID)
	}
}

func TestRunMalformedLinesIgnored(t *testing.T) {
	script := `printf '%s\n' 'not json' '{"type":"what"}' '[]' '{"type":"result","text":"ok"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "ok" {
		t.Errorf("text = %q", res.Text)
	}
}

func TestRunUsageCaptured(t *testing.T) {
	script := `printf '%s\n' '{"type":"result","text":"x","usage":{"input_tokens":12,"output_tokens":34}}'`
	res, err := runScript(t, script, RunOptions{Message: "hi"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Usage == nil || res.Usage["input_tokens"] != float64(12) {
		t.Errorf("usage = %+v", res.Usage)
	}
}

func TestRunTurnMessageDeliveredOnStdin(t *testing.T) {
	script := `IFS= read -r line; escaped=$(printf '%s' "$line" | sed 's/"/\\"/g'); printf '{"type":"result","text":"%s"}\n' "$escaped"`
	res, err := runScript(t, script, RunOptions{Message: "the prompt body"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(res.Text, "the prompt body") {
		t.Errorf("turn line not delivered: %q", res.Text)
	}
}

func TestRunTimeoutKillsProcessGroup(t *testing.T) {
	start := time.Now()
	// child sleeps well past the timeout; the kill must reap the group and
	// return promptly
	_, err := runScript(t, `sleep 30`, RunOptions{Message: "hi"}, 300*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("err = %v, want timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("timeout kill took %s", elapsed)
	}
}

func TestRunDetailsCollected(t *testing.T) {
	script := `printf '%s\n' '{"type":"partial","text":"a"}' '{"type":"result","text":"b"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi", CollectDetails: true}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Details) != 2 {
		t.Errorf("details = %v", res.Details)
	}
}

func TestRunEmptyMessageRejected(t *testing.T) {
	spec := &AgentSpec{Name: "fake", Command: "/bin/sh", Args: []string{"-c", "exit 0"}}
	if _, err := Run(context.Background(), spec, RunOptions{Message: "  "}); err == nil {
		t.Fatal("empty turn must be rejected (spec: empty turns are an error)")
	}
}
