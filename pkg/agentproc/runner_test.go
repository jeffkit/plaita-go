package agentproc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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
	res, err := runScript(t, `IFS= read -r _ || true; printf '%s\n' '{"type":"result","text":"hello world"}'`, RunOptions{Message: "hi", Streaming: true}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "hello world" {
		t.Errorf("text = %q", res.Text)
	}
}

func TestRunStreamingAssembly(t *testing.T) {
	script := `IFS= read -r _ || true; printf '%s\n' '{"type":"partial","text":"hel"}' '{"type":"partial","text":"lo "}' '{"type":"result","text":"hello world FULL"}'`
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
	script := `IFS= read -r _ || true; printf '%s\n' '{"type":"partial","text":"ignored"}' '{"type":"result","text":"final"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi", Streaming: false, CollectDetails: true}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "final" {
		t.Fatalf("text = %q, want result.text; raw lines: %q", res.Text, res.Details)
	}
}

func TestRunErrorEventIsTerminalEvenWithExitZero(t *testing.T) {
	script := `IFS= read -r _ || true; printf '%s\n' '{"type":"partial","text":"half "}' '{"type":"error","message":"upstream rate limited"}'; exit 0`
	res, err := runScript(t, script, RunOptions{Message: "hi", Streaming: true, CollectDetails: true}, 0)
	if err == nil {
		t.Fatalf("expected failure on error event; got res=%+v details=%q", res, res.Details)
	}
	if !strings.Contains(err.Error(), "upstream rate limited") {
		t.Errorf("error = %v", err)
	}
	if res == nil || !strings.Contains(res.Text, "half") {
		t.Errorf("delivered partials should survive: %+v", res)
	}
}

func TestRunNoResultExitZeroSucceeds(t *testing.T) {
	res, err := runScript(t, `IFS= read -r _ || true; exit 0`, RunOptions{Message: "hi", Streaming: true}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "" {
		t.Errorf("text = %q, want empty", res.Text)
	}
}

func TestRunNonZeroExitWithoutErrorEvent(t *testing.T) {
	_, err := runScript(t, `IFS= read -r _ || true; exit 3`, RunOptions{Message: "hi"}, 0)
	if err == nil || !strings.Contains(err.Error(), "code 3") {
		t.Fatalf("err = %v, want exit code 3", err)
	}
}

func TestRunSessionFirstNonEmptyWins(t *testing.T) {
	script := `IFS= read -r _ || true; printf '%s\n' '{"type":"partial","text":"a","session_id":"sess-1"}' '{"type":"result","text":"x","session_id":"sess-2"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.SessionID != "sess-1" {
		t.Errorf("session = %q, want first non-empty sess-1", res.SessionID)
	}
}

func TestRunSessionInboundPreservedWhenEventsSilent(t *testing.T) {
	res, err := runScript(t, `IFS= read -r _ || true; exit 0`, RunOptions{Message: "hi", SessionID: "inbound-1"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.SessionID != "inbound-1" {
		t.Errorf("session = %q, want inbound-1", res.SessionID)
	}
}

func TestRunInvalidSessionDropped(t *testing.T) {
	script := `IFS= read -r _ || true; printf '%s\n' '{"type":"result","text":"x","session_id":"a/b"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.SessionID != "" {
		t.Errorf("session = %q, want dropped (path separator)", res.SessionID)
	}
}

func TestRunMalformedLinesIgnored(t *testing.T) {
	script := `IFS= read -r _ || true; printf '%s\n' 'not json' '{"type":"what"}' '[]' '{"type":"result","text":"ok"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi"}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "ok" {
		t.Errorf("text = %q", res.Text)
	}
}

func TestRunUsageCaptured(t *testing.T) {
	script := `IFS= read -r _ || true; printf '%s\n' '{"type":"result","text":"x","usage":{"input_tokens":12,"output_tokens":34}}'`
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
	script := `IFS= read -r _ || true; printf '%s\n' '{"type":"partial","text":"a"}' '{"type":"result","text":"b"}'`
	res, err := runScript(t, script, RunOptions{Message: "hi", CollectDetails: true}, 0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Details) != 2 {
		t.Errorf("details = %v", res.Details)
	}
}

// TestRecursiveDirectDetailsCollected pins the plain path (#16): with
// CollectDetails the recursive-direct transcript lines are recorded — the
// stream-json NDJSON the observations parser consumes — while the trailing
// result object still assembles the reply.
func TestRecursiveDirectDetailsCollected(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "recbin")
	script := `#!/bin/sh
printf '%s\n' \
'{"type":"assistant","message":{"content":[{"type":"text","text":"step"}]}}' \
'{"type":"result","result":"ok reply","session_id":"rc-1"}'`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	spec := &AgentSpec{
		Name: "rec", Executor: ModeRecursiveDirect, Mode: ModeRecursiveDirect,
		Env: map[string]string{"RECURSIVE_BIN": bin},
	}
	res, err := Run(context.Background(), spec, RunOptions{Message: "hi", CollectDetails: true, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Text != "ok reply" {
		t.Errorf("text = %q", res.Text)
	}
	if len(res.Details) != 2 {
		t.Errorf("details = %v, want the 2 transcript lines", res.Details)
	}
}

// TestRecursiveDirectNoDetailsByDefault pins that Details stays empty unless
// CollectDetails is requested.
func TestRecursiveDirectNoDetailsByDefault(t *testing.T) {
	spec := &AgentSpec{
		Name: "rec", Executor: ModeRecursiveDirect, Mode: ModeRecursiveDirect,
		Env: map[string]string{"RECURSIVE_BIN": "/bin/sh", "RECURSIVE_WORKSPACE": "."},
	}
	// /bin/sh will fail on recursive's argv, but exit fast; the assertion is
	// on Details being empty in the returned (failed) result — use a script
	// that ignores argv instead so the run succeeds.
	spec.Env["RECURSIVE_BIN"] = "/bin/echo" // ignore args, prints nothing
	res, err := Run(context.Background(), spec, RunOptions{Message: "hi", Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Details) != 0 {
		t.Errorf("details = %v, want empty without CollectDetails", res.Details)
	}
}

func TestRunEmptyMessageRejected(t *testing.T) {
	spec := &AgentSpec{Name: "fake", Command: "/bin/sh", Args: []string{"-c", "exit 0"}}
	if _, err := Run(context.Background(), spec, RunOptions{Message: "  "}); err == nil {
		t.Fatal("empty turn must be rejected (spec: empty turns are an error)")
	}
}

// #66: after a terminal error event the bridge may keep streaming (the spec
// only says the host MAY stop reading) or keep running without exiting —
// Run must still return promptly. Pre-fix the host broke out of the read
// loop, the watchdog stood down on the closed done channel, and cmd.Wait()
// blocked forever, defeating both Timeout and the caller's ctx.
func TestRunErrorEventThenStreamReturns(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script string
	}{
		{"loud stream", `printf '%s\n' '{"type":"error","message":"boom"}'; yes hello`},
		{"silent hang", `printf '%s\n' '{"type":"error","message":"boom"}'; sleep 600`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pidFile := filepath.Join(t.TempDir(), "pid")
			script := fmt.Sprintf("echo $$ > %s; %s", pidFile, tc.script)
			spec := &AgentSpec{Name: "fake", Command: "/bin/sh", Args: []string{"-c", script}}
			start := time.Now()
			done := make(chan error, 1)
			var res *RunResult
			go func() {
				var err error
				res, err = Run(context.Background(), spec, RunOptions{Message: "hi", Streaming: true, Timeout: 2 * time.Second})
				done <- err
			}()
			select {
			case err := <-done:
				if elapsed := time.Since(start); elapsed > 3*time.Second {
					t.Errorf("Run returned in %s, want ≤3s", elapsed)
				}
				if err == nil || !strings.Contains(err.Error(), "error: boom") {
					t.Fatalf("err = %v, want error: boom", err)
				}
				if res == nil {
					t.Fatal("Run returned nil result; partials/session must survive the error")
				}
			case <-time.After(8 * time.Second):
				t.Fatal("Run did not return within 8s — unbounded wait after the error event (#66)")
			}
			assertBridgeGone(t, pidFile)
		})
	}
}

// #66 ctx variant: with no host timeout the caller's context must still
// bound the turn. The error event arrives first, so the message is the
// error's, but the invariant under test is bounded return.
func TestRunErrorEventThenCtxCancelReturns(t *testing.T) {
	spec := &AgentSpec{Name: "fake", Command: "/bin/sh", Args: []string{"-c",
		`printf '%s\n' '{"type":"error","message":"boom"}'; sleep 600`}}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := Run(ctx, spec, RunOptions{Message: "hi", Streaming: true})
		done <- err
	}()
	select {
	case err := <-done:
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("Run returned in %s, want ≤1s", elapsed)
		}
		if err == nil || !(strings.Contains(err.Error(), "error: boom") || strings.Contains(err.Error(), "canceled")) {
			t.Fatalf("err = %v, want error: boom or canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within 5s — ctx unenforced after the error event (#66)")
	}
}

// Conservation: a streaming writer WITHOUT an error event still goes through
// the watchdog — timeout text and prompt return are unchanged.
func TestRunTimeoutStillKillsStreamingWriter(t *testing.T) {
	start := time.Now()
	_, err := runScript(t, `yes hello`, RunOptions{Message: "hi"}, 300*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timeout after 300ms") {
		t.Fatalf("err = %v, want timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("timeout kill took %s", elapsed)
	}
}

// assertBridgeGone signals the recorded bridge pid with 0 — after the group
// kill and reap it must be gone (ESRCH).
func assertBridgeGone(t *testing.T, pidFile string) {
	t.Helper()
	pidBuf, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("pid file: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBuf)))
	if err != nil || pid <= 0 {
		t.Fatalf("bad pid %q", pidBuf)
	}
	if p, err := os.FindProcess(pid); err == nil {
		if err := p.Signal(syscall.Signal(0)); err == nil {
			t.Errorf("bridge pid %d still alive after Run returned", pid)
		}
	}
}
