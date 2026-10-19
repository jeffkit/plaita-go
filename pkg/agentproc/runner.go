package agentproc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// protocolVersion is the agentproc wire version this host speaks
// (spec/protocol.md v0.4). The turn object is opaque to this host — the
// bridge negotiates with the real CLI behind it.
const protocolVersion = "0.4"

// RunOptions configures one turn.
type RunOptions struct {
	// Message is the user message; mandatory (empty turns are a spec error).
	Message string
	// SessionID resumes an agent session ("" = new session).
	SessionID string
	// Dir is the agent's working directory ("" = inherit the parent's).
	Dir string
	// Timeout kills the process group when the turn exceeds it.
	// 0 = no host-side timeout (the caller's context still applies).
	Timeout time.Duration
	// Streaming controls reply assembly: with streaming (default) the body
	// is the concatenated partials and a result.text is NOT appended; with
	// streaming disabled partials are dropped and the body is result.text.
	Streaming bool
	// CollectDetails records every raw protocol line (agentrun details=true).
	CollectDetails bool
}

// RunResult is the assembled outcome of one turn.
type RunResult struct {
	// Text is the user-visible reply body (spec reply-assembly rules).
	Text string
	// SessionID is the first non-empty session id learned from events (or
	// the inbound one) — pass it back on the next turn.
	SessionID string
	// Usage is the terminal event's usage object, when present.
	Usage map[string]any
	// Details carries the raw protocol lines (CollectDetails only).
	Details []string
}

// Run spawns the agent bridge, delivers the turn, reads the NDJSON event
// stream and assembles the reply per the protocol spec:
//
//   - error event is terminal: the turn fails even if the process exits 0;
//   - streaming: body = concatenated partials; a non-empty result.text is
//     only used when no partial was seen (many CLIs repeat the full body in
//     their terminal event);
//   - non-streaming: body = result.text, partials dropped;
//   - a turn with no result and no partials that exits 0 succeeds with an
//     empty body;
//   - malformed lines are ignored;
//   - session ids violating the wire rules are dropped (first non-empty
//     valid one wins).
func Run(ctx context.Context, spec *AgentSpec, opts RunOptions) (*RunResult, error) {
	if strings.TrimSpace(opts.Message) == "" {
		return nil, fmt.Errorf("agentproc: empty turn (message is required)")
	}
	if spec == nil || spec.Command == "" {
		return nil, fmt.Errorf("agentproc: agent spec has no command")
	}

	cmd := exec.Command(spec.Command, spec.Args...) // #nosec G204 — command comes from the operator's agents.json
	cmd.Dir = opts.Dir
	cmd.Env = composeEnv(spec.Env)
	// Own process group so the timeout/cancel path can kill the agent AND
	// anything it spawned (the spec's bridges wrap real CLIs that fork).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("agentproc: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("agentproc: stdout pipe: %w", err)
	}
	cmd.Stderr = nil // bridges log to stderr; drop it (spec: warnings go to stderr)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("agentproc: spawn %q: %w", spec.Command, err)
	}

	// Exactly one turn line, then EOF (permission channel not used by this host).
	turn, err := json.Marshal(map[string]any{
		"type":             "turn",
		"message":          opts.Message,
		"session_id":       opts.SessionID,
		"protocol_version": protocolVersion,
	})
	if err == nil {
		_, err = stdin.Write(append(turn, '\n'))
	}
	if closeErr := stdin.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		killGroup(cmd)
		_, _ = cmd.Process.Wait()
		return nil, fmt.Errorf("agentproc: write turn: %w", err)
	}
	return readEvents(ctx, cmd, stdout, opts)
}

// turnOutcome accumulates the event-stream state machine.
type turnOutcome struct {
	RunResult
	failedReason   string
	sawPartial     bool
	finalText      *string
	streaming      bool
	terminalErrSet bool
}

func readEvents(ctx context.Context, cmd *exec.Cmd, stdout interface{ Read([]byte) (int, error) }, opts RunOptions) (*RunResult, error) {
	out := &turnOutcome{streaming: opts.Streaming, RunResult: RunResult{Details: []string{}}}
	if opts.SessionID != "" {
		out.SessionID = opts.SessionID
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if opts.CollectDetails {
				out.Details = append(out.Details, line)
			}
			out.consumeLine(line)
			if out.terminalErrSet {
				// spec: after an error the host may stop reading entirely.
				break
			}
		}
	}()

	timeout := opts.Timeout
	if timeout <= 0 {
		// fall back to a very long ceiling so ctx still governs
		timeout = time.Hour * 24
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	var waitErr error
	monitoring := true
	for monitoring {
		select {
		case waitErr = <-waitCh:
			monitoring = false
		case <-timer.C:
			killGroup(cmd)
			<-waitCh
			return &out.RunResult, fmt.Errorf("agentproc: timeout after %s (process group killed)", timeout)
		case <-ctx.Done():
			killGroup(cmd)
			<-waitCh
			return &out.RunResult, fmt.Errorf("agentproc: canceled: %w", ctx.Err())
		}
	}
	wg.Wait()

	if out.failedReason == "" && waitErr != nil {
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			out.failedReason = fmt.Sprintf("agent exited with code %d", exitErr.ExitCode())
		} else {
			out.failedReason = fmt.Sprintf("agent wait: %v", waitErr)
		}
	}
	out.assemble()
	if out.failedReason != "" {
		return &out.RunResult, fmt.Errorf("agentproc: %s", out.failedReason)
	}
	return &out.RunResult, nil
}

func (o *turnOutcome) consumeLine(line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(trimmed), &event); err != nil {
		return // malformed: spec says log-and-ignore
	}
	eventType, _ := event["type"].(string)
	o.learnSession(stringField(event, "session_id"))

	switch eventType {
	case "partial":
		if o.terminalErrSet {
			return // spec: future partials suppressed after error
		}
		if !o.streaming {
			return
		}
		text := stringField(event, "text")
		if text != "" {
			o.sawPartial = true
			o.Text += text
		}
	case "result":
		if o.finalText != nil {
			return // at most one result per turn
		}
		text := stringField(event, "text")
		o.finalText = &text
		if usage, ok := event["usage"].(map[string]any); ok && o.Usage == nil {
			o.Usage = usage
		}
	case "error":
		if o.terminalErrSet {
			return
		}
		msg := stringField(event, "message")
		o.terminalErrSet = true
		o.failedReason = "error: " + msg
		if usage, ok := event["usage"].(map[string]any); ok && o.Usage == nil {
			o.Usage = usage
		}
	default:
		// unknown type (incl. permission_request this host does not speak):
		// spec says ignore the line.
	}
}

// learnSession applies the first-non-empty-wins rule with the wire
// constraint (no path separators, no control characters).
func (o *turnOutcome) learnSession(id string) {
	if o.SessionID != "" || !validWireSessionID(id) {
		return
	}
	o.SessionID = id
}

func validWireSessionID(id string) bool {
	if id == "" {
		return false
	}
	for _, r := range id {
		if r == '/' || r == '\\' || r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// assemble applies the reply-assembly rules once the stream ended.
func (o *turnOutcome) assemble() {
	if o.failedReason != "" {
		return
	}
	if o.streaming && o.sawPartial {
		return // body already carried by partials; result.text not appended
	}
	if o.finalText != nil {
		o.Text = *o.finalText
	}
}

func killGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	if pgid := cmd.Process.Pid; pgid > 0 {
		// negative pid targets the process group (bridges fork real CLIs)
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
}

func composeEnv(extra map[string]string) []string {
	env := map[string]string{}
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}
	for k, v := range extra {
		env[k] = v
	}
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}
