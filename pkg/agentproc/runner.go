package agentproc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
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
	if spec == nil {
		return nil, fmt.Errorf("agentproc: nil agent spec")
	}

	args := spec.Args
	if spec.Mode == ModeRecursiveDirect {
		// native recursive-direct executor: argv is built per turn (the
		// message rides in argv — the spec's bounded exception), plain stdout
		built := buildRecursiveDirectArgs(spec.Env, opts.Message, opts.SessionID)
		spec.Command, args = built[0], built[1:]
	}
	if spec.Command == "" {
		return nil, fmt.Errorf("agentproc: agent spec has no command")
	}

	cmd := exec.Command(spec.Command, args...) // #nosec G204 — command comes from the operator's agents.json
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

	// Protocol path: exactly one turn line, then EOF (permission channel not
	// used by this host). The recursive-direct path carries the message in
	// argv and leaves stdin closed from the start.
	var writeErr error
	if spec.Mode != ModeRecursiveDirect {
		var turn []byte
		turn, writeErr = json.Marshal(map[string]any{
			"type":             "turn",
			"message":          opts.Message,
			"session_id":       opts.SessionID,
			"protocol_version": protocolVersion,
		})
		if writeErr == nil {
			_, writeErr = stdin.Write(append(turn, '\n'))
		}
	}
	if closeErr := stdin.Close(); writeErr == nil && !errors.Is(closeErr, os.ErrClosed) {
		// exec's Wait may close the write end first when the agent exits
		// immediately — that race is a successful close, not an error.
		writeErr = closeErr
	}
	if writeErr != nil {
		killGroup(cmd)
		_, _ = cmd.Process.Wait()
		return nil, fmt.Errorf("agentproc: write turn: %w", writeErr)
	}
	if spec.Mode == ModeRecursiveDirect {
		return readPlain(ctx, cmd, stdout, opts)
	}
	return readEvents(ctx, cmd, stdout, opts)
}

// streamGrace bounds the waits that follow the end of the stream (or a
// watchdog kill): how long the direct child may take to exit — and the pipe
// to drain — before the group is killed and the wait is abandoned. Bridges
// exit as soon as their stdout closes, so the ceiling only bites a bridge
// that hangs after its last byte, or a grandchild that escaped the process
// group and holds the write end; without it Run's bounded phases turn back
// into an unbounded one (#66).
const streamGrace = 5 * time.Second

func closeReader(stdout interface {
	Read([]byte) (int, error)
}) {
	if c, ok := stdout.(io.Closer); ok {
		_ = c.Close()
	}
}

// drainStream waits for the reader goroutine, but no longer than
// streamGrace. On expiry the read end is closed so a scanner blocked on a
// pipe that will never EOF (an escapee holding the write end) wakes up and
// the goroutine finishes; either way it returns only after done is closed,
// so everything the reader wrote is visible to the caller.
func drainStream(stdout interface {
	Read([]byte) (int, error)
}, done <-chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(streamGrace):
	}
	closeReader(stdout)
	<-done
}

// waitCommand reaps the child behind a streamGrace ceiling: a process still
// running after the stream is over gets killed, and a kill that somehow does
// not reap within another grace is reported instead of waited on. The bool
// reports that the ceiling fired — the caller prefers a named failure over
// interpreting whatever exit status the kill raced into.
func waitCommand(cmd *exec.Cmd) (graceKilled bool, waitErr error) {
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case err := <-waitDone:
		return false, err
	case <-time.After(streamGrace):
		killGroup(cmd)
		select {
		case err := <-waitDone:
			return true, err
		case <-time.After(streamGrace):
			return true, fmt.Errorf("process did not exit within %s of the group kill", 2*streamGrace)
		}
	}
}

// readPlain handles the plain stdout path (recursive-direct): accumulate
// everything, then extract the trailing JSON result object.
//
// Ordering matters: the stream must be drained to EOF BEFORE cmd.Wait() —
// exec closes the pipe's read end inside Wait, so reaping first can lose
// buffered output (documented os/exec pitfall).
func readPlain(ctx context.Context, cmd *exec.Cmd, stdout interface {
	Read([]byte) (int, error)
}, opts RunOptions) (*RunResult, error) {
	var body strings.Builder
	var details []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 64*1024), 32*1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			// details=true on this path means the recursive CLI was switched
			// to --output-format stream-json: the lines ARE the event stream
			// the observations parser consumes (plaita-nodes parity).
			if opts.CollectDetails {
				details = append(details, string(line))
			}
			body.Write(line)
			body.WriteByte('\n')
		}
	}()

	// watchdog only kills; the stream goroutine observes EOF and finishes
	if err := armWatchdog(ctx, cmd, opts.Timeout, done); err != nil {
		drainStream(stdout, done)
		_, _ = waitCommand(cmd)
		return nil, fmt.Errorf("agentproc: %s", err)
	}

	graceKilled, waitErr := waitCommand(cmd)

	res := &RunResult{SessionID: opts.SessionID, Details: details}
	obj := extractRecursiveResult(body.String())
	if v, ok := obj["session_id"].(string); ok && v != "" && res.SessionID == "" {
		res.SessionID = v
	}
	if v, ok := obj["usage"].(map[string]any); ok {
		res.Usage = v
	}
	if isErr, _ := obj["is_error"].(bool); isErr {
		msg := fmt.Sprint(obj["result"])
		return res, fmt.Errorf("agentproc: error: %s", msg)
	}
	res.Text = ResultText(obj)
	if raw, _ := obj["raw"].(bool); raw {
		res.Text = strings.TrimSpace(res.Text)
	}
	if graceKilled {
		return res, fmt.Errorf("agentproc: stream ended but the process did not exit within %s (process group killed)", streamGrace)
	}
	if waitErr != nil {
		if exitErr, ok := waitErr.(*exec.ExitError); ok && res.Text == "" {
			return res, fmt.Errorf("agentproc: agent exited with code %d", exitErr.ExitCode())
		}
	}
	return res, nil
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

func readEvents(ctx context.Context, cmd *exec.Cmd, stdout interface {
	Read([]byte) (int, error)
}, opts RunOptions) (*RunResult, error) {
	out := &turnOutcome{streaming: opts.Streaming, RunResult: RunResult{Details: []string{}}}
	if opts.SessionID != "" {
		out.SessionID = opts.SessionID
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
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
				// The bridge neither exits nor stops writing on our behalf,
				// so stop it here: an alive process would pin cmd.Wait()
				// below forever, defeating Timeout and the caller's ctx
				// (#66). failedReason is already recorded and outranks
				// whatever exit status the kill produces.
				killGroup(cmd)
				closeReader(stdout)
				break
			}
		}
	}()

	// watchdog only kills; the stream goroutine observes EOF and finishes.
	// Drain to EOF BEFORE cmd.Wait() — exec closes the read end inside Wait.
	if err := armWatchdog(ctx, cmd, opts.Timeout, done); err != nil {
		drainStream(stdout, done)
		_, _ = waitCommand(cmd)
		// a recorded terminal error outranks the watchdog's cleanup reason
		if out.failedReason != "" {
			return &out.RunResult, fmt.Errorf("agentproc: %s", out.failedReason)
		}
		return &out.RunResult, fmt.Errorf("agentproc: %s", err)
	}

	graceKilled, waitErr := waitCommand(cmd)

	switch {
	case out.failedReason != "":
		// terminal error event wins over the exit status
	case graceKilled:
		out.failedReason = fmt.Sprintf("stream ended but the process did not exit within %s (process group killed)", streamGrace)
	case waitErr != nil:
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

// armWatchdog kills the process group when the turn exceeds its timeout or
// the caller's context is canceled. It reports the kill reason as an error;
// nil means the reader goroutine finished first — stream EOF, a terminal
// error event's early exit, or a read failure. done is the reader's
// liveness, NOT proof the child exited or the pipe drained; the bounded
// drain/wait that follows covers what remains (#66).
func armWatchdog(ctx context.Context, cmd *exec.Cmd, timeout time.Duration, done <-chan struct{}) error {
	if timeout <= 0 {
		// fall back to a very long ceiling so ctx still governs
		timeout = time.Hour * 24
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var ctxDone <-chan struct{}
	if ctx != nil {
		ctxDone = ctx.Done()
	}
	if ctxDone == nil {
		ctxDone = make(chan struct{}) // never fires
	}
	select {
	case <-done:
		return nil // stream finished before anything bad happened
	case <-timer.C:
		killGroup(cmd)
		return fmt.Errorf("timeout after %s (process group killed)", timeout)
	case <-ctxDone:
		killGroup(cmd)
		return fmt.Errorf("canceled: %w", ctx.Err())
	}
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

// HostEnvAllowlist is the host environment an agent process inherits
// (plaita subprocess_env.SUBPROCESS_ENV_ALLOWLIST, the same public security
// layer the Python gate / capture / agent nodes use). Nothing else crosses
// into the child: the worker's provider keys, PLAITA_CREDENTIALS_KEY, DB /
// Redis DSNs and every other tenant's secrets stay in the parent. Variables
// the agent genuinely needs are declared explicitly in the agents.json
// entry's env (spec.Env), which overrides this list.
var HostEnvAllowlist = []string{
	"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE", "PYTHONIOENCODING",
}

// HostEnvExtra mirrors plaita's SUBPROCESS_ENV_EXTRA deployment hook: host
// variables every agent needs (e.g. HTTP(S)_PROXY) can be added at process
// start. It is merged after the allowlist and before spec.Env (spec wins).
// Only deployment bootstrap code should write it.
var HostEnvExtra = map[string]string{}

// composeEnv rebuilds the child environment from the allowlist, then the
// deployment extras, then the agent's explicit env (later wins) — the same
// merge order as plaita build_subprocess_env. The host process environment
// is never copied wholesale.
func composeEnv(extra map[string]string) []string {
	env := map[string]string{}
	for _, key := range HostEnvAllowlist {
		if v, ok := os.LookupEnv(key); ok {
			env[key] = v
		}
	}
	for k, v := range HostEnvExtra {
		env[k] = v
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
