// Command plaita runs plaita-go flows inline (the 直跑/inline execution face,
// Python Flow.run() / plaita-rs `plaita-rs run` counterpart). Managed
// execution stays with cmd/plaita-worker: this binary never touches Redis.
//
//	plaita run -flow examples/hello_world.json [-input '{"x":1}'] [-input-file in.json]
//	plaita validate -flow flow.json
//
// Inline semantics (aligned with the Python two-tier model, ADR-001 /
// docs/governance/execution-capabilities.md):
//
//   - Execution tier gate (stage=entry): a flow carrying managed-tier nodes
//     (the suspending family — event/hitl_await/delay/approval/http_callback/
//     redis_queue/kafka_queue) is refused before execution with the stable
//     machine anchor error_type="suspend_not_executable" and the offending
//     nodes named (报缺不报档: name the nodes and the fix, not a tier name).
//     Inline would park them forever — no wakeup source exists in-process.
//   - Execution budget: PLAITA_EXECUTION_DEADLINE_SECONDS (the same env the
//     worker uses, pkg/worker.ExecutionDeadlineEnv) caps one engine call,
//     default 1800s. An invalid or ≤0 value falls back to the default with a
//     warning — never silently unlimited. A deadline hit is terminalized with
//     error_type="execution_deadline_exceeded" (plaita#267 vocabulary).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/ir"
	"github.com/jeffkit/plaita-go/pkg/worker"
	"github.com/jeffkit/plaita-go/pkg/workflow"
	"github.com/jeffkit/plaita-go/pkg/workflow/nodes" // node registry + suspend scan (init)
)

const (
	// Stable cross-runtime anchors (Python #317/#331: the string is the
	// machine-checkable contract, not the message text).
	errorSuspendNotExecutable      = "suspend_not_executable"
	errorExecutionDeadlineExceeded = "execution_deadline_exceeded"
)

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr))
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: plaita <command> [flags]

commands:
  run      execute a flow inline (no Redis, no queue)
             -flow <path>         deployment-IR flow JSON (required)
             -input <json>        flow input as a JSON literal
             -input-file <path>   flow input read from a JSON file
             -execution-id <id>   execution id (default: generated)
  validate load-check a flow (normalize + validate, no execution)
             -flow <path>         deployment-IR flow JSON (required)

environment:
  PLAITA_EXECUTION_DEADLINE_SECONDS   inline execution budget, default 1800s
`)
}

func runCLI(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "run":
		return runInline(args[1:], stdout, stderr)
	case "validate":
		return runValidate(args[1:], stdout, stderr)
	case "version", "-version", "--version":
		fmt.Fprintln(stdout, "plaita (plaita-go inline runner)")
		return 0
	case "-h", "-help", "--help":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "error: unknown command %q\n\n", args[0])
		usage(stderr)
		return 2
	}
}

// emitError writes one structured error line to stderr. errorType, when
// non-empty, carries a stable cross-runtime anchor; stage marks where the
// refusal happened (Python #331 vocabulary: entry = before execution).
func emitError(stderr io.Writer, errorType, stage, message string, extra map[string]any) {
	body := map[string]any{"error": message}
	if errorType != "" {
		body["error_type"] = errorType
	}
	if stage != "" {
		body["stage"] = stage
	}
	for k, v := range extra {
		body[k] = v
	}
	blob, err := json.Marshal(body)
	if err != nil {
		fmt.Fprintf(stderr, "error: %s\n", message)
		return
	}
	fmt.Fprintln(stderr, string(blob))
}

func loadFlaggedFlow(fs *flag.FlagSet) (*workflow.Workflow, error) {
	path := fs.Arg(0)
	if p := fs.Lookup("flow"); p != nil {
		path = p.Value.String()
	}
	if path == "" {
		return nil, fmt.Errorf("-flow is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ir.Load(raw)
}

func runValidate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.String("flow", "", "deployment-IR flow JSON (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	wf, err := loadFlaggedFlow(fs)
	if err != nil {
		emitError(stderr, "", "load", err.Error(), nil)
		return 1
	}
	return emitJSON(stdout, map[string]any{
		"ok":      true,
		"flow_id": string(wf.ID),
		"nodes":   len(wf.Nodes),
	})
}

func runInline(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	flowPath := fs.String("flow", "", "deployment-IR flow JSON (required)")
	inputJSON := fs.String("input", "", "flow input as a JSON literal")
	inputFile := fs.String("input-file", "", "flow input read from a JSON file")
	executionID := fs.String("execution-id", "", "execution id (default: generated)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *flowPath == "" && fs.NArg() > 0 {
		*flowPath = fs.Arg(0)
	}
	if *flowPath == "" {
		fmt.Fprintln(stderr, "error: -flow is required")
		return 2
	}
	raw, err := os.ReadFile(*flowPath)
	if err != nil {
		emitError(stderr, "", "load", err.Error(), nil)
		return 1
	}
	wf, err := ir.Load(raw)
	if err != nil {
		emitError(stderr, "", "load", err.Error(), nil)
		return 1
	}

	// Execution tier gate (stage=entry): inline has no wakeup source, so a
	// managed-tier (suspending) node would park forever. Refuse before
	// execution with the stable anchor + the named nodes.
	if refs := nodes.FlowSuspendingNodes(wf); len(refs) > 0 {
		list := make([]map[string]string, 0, len(refs))
		names := make([]string, 0, len(refs))
		for _, r := range refs {
			list = append(list, map[string]string{"id": r.NodeID, "type": r.NodeType})
			names = append(names, fmt.Sprintf("%s(%s)", r.NodeID, r.NodeType))
		}
		emitError(stderr, errorSuspendNotExecutable, "entry",
			fmt.Sprintf("inline execution cannot run suspending nodes: %s would park without a wakeup source; run on a managed host (plaita-worker with wake services) or rewrite the flow",
				string(joinCommas(names))),
			map[string]any{"nodes": list})
		return 1
	}

	input, err := parseInput(*inputJSON, *inputFile)
	if err != nil {
		emitError(stderr, "", "input", err.Error(), nil)
		return 1
	}

	deadline := executionDeadline(stderr)
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	execID := *executionID
	if execID == "" {
		execID = uuid.NewString()
	}
	ctx = runtime.WithExecutionID(ctx, execID)

	result, execErr := wf.Execute(ctx, input)
	if execErr != nil {
		if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			emitError(stderr, errorExecutionDeadlineExceeded, "run",
				"inline execution exceeded its budget", map[string]any{
					"deadline_seconds": int(deadline.Seconds()),
				})
			return 1
		}
		emitError(stderr, "", "run", execErr.Error(), nil)
		return 1
	}
	return emitJSON(stdout, result)
}

// parseInput resolves the flow input from -input / -input-file (file wins on
// conflict is NOT silent: both set is an error). Empty = nil input.
func parseInput(literal, file string) (any, error) {
	switch {
	case literal != "" && file != "":
		return nil, fmt.Errorf("-input and -input-file are mutually exclusive")
	case file != "":
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("parse -input-file: %w", err)
		}
		return v, nil
	case literal != "":
		var v any
		if err := json.Unmarshal([]byte(literal), &v); err != nil {
			return nil, fmt.Errorf("parse -input: %w", err)
		}
		return v, nil
	default:
		return nil, nil
	}
}

// executionDeadline mirrors the worker's budget knob (worker.ExecutionDeadlineEnv /
// worker.DefaultExecutionDeadline): invalid or ≤0 falls back to the default
// with a warning — never silently unlimited. The inline face keeps the
// default always-on, like the Python/RS inline faces.
func executionDeadline(stderr io.Writer) time.Duration {
	raw := os.Getenv(worker.ExecutionDeadlineEnv)
	if raw == "" {
		return worker.DefaultExecutionDeadline
	}
	secs, err := strconv.Atoi(raw)
	if err != nil || secs <= 0 {
		fmt.Fprintf(stderr, "warning: %s=%q is invalid or non-positive; falling back to default %s\n",
			worker.ExecutionDeadlineEnv, raw, worker.DefaultExecutionDeadline)
		return worker.DefaultExecutionDeadline
	}
	return time.Duration(secs) * time.Second
}

func emitJSON(w io.Writer, v any) int {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		// Non-JSON-serializable results fall back to their printed form.
		fmt.Fprintf(w, "%v\n", v)
	}
	return 0
}

func joinCommas(parts []string) []byte {
	buf := make([]byte, 0, 64)
	for i, p := range parts {
		if i > 0 {
			buf = append(buf, ',', ' ')
		}
		buf = append(buf, p...)
	}
	return buf
}
