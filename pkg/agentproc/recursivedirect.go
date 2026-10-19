package agentproc

import (
	"encoding/json"
	"strings"
)

// ModeRecursiveDirect is the native Go executor mirroring plaita-nodes'
// "recursive-direct" (plain mode): spawn the recursive CLI with
// `--output-format json`, read the whole stdout, and extract the trailing
// JSON result object. Production (Python) runs the exact same semantics via
// its in-process executor, so both runtimes share one adapter contract.
const ModeRecursiveDirect = "recursive-direct"

// buildRecursiveDirectArgs mirrors plaita-nodes _make_recursive_handlers
// build_args: headless automation flags, provider/model via env, then
// `run <message>` for a new session or `resume <session> --message <m>` to
// continue one (L2 breakpoint resume).
func buildRecursiveDirectArgs(env map[string]string, message, sessionID string) []string {
	bin := envOr(env, "RECURSIVE_BIN", "recursive")
	args := []string{
		bin,
		"--workspace", envOr(env, "RECURSIVE_WORKSPACE", "."),
		"--output-format", envOr(env, "RECURSIVE_OUTPUT_FORMAT", "json"),
		// headless: no permission bridge — explicit auto mode, otherwise a
		// tool call would emit a control_request and hang until timeout
		"--permission-mode", "auto",
	}
	if v := envOr(env, "RECURSIVE_MODEL", ""); v != "" {
		args = append(args, "--model", v)
	}
	if v := envOr(env, "RECURSIVE_MAX_STEPS", ""); v != "" {
		args = append(args, "--max-steps", v)
	}
	if sessionID != "" {
		args = append(args, "resume", sessionID, "--message", message)
	} else {
		args = append(args, "run", message)
	}
	return args
}

func envOr(env map[string]string, key, def string) string {
	if v, ok := env[key]; ok && v != "" {
		return v
	}
	return def
}

// extractRecursiveResult ports plaita-nodes extract_recursive_result: pull
// the trailing JSON result object out of recursive's `--output-format json`
// stdout. Strategy: line-start `{` objects scanned last-first (the result
// object closes the stream; log noise appears mid-line), falling back to the
// first `{"` with brace matching that skips string literals; raw text as a
// last resort.
func extractRecursiveResult(stdout string) map[string]any {
	var lineStarts []int
	offset := 0
	for _, line := range strings.Split(stdout, "\n") {
		if strings.HasPrefix(strings.TrimLeft(line, " \t"), "{") {
			lineStarts = append(lineStarts, offset)
		}
		offset += len(line) + 1
	}

	scanOrder := make([]int, 0, len(lineStarts)+1)
	for i := len(lineStarts) - 1; i >= 0; i-- {
		scanOrder = append(scanOrder, lineStarts[i])
	}
	if idx := strings.Index(stdout, `{"`); idx >= 0 {
		scanOrder = append(scanOrder, idx)
	}

	for _, start := range scanOrder {
		if obj, ok := balancedJSONAt(stdout, start); ok {
			return obj
		}
	}
	return map[string]any{"result": stdout, "is_error": false, "raw": true}
}

// balancedJSONAt decodes the JSON object whose `{` sits exactly at start
// (brace matching honouring string escapes). ok=false when no balanced
// object starts there.
func balancedJSONAt(s string, start int) (map[string]any, bool) {
	if start >= len(s) || s[start] != '{' {
		return nil, false
	}
	depth, inString, escaped := 0, false, false
	for i := start; i < len(s); i++ {
		ch := s[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case ch == '\\':
				escaped = true
			case ch == '"':
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				obj, err := jsonUnmarshalObject(s[start : i+1])
				if err != nil {
					return nil, false
				}
				return obj, true
			}
		}
	}
	return nil, false
}

// jsonUnmarshalObject decodes a JSON object string.
func jsonUnmarshalObject(s string) (map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal([]byte(s), &obj); err != nil {
		return nil, err
	}
	return obj, nil
}
