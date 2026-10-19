package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/pkg/expression"
)

// hitl-server protocol shared layer, ported from plaita-nodes
// _hitl_client.py (the client is shared by the blocking HitlNode and the
// suspended HitlAwaitNode; this engine only ports the blocking variant).
//
// Protocol (iLink channel, mirrors mediaflow scripts/hitl.js):
//   - POST {base}/api/send  body {message, wait_reply, timeout, upstream:'ilink'}
//     → {success, session_id, error}
//   - GET  {base}/api/poll/{session_id} → {has_reply, replies:[{content|text}], status}
//
// Images: a wait_reply=false probe message goes out first; on failure the
// degradation notice is appended to the body (hitl.js parity).
//
// Credential red line: the server address comes from the base_url field,
// env HITL_URL, or the loopback default — exactly like Python. No auth
// material lives in this repository; hosts inject credentials via env.

const (
	// hitlDefaultBaseURL mirrors _hitl_client.DEFAULT_BASE_URL.
	hitlDefaultBaseURL = "http://127.0.0.1:8081"
	// hitlDefaultTimeoutSecs mirrors _hitl_client.DEFAULT_TIMEOUT_SECS.
	hitlDefaultTimeoutSecs = 3600
	// hitlDefaultPollIntervalSecs mirrors HitlNode.poll_interval's default.
	hitlDefaultPollIntervalSecs = 5.0

	// per-request deadlines, mirroring the Python requests timeouts:
	// probe 15s, main send 30s, poll capped at 15s (see hitlPollTimeout).
	hitlProbeTimeout = 15 * time.Second
	hitlSendTimeout  = 30 * time.Second
	hitlPollCapSecs  = 15.0
	// hitlPollFloorSecs: a request deadline must be positive (A2R8-B7 keeps
	// the poll inside the remaining deadline; the floor mirrors Python's
	// max(0.05, ...) which requests requires to be > 0).
	hitlPollFloorSecs = 0.05

	// hitlImageFallbackNote is appended to the message when the image probe
	// fails (verbatim from _hitl_client.send_message).
	hitlImageFallbackNote = "\n\n⚠️ 封面图发送失败（媒体窗口过期）：请先给 bot 发任意一条消息后说\"重发图\"。"
)

// hitlHTTPClient serves every hitl-server call; each request carries its own
// context deadline (probe/send/poll above), so no client-wide timeout is set.
var hitlHTTPClient = &http.Client{}

// HitlError mirrors plaita-nodes HitlError: the hitl-server explicitly
// rejected the operation (send success=false, poll status=error). Transport
// failures (connection refused, deadlines) surface as plain wrapped errors,
// letting the engine's retry wiring tell deterministic rejections from
// transient faults by type.
type HitlError struct{ msg string }

func (e *HitlError) Error() string { return e.msg }

// hitlResolveBaseURL ports _hitl_client.resolve_base_url: the base_url field
// (expression, truthiness-checked like Python's `if base_url:`) wins over
// env HITL_URL, which wins over the loopback default. Trailing slashes are
// stripped on every branch.
func hitlResolveBaseURL(ctx context.Context, id string, baseURL any) (string, error) {
	if isTruthy(baseURL) {
		ev, err := expression.Eval(ctx, baseURL)
		if err != nil {
			return "", fmt.Errorf("hitl %q: eval base_url: %w", id, err)
		}
		return strings.TrimRight(gateToStr(ev), "/"), nil
	}
	if v, ok := os.LookupEnv("HITL_URL"); ok {
		return strings.TrimRight(v, "/"), nil
	}
	return hitlDefaultBaseURL, nil
}

// hitlResolveTimeoutSecs ports _hitl_client.resolve_timeout_secs: the
// timeout_secs expression evaluates to a Python-int-like value; anything
// int() would reject falls back to the 3600s default.
func hitlResolveTimeoutSecs(ctx context.Context, id string, timeoutSecs any) (int, error) {
	if timeoutSecs == nil {
		return hitlDefaultTimeoutSecs, nil
	}
	ev, err := expression.Eval(ctx, timeoutSecs)
	if err != nil {
		return 0, fmt.Errorf("hitl %q: eval timeout_secs: %w", id, err)
	}
	if n, ok := hitlToInt(ev); ok {
		return n, nil
	}
	return hitlDefaultTimeoutSecs, nil
}

// hitlToInt approximates Python int() over the JSON scalar domain: bools are
// 1/0, integral strings parse (whitespace tolerated), floats truncate via
// toInt. Everything else reports failure so the caller can fall back.
func hitlToInt(v any) (int, bool) {
	switch t := v.(type) {
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		return n, err == nil
	default:
		n, err := toInt(v)
		return n, err == nil
	}
}

// hitlSendMessage ports _hitl_client.send_message: optional image probe with
// graceful degradation, then the main send. Returns the session_id. A
// server-rejected send (success=false) is a *HitlError; transport/decode
// failures are plain errors (Python lets requests exceptions propagate).
func hitlSendMessage(ctx context.Context, base, message string, images []string, timeoutSecs int, waitReply bool) (string, error) {
	if len(images) > 0 {
		imageFailed := false
		probe, err := hitlPostJSON(ctx, base+"/api/send", map[string]any{
			"message":    "（图片见下）",
			"images":     images,
			"wait_reply": false,
			"upstream":   "ilink",
		}, hitlProbeTimeout)
		if err != nil {
			imageFailed = true
		} else if !isTruthy(probe["success"]) {
			imageFailed = true
		}
		if imageFailed {
			message += hitlImageFallbackNote
		}
	}

	payload, err := hitlPostJSON(ctx, base+"/api/send", map[string]any{
		"message":    message,
		"wait_reply": waitReply,
		"timeout":    timeoutSecs,
		"upstream":   "ilink",
	}, hitlSendTimeout)
	if err != nil {
		return "", err
	}
	if !isTruthy(payload["success"]) {
		detail := any(payload)
		if e := payload["error"]; isTruthy(e) {
			detail = e
		}
		return "", &HitlError{msg: fmt.Sprintf("hitl-server 发送失败: %v", detail)}
	}
	if sid := payload["session_id"]; isTruthy(sid) {
		return gateToStr(sid), nil
	}
	return "", nil
}

// hitlPollReply performs one GET /api/poll/{session_id} and decodes the JSON
// body. Like Python's requests.get(...).json(), there is no HTTP status
// check — a non-JSON body is a decode error, and transport failures propagate
// to the node (they are not HitlError).
func hitlPollReply(ctx context.Context, base, sessionID string, pollTimeout time.Duration) (map[string]any, error) {
	reqCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, base+"/api/poll/"+sessionID, nil)
	if err != nil {
		return nil, err
	}
	resp, err := hitlHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("hitl-server 轮询响应不是 JSON: %w", err)
	}
	return payload, nil
}

// hitlPostJSON POSTs a JSON body and decodes the JSON response.
func hitlPostJSON(ctx context.Context, url string, body any, timeout time.Duration) (map[string]any, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := hitlHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("hitl-server 响应不是 JSON: %w", err)
	}
	return payload, nil
}

// hitlPollTimeout bounds a single poll GET: never past the node's own
// deadline, capped at 15s, floored at 0.05s (A2R8-B7).
func hitlPollTimeout(deadline time.Time) time.Duration {
	remaining := time.Until(deadline).Seconds()
	if remaining > hitlPollCapSecs {
		remaining = hitlPollCapSecs
	}
	if remaining < hitlPollFloorSecs {
		remaining = hitlPollFloorSecs
	}
	return time.Duration(remaining * float64(time.Second))
}
