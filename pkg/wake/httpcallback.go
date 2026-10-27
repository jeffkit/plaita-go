package wake

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/jeffkit/plaita-go/pkg/worker"
)

// HttpCallbackSource is the Go counterpart of Python plaita
// HttpCallbackService — with one addition: the HTTP ingress lives HERE (the
// Python service only exposes handle_callback_request as an in-process API;
// no server-side HTTP route exists in plaita today, so a pure-Go deployment
// gets its front door from plaita-wake).
//
// Faces:
//
//  1. Intake: consume `{prefix}:http_callback:queue` (the worker's §5.2
//     dispatch) → register `{prefix}:http_callback:registered:{path}`
//     (TTL 7d, shape byte-compatible with Python: {task_config,
//     registered_time}).
//  2. Ingress: an HTTP server (PLAITA_HTTP_CALLBACK_ADDR, default ":8080" —
//     the address the node's full_url points at) accepting POSTs on the
//     registered paths.
//
// Request handling mirrors Python handle_callback_request exactly:
//
//	1) read the record (no delete) → 2) verify credentials (auth_config from
//	the registration) → 3) atomic two-phase claim (Lua GET + SET NX PX) →
//	4) publish the resume event → 5) confirmed ⇒ DEL record + claim;
//	publish failure ⇒ DEL claim only (the record stays claimable, external
//	retries are NOT answered as unregistered — plaita#188/#35 family).
//
// Q5: unauthenticated callers get ONE response body for "unknown path",
// "claim race loser" and "bad credentials" (`回调路径未注册`) — three
// distinct messages would be a path-existence oracle. Auth details go to the
// log, never to the response.
type HttpCallbackSource struct {
	*listSource

	cfg          *Config
	pub          *worker.EventPublisher
	recordPrefix string
	claimPrefix  string
	recordTTL    time.Duration
	claimTTL     time.Duration
	sigTolerance time.Duration
	failureBody  map[string]any
	addr         string

	listener net.Listener
	server   *http.Server
}

const claimCallbackLua = `
local v = redis.call('get', KEYS[1])
if not v then return false end
if not redis.call('set', KEYS[2], '1', 'NX', 'PX', ARGV[1]) then
  return false
end
return v
`

func NewHttpCallbackSource(cfg *Config, rdb *redis.Client) (*HttpCallbackSource, error) {
	core := newListSource(SourceHTTPCallback, cfg, rdb)
	s := &HttpCallbackSource{
		listSource:    core,
		cfg:           cfg,
		pub:           worker.NewEventPublisher(rdb, &worker.Config{KeyPrefix: cfg.KeyPrefix, DefaultTenantID: cfg.DefaultTenantID, EventTTLSeconds: cfg.EventTTLSeconds}),
		recordPrefix:  cfg.KeyPrefix + ":http_callback:registered:",
		claimPrefix:   cfg.KeyPrefix + ":http_callback:claim:",
		recordTTL:     7 * 24 * time.Hour,
		claimTTL:      30 * time.Second,
		sigTolerance:  300 * time.Second,
		failureBody:   map[string]any{"status": "error", "message": "回调路径未注册"},
		addr:          callbackAddr(),
	}
	s.publishFn = s.pub.Publish
	core.validate = func(task map[string]any) bool {
		cc, _ := task["callback_config"].(map[string]any)
		return str(task["node_id"]) != "" && str(task["event_type"]) != "" &&
			cc != nil && str(cc["path"]) != ""
	}
	core.handle = s.registerCallback
	return s, nil
}

func callbackAddr() string {
	if v := os.Getenv("PLAITA_HTTP_CALLBACK_ADDR"); v != "" {
		return v
	}
	return ":8080"
}

// registerCallback stores the registration record (Python handle_task).
func (s *HttpCallbackSource) registerCallback(ctx context.Context, raw string, task map[string]any) bool {
	cc, _ := task["callback_config"].(map[string]any)
	path := str(cc["path"])
	record := map[string]any{
		"task_config":     task,
		"registered_time": time.Now().UnixMilli(),
	}
	blob, err := json.Marshal(record)
	if err != nil {
		s.logger.Error("http_callback record marshal failed", "path", path, "error", err)
		return true
	}
	if err := s.rdb.SetEx(ctx, s.recordPrefix+path, string(blob), s.recordTTL).Err(); err != nil {
		s.logger.Error("http_callback record write failed; entry kept for retry", "path", path, "error", err)
		return false
	}
	s.logger.Info("http_callback path registered", "path", path, "node", str(task["node_id"]))
	return true
}

// Run drives the consume loop (embedded) plus the HTTP ingress; both stop
// with ctx.
func (s *HttpCallbackSource) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("http_callback ingress listen %s: %w", s.addr, err)
	}
	s.listener = ln
	s.logger.Info("http_callback ingress listening", "addr", ln.Addr().String())
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.serveHTTP)
	s.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		if err := s.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("http_callback ingress serve failed", "error", err)
		}
	}()
	runErr := s.listSource.Run(ctx)
	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = s.server.Shutdown(shutCtx)
	<-srvDone
	return runErr
}

// BoundAddr reports the ingress listener address (":0" in tests).
func (s *HttpCallbackSource) BoundAddr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *HttpCallbackSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(s.failureBody)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
	if err != nil {
		_ = json.NewEncoder(w).Encode(s.failureBody)
		return
	}
	var requestData map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &requestData); err != nil || requestData == nil {
			// An unparsable body cannot match a signature over the parsed
			// form — same unified failure, no oracle.
			_ = json.NewEncoder(w).Encode(s.failureBody)
			return
		}
	}
	result := s.HandleCallbackRequest(r.Context(), r.URL.Path, requestData, r.Header)
	_ = json.NewEncoder(w).Encode(result)
}

// HandleCallbackRequest processes one external callback (Python
// handle_callback_request). Exported so hosts can front it with their own
// HTTP layer.
func (s *HttpCallbackSource) HandleCallbackRequest(ctx context.Context, path string, requestData map[string]any, headers http.Header) map[string]any {
	// 1) Read the record (no delete).
	raw, err := s.rdb.Get(ctx, s.recordPrefix+path).Result()
	if err != nil || raw == "" {
		return s.failureBody
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(raw), &record); err != nil || record == nil {
		return s.failureBody
	}
	taskCfg, _ := record["task_config"].(map[string]any)

	// 2) Verify credentials BEFORE the claim (#113: claiming first would let
	// one unauthorized request block the legitimate callback for the claim
	// TTL).
	if authErr := s.verifyAuth(taskCfg["auth_config"], requestData, headers); authErr != "" {
		s.logger.Warn("http_callback auth failed", "path", path, "detail", authErr)
		return s.failureBody
	}

	// 3) Atomic two-phase claim.
	claimCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	claimRes, err := s.rdb.Eval(claimCtx, claimCallbackLua,
		[]string{s.recordPrefix + path, s.claimPrefix + path},
		s.claimTTL.Milliseconds()).Result()
	if err != nil { // Lua false surfaces as redis.Nil — the claim race loser
		return s.failureBody
	}
	if _, ok := claimRes.(string); !ok {
		return s.failureBody
	}

	// 4) Publish the resume event.
	now := time.Now().UnixMilli()
	tenant := strOr(taskCfg["tenant_id"], s.cfg.DefaultTenantID)
	executionID := str(taskCfg["execution_id"])
	eventData := map[string]any{
		"node_id":       taskCfg["node_id"],
		"execution_id":  executionID,
		"flow_id":       taskCfg["flow_id"],
		"tenant_id":     tenant,
		"trigger_type":  "http_callback",
		"callback_path": path,
		"request_data":  requestData,
		"timestamp":     now,
		"success":       true,
	}
	ev := &worker.Event{
		EventID:       uuid.NewString(),
		EventType:     str(taskCfg["event_type"]),
		Data:          eventData,
		Timestamp:     float64(now),
		CorrelationID: executionID,
		TenantID:      tenant,
	}
	pubCtx, cancelPub := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPub()
	if err := s.publishFn(pubCtx, ev); err != nil {
		// 5b) Publish failed: release the claim only — the record stays
		// claimable so the provider's retry is not answered "unregistered".
		s.logger.Error("http_callback resume event publish failed; claim released, record claimable", "path", path, "error", err)
		_ = s.rdb.Del(context.Background(), s.claimPrefix+path).Err()
		return s.failureBody
	}
	// 5a) Published: record first, claim second (a crash in between must
	// never double-resume).
	_ = s.rdb.Del(context.Background(), s.recordPrefix+path).Err()
	_ = s.rdb.Del(context.Background(), s.claimPrefix+path).Err()
	s.logger.Info("http_callback processed", "path", path)

	rc, _ := taskCfg["response_config"].(map[string]any)
	if rc != nil {
		if sr, ok := rc["success_response"].(map[string]any); ok {
			return sr
		}
	}
	return map[string]any{"status": "success"}
}

// verifyAuth mirrors Python _verify_callback_auth: no auth_config (or neither
// enabled nor type) = no auth declared = allow (legacy records); anything
// else verifies strictly by type, unknown types fail closed. Returns "" on
// success, the log-only detail on refusal.
func (s *HttpCallbackSource) verifyAuth(authConfig any, requestData map[string]any, headers http.Header) string {
	ac, ok := authConfig.(map[string]any)
	if !ok || len(ac) == 0 {
		return ""
	}
	enabled := false
	if v, ok := ac["enabled"].(bool); ok && v {
		enabled = true
	}
	if _, ok := ac["type"]; !ok && !enabled {
		return ""
	}
	authType := str(ac["type"])
	switch authType {
	case "token":
		if !constEq(bearerToken(headers), str(ac["token"])) {
			return "认证失败：token 不匹配"
		}
		return ""
	case "basic":
		user, pass, ok := basicCredentials(headers)
		if !ok ||
			!constEq(user, str(ac["username"])) ||
			!constEq(pass, str(ac["password"])) {
			return "认证失败：basic 凭据不匹配"
		}
		return ""
	case "signature":
		return s.verifySignature(ac, requestData, headers)
	default:
		return fmt.Sprintf("认证失败：不支持的认证类型 %q", authType)
	}
}

// verifySignature checks the HMAC-SHA256 over
// `<unix-seconds>.<canonical-json(request_data)>` with the
// X-Plaita-Timestamp / X-Plaita-Signature headers (Python _verify_signature;
// the canonical form is the shared CPython json.dumps sort_keys encoder).
func (s *HttpCallbackSource) verifySignature(ac map[string]any, requestData map[string]any, headers http.Header) string {
	secret := str(ac["secret"])
	if secret == "" {
		return "认证失败：签名认证缺少 secret"
	}
	if !strings.EqualFold(str(ac["algorithm"]), "HMAC-SHA256") {
		return fmt.Sprintf("认证失败：不支持的签名算法 %q", str(ac["algorithm"]))
	}
	tsRaw := headers.Get("X-Plaita-Timestamp")
	sig := headers.Get("X-Plaita-Signature")
	if tsRaw == "" || sig == "" {
		return "认证失败：缺少 X-Plaita-Timestamp / X-Plaita-Signature"
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(tsRaw), 10, 64)
	if err != nil {
		return "认证失败：时间戳不是整数"
	}
	if abs64(time.Now().Unix()-ts) > int64(s.sigTolerance.Seconds()) {
		return "认证失败：签名时间戳超出允许窗口"
	}
	canonical, err := worker.CanonicalJSON(requestData)
	if err != nil {
		return "认证失败：签名原文无法构造"
	}
	payload := fmt.Sprintf("%d.%s", ts, canonical)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	expected := mac.Sum(nil)
	provided := strings.TrimSpace(sig)
	provided = strings.TrimPrefix(provided, "sha256=")
	provided = strings.TrimPrefix(provided, "SHA256=")
	got, err := hexDecode(provided)
	if err != nil {
		return "认证失败：签名不匹配"
	}
	if subtle.ConstantTimeCompare(got, expected) != 1 {
		return "认证失败：签名不匹配"
	}
	return ""
}

// ---- header credential helpers (Python _bearer_token / _basic_credentials) ----

func bearerToken(h http.Header) string {
	raw := h.Get("Authorization")
	if raw == "" {
		return ""
	}
	parts := strings.SplitN(raw, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func basicCredentials(h http.Header) (string, string, bool) {
	raw := h.Get("Authorization")
	if raw == "" {
		return "", "", false
	}
	parts := strings.SplitN(raw, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "basic") {
		return "", "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(parts[1]))
	if err != nil {
		return "", "", false
	}
	user, pass, found := strings.Cut(string(decoded), ":")
	if !found {
		return "", "", false
	}
	return user, pass, true
}

func constEq(a, b string) bool {
	if a == "" || b == "" {
		// Mirror Python: either side missing ⇒ mismatch (never equal).
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, errors.New("odd hex length")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi, ok1 := hexVal(s[2*i])
		lo, ok2 := hexVal(s[2*i+1])
		if !ok1 || !ok2 {
			return nil, errors.New("bad hex")
		}
		out[i] = hi<<4 | lo
	}
	return out, nil
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
