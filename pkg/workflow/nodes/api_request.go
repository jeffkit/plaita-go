package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// nodeTypeAPIRequest mirrors plaita-nodes ApiRequestNode.node_type ("api_request").
const nodeTypeAPIRequest types.NodeType = "api_request"

const (
	apiRequestDefaultTimeoutMs = 15000
	apiRequestMaxRedirects     = 10
	// resp.text[:2000] fallback when the body is not JSON (api.py:233).
	apiRequestBodyTextLimit = 2000
	// str(data)[:200] in the raise_on_error detail (api.py:236).
	apiRequestErrorDetailLimit = 200
)

// apiRequestRedirectStatuses mirrors _REDIRECT_STATUSES (api.py:27).
var apiRequestRedirectStatuses = map[int]bool{301: true, 302: true, 303: true, 307: true, 308: true}

// apiRequestBodyHeaders mirrors _BODY_HEADERS (api.py:30): headers that must
// be dropped together with a request body lost to a redirect.
var apiRequestBodyHeaders = []string{"Content-Length", "Content-Type", "Transfer-Encoding"}

// apiRequestTokenRe mirrors _TOKEN_RE (api.py:25): $INPUT/$NODE/$GLOBAL
// dotted tokens interpolated inside mixed text such as URL paths.
var apiRequestTokenRe = regexp.MustCompile(`\$(?:INPUT|NODE|GLOBAL)(?:\.\w+)+`)

// APIRequestCredentialResolver resolves a credential name into its data dict
// (the plaita console credential store equivalent: {"base_url": ..., "headers":
// {...}} for api_request). plaita-go deliberately has no encrypted credential
// store of its own — the console side stays Python — so the engine takes the
// resolver as an injection point and fails closed when none is configured.
type APIRequestCredentialResolver func(ctx context.Context, name string) (map[string]any, error)

// apiRequestCredentialStore is the process-wide default resolver; deployers
// wire it via SetAPIRequestCredentialStore at startup.
var apiRequestCredentialStore APIRequestCredentialResolver

// SetAPIRequestCredentialStore installs the process-wide credential resolver
// (deployment-time hook, mirroring plaita's get_credential wiring).
func SetAPIRequestCredentialStore(r APIRequestCredentialResolver) {
	apiRequestCredentialStore = r
}

// APIRequest is the REST connector node mirroring plaita-nodes
// ApiRequestNode (api.py:167): the credential provides base_url + static
// auth headers, the node fields describe the request itself.
//
// Redirect semantics (api.py:46-100): redirects are NOT followed unless
// allow_redirects is set; when following, cross-origin hops strip ALL
// headers (requests only strips Authorization — custom auth headers would
// leak to the redirect target, pn#11), 303/302 rewrite non-HEAD to GET,
// 301 rewrites POST to GET, bodies are dropped on 301/302/303 (and on
// 307/308 once the method became GET/HEAD), and query params are not
// re-appended after the first hop.
type APIRequest struct {
	BaseNode
	Credential     string `json:"credential,omitempty"`
	Method         string `json:"method,omitempty"`
	Path           string `json:"path,omitempty"`
	Query          any    `json:"query,omitempty"`           // expression, dict
	Body           any    `json:"body,omitempty"`            // expression, any JSON value
	TimeoutMs      any    `json:"timeout_ms,omitempty"`      // literal positive number (fail-closed at validate/execute)
	RaiseOnError   *bool  `json:"raise_on_error,omitempty"`  // default true
	AllowRedirects bool   `json:"allow_redirects,omitempty"` // default false
	DryRun         bool   `json:"dry_run,omitempty"`

	// CredentialResolver overrides the process-wide store for this node
	// (tests inject httptest servers through it).
	CredentialResolver APIRequestCredentialResolver `json:"-"`
}

func init() {
	workflow.RegisterNodeType(nodeTypeAPIRequest, func(ctx context.Context) workflow.Node { return new(APIRequest) })
}

// GetType returns the node type.
func (a *APIRequest) GetType() workflow.NodeType { return nodeTypeAPIRequest }

// Validate checks the build-time contract: timeout_ms must be a positive
// number when set (mirrors the pydantic positive_millis validator, A2R7-05).
func (a *APIRequest) Validate() error {
	if err := a.BaseNode.Validate(); err != nil {
		return err
	}
	if a.TimeoutMs != nil {
		if _, err := apiRequestTimeoutMs(a.TimeoutMs); err != nil {
			return fmt.Errorf("api_request %q: %w", a.ID, err)
		}
	}
	return nil
}

// Execute evaluates the expression fields and performs the request.
func (a *APIRequest) Execute(ctx context.Context) (*workflow.ExecuteResult, error) {
	// dry-run first: no credential resolution, no request (api.py:204).
	if a.DryRun || apiRequestGlobalDryRun(ctx) {
		return &workflow.ExecuteResult{Result: map[string]any{
			"status": nil, "data": "[dry-run] would request", "dry_run": true,
		}, NextNode: a.NextNode()}, nil
	}

	if a.Credential == "" {
		return nil, fmt.Errorf("api_request %q: missing credential field", a.ID)
	}
	cred, err := apiRequestResolveCredential(ctx, a)
	if err != nil {
		return nil, err
	}
	baseURL := strings.TrimRight(apiRequestCredentialString(cred, "base_url"), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("api_request %q: credential %q is missing the base_url field", a.ID, a.Credential)
	}
	headers, err := apiRequestCredentialHeaders(cred)
	if err != nil {
		return nil, fmt.Errorf("api_request %q: %w", a.ID, err)
	}

	path := strings.TrimLeft(apiRequestInterpolate(ctx, a.Path), "/")
	rawURL := baseURL
	if path != "" {
		rawURL = baseURL + "/" + path
	}
	method := strings.ToUpper(a.Method)
	if method == "" {
		method = http.MethodGet
	}

	var params url.Values
	if a.Query != nil {
		ev, err := expression.Eval(ctx, a.Query)
		if err != nil {
			return nil, fmt.Errorf("api_request %q: eval query: %w", a.ID, err)
		}
		if params, err = apiRequestParamsOf(ev); err != nil {
			return nil, fmt.Errorf("api_request %q: %w", a.ID, err)
		}
	}

	var body any
	if a.Body != nil {
		body, err = expression.Eval(ctx, a.Body)
		if err != nil {
			return nil, fmt.Errorf("api_request %q: eval body: %w", a.ID, err)
		}
	}
	if method == http.MethodGet || method == http.MethodHead {
		body = nil
	}

	timeoutMs := apiRequestDefaultTimeoutMs
	if a.TimeoutMs != nil {
		v, err := apiRequestTimeoutMs(a.TimeoutMs)
		if err != nil {
			return nil, fmt.Errorf("api_request %q: %w", a.ID, err)
		}
		timeoutMs = v
	}
	// timeout_ms maps to Client.Timeout (per-request budget, retryable on
	// expiry — mirrors requests' timeout= being a RequestException); the
	// caller's ctx stays the engine-level cancellation path.
	client := &http.Client{
		Timeout: time.Duration(timeoutMs) * time.Millisecond,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse // never auto-follow; the loop below decides
		},
	}

	resp, err := apiRequestPerform(ctx, client, method, rawURL, headers, params, body, a.AllowRedirects)
	if err != nil {
		return nil, fmt.Errorf("api_request %q: %w", a.ID, err)
	}

	var data any
	if err := json.Unmarshal(resp.body, &data); err != nil {
		data = apiRequestTruncate(string(resp.body), apiRequestBodyTextLimit)
	}
	if resp.statusCode >= 400 && (a.RaiseOnError == nil || *a.RaiseOnError) {
		detail := apiRequestScrubText(fmt.Sprintf("API 请求失败 HTTP %d: %s",
			resp.statusCode, apiRequestTruncate(apiRequestDataString(data), apiRequestErrorDetailLimit)))
		if apiRequestIsRetryableStatus(resp.statusCode) {
			return nil, workflow.RetryableError(errors.New(detail))
		}
		// Deterministic 4xx: not re-sent by retryTimes (A2R7-04).
		return nil, errors.New(detail)
	}
	return &workflow.ExecuteResult{Result: map[string]any{
		"status": resp.statusCode, "data": data,
	}, NextNode: a.NextNode()}, nil
}

type apiRequestResponse struct {
	statusCode int
	body       []byte
}

// apiRequestPerform mirrors _perform_request (api.py:46-100).
func apiRequestPerform(ctx context.Context, client *http.Client, method, rawURL string,
	headers http.Header, params url.Values, body any, allowRedirects bool,
) (*apiRequestResponse, error) {
	currentURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse url %q: %w", apiRequestScrubText(rawURL), err)
	}
	currentHeaders := headers.Clone()
	currentParams := params
	currentBody := body
	hops := 0
	for {
		reqURL := *currentURL
		if len(currentParams) > 0 {
			q := reqURL.Query()
			for k, vs := range currentParams {
				for _, v := range vs {
					q.Add(k, v)
				}
			}
			reqURL.RawQuery = q.Encode()
		}
		var reader io.Reader
		if currentBody != nil {
			encoded, err := json.Marshal(currentBody)
			if err != nil {
				return nil, fmt.Errorf("marshal body: %w", err)
			}
			reader = bytes.NewReader(encoded)
		}
		req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), reader)
		if err != nil {
			return nil, fmt.Errorf("new request: %w", err)
		}
		req.Header = currentHeaders.Clone()
		// requests' json= kwarg sets Content-Type only when absent.
		if currentBody != nil && req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := client.Do(req)
		if err != nil {
			// requests.RequestException → RuntimeError → retryable (A2R7-04).
			return nil, workflow.RetryableError(fmt.Errorf("transport error (%s): %w",
				apiRequestScrubText(reqURL.String()), err))
		}
		if !allowRedirects || !apiRequestRedirectStatuses[resp.StatusCode] {
			return apiRequestReadResponse(resp)
		}
		location := resp.Header.Get("Location")
		if location == "" {
			return apiRequestReadResponse(resp)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()

		hops++
		if hops > apiRequestMaxRedirects {
			return nil, workflow.RetryableError(fmt.Errorf("API 请求重定向超过 %d 次，已中止", apiRequestMaxRedirects))
		}
		target, err := req.URL.Parse(location)
		if err != nil {
			return nil, workflow.RetryableError(fmt.Errorf("resolve redirect location %q: %w", location, err))
		}
		// Cross-origin hop (scheme/host/port any differs) strips ALL headers;
		// same-origin keeps credentials; once cross-origin, later hops do not
		// re-add them (pn#11).
		if !apiRequestSameOrigin(target, req.URL) {
			currentHeaders = http.Header{}
		}
		// Redirect method semantics (api.py:86-91): 303/302 rewrite non-HEAD
		// to GET; 301 rewrites only POST.
		switch {
		case resp.StatusCode == 303 && method != http.MethodHead:
			method = http.MethodGet
		case resp.StatusCode == 302 && method != http.MethodHead:
			method = http.MethodGet
		case resp.StatusCode == 301 && method == http.MethodPost:
			method = http.MethodGet
		}
		// 301/302/303 always drop the body; 307/308 keep it unless the method
		// became GET/HEAD (api.py:92-93).
		if (resp.StatusCode != 307 && resp.StatusCode != 308) ||
			method == http.MethodGet || method == http.MethodHead {
			currentBody = nil
		}
		if currentBody == nil {
			for _, h := range apiRequestBodyHeaders {
				currentHeaders.Del(h)
			}
		}
		currentURL = target
		currentParams = nil // Location already carries the full query
	}
}

func apiRequestReadResponse(resp *http.Response) (*apiRequestResponse, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, workflow.RetryableError(fmt.Errorf("read response body: %w", err))
	}
	return &apiRequestResponse{statusCode: resp.StatusCode, body: body}, nil
}

// apiRequestSameOrigin compares normalized (scheme, host, port) triples;
// default ports are normalized (api.py:33-43).
func apiRequestSameOrigin(a, b *url.URL) bool {
	return apiRequestOrigin(a) == apiRequestOrigin(b)
}

func apiRequestOrigin(u *url.URL) string {
	if u == nil {
		return "\x00\x00\x00"
	}
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		switch scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}
	return scheme + "\x00" + host + "\x00" + port
}

// apiRequestInterpolate mirrors _interpolate (api.py:103-115): the engine
// evaluate only parses whole-string expressions; mixed text like URL paths
// gets its $INPUT/$NODE/$GLOBAL tokens evaluated individually and joined back.
func apiRequestInterpolate(ctx context.Context, text string) string {
	if !strings.Contains(text, "$") {
		return text
	}
	return apiRequestTokenRe.ReplaceAllStringFunc(text, func(token string) string {
		v, err := expression.Eval(ctx, token)
		if err != nil {
			return token // leave the broken token literal; the request will fail visibly
		}
		return apiRequestStringify(v)
	})
}

func apiRequestResolveCredential(ctx context.Context, a *APIRequest) (map[string]any, error) {
	resolver := a.CredentialResolver
	if resolver == nil {
		resolver = apiRequestCredentialStore
	}
	if resolver == nil {
		return nil, fmt.Errorf("api_request %q: no credential store configured (wire SetAPIRequestCredentialStore or set CredentialResolver)", a.ID)
	}
	cred, err := resolver(ctx, a.Credential)
	if err != nil {
		return nil, fmt.Errorf("api_request %q: resolve credential %q: %w", a.ID, a.Credential, err)
	}
	if cred == nil {
		return nil, fmt.Errorf("api_request %q: credential %q resolved to no data", a.ID, a.Credential)
	}
	return cred, nil
}

func apiRequestCredentialString(cred map[string]any, key string) string {
	if v, ok := cred[key]; ok {
		return apiRequestStringify(v)
	}
	return ""
}

func apiRequestCredentialHeaders(cred map[string]any) (http.Header, error) {
	headers := http.Header{}
	raw, ok := cred["headers"]
	if !ok || raw == nil {
		return headers, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("credential headers must be a dict, got %T", raw)
	}
	for k, v := range m {
		headers.Set(k, apiRequestStringify(v))
	}
	return headers, nil
}

// apiRequestParamsOf converts the evaluated query field into request params.
// nil values are dropped (requests params semantics); lists expand to
// repeated keys; an empty dict means no params (api.py's falsy check).
func apiRequestParamsOf(v any) (url.Values, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("query must be a dict, got %T", v)
	}
	params := url.Values{}
	for k, val := range m {
		switch t := val.(type) {
		case nil:
			continue
		case []any:
			for _, item := range t {
				if item == nil {
					continue
				}
				params.Add(k, apiRequestStringify(item))
			}
		default:
			params.Set(k, apiRequestStringify(val))
		}
	}
	return params, nil
}

func apiRequestGlobalDryRun(ctx context.Context) bool {
	if rc := runtime.WorkflowRuntimeContext(ctx); rc != nil {
		if g := rc.GetGlobalVariable("dry_run"); g != nil {
			if b, ok := g.(bool); ok && b {
				return true
			}
		}
	}
	return false
}

// apiRequestTimeoutMs mirrors positive_millis (A2R7-05): booleans and
// non-positive / non-numeric values are rejected by name, fail-closed.
func apiRequestTimeoutMs(v any) (int, error) {
	var f float64
	switch t := v.(type) {
	case bool:
		return 0, apiRequestTimeoutMsErr(v)
	case int:
		f = float64(t)
	case int64:
		f = float64(t)
	case float64:
		f = t
	case json.Number:
		var err error
		if f, err = t.Float64(); err != nil {
			return 0, apiRequestTimeoutMsErr(v)
		}
	case string:
		var err error
		if f, err = strconv.ParseFloat(t, 64); err != nil {
			return 0, apiRequestTimeoutMsErr(v)
		}
	default:
		return 0, apiRequestTimeoutMsErr(v)
	}
	if f <= 0 {
		return 0, apiRequestTimeoutMsErr(v)
	}
	return int(f), nil
}

func apiRequestTimeoutMsErr(v any) error {
	return fmt.Errorf("timeout_ms must be a positive number of milliseconds (got %v); 0 and negative values are rejected (fail-closed)", v)
}

// apiRequestIsRetryableStatus mirrors is_retryable_http_status (_retry.py:50):
// 5xx and 408/425/429 are transient; other 4xx are deterministic.
func apiRequestIsRetryableStatus(status int) bool {
	return status >= 500 || status == 408 || status == 425 || status == 429
}

// apiRequestIsRetryable reports whether err carries the workflow.RetryableError
// marker (its Error() renders the "retryable: " prefix); test helper.
func apiRequestIsRetryable(err error) bool {
	for err != nil {
		if strings.HasPrefix(err.Error(), "retryable: ") {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func apiRequestDataString(data any) string {
	switch t := data.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		if b, err := json.Marshal(t); err == nil {
			return string(b)
		}
		return fmt.Sprint(t)
	}
}

// apiRequestTruncate truncates by runes (Python slices str by characters).
func apiRequestTruncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// apiRequestStringify mirrors Python str() for the scalar cases that reach
// URL/query/header/content positions (None renders as "None", Python parity).
func apiRequestStringify(v any) string {
	switch t := v.(type) {
	case nil:
		return "None"
	case string:
		return t
	case bool:
		if t {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case json.Number:
		return t.String()
	default:
		return fmt.Sprint(v)
	}
}

// apiRequestScrubText is the minimal port of plaita-nodes _redact.py: absolute
// URLs in error text keep only scheme://host — every path segment and the
// whole query/fragment are masked; bare credential runs (token=..., api_key:
// ...) are masked; non-http(s) or unparseable URLs degrade to a safe constant.
// Total: never returns an error, never echoes the raw secret-bearing text.
var (
	apiRequestAbsURLRe = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.\-]{0,31}://[^ \t\r\n\f\v]+`)
	// apiRequestCredentialRunRe masks password/token/api_key style runs.
	apiRequestCredentialRunRe = regexp.MustCompile(`(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|client[_-]?secret|private[_-]?key)\b(\s*[=:]\s*)("[^"]*"|'[^']*'|[^\s&,;]+)`)
)

const (
	apiRequestUnprintable = "<unprintable>"
	apiRequestPlaceholder = "***"
)

func apiRequestScrubText(text string) string {
	out := apiRequestAbsURLRe.ReplaceAllStringFunc(text, apiRequestRenderURL)
	out = apiRequestCredentialRunRe.ReplaceAllString(out, "$1$2"+apiRequestPlaceholder)
	return out
}

func apiRequestRenderURL(token string) string {
	parts, err := url.Parse(token)
	if err != nil {
		return apiRequestUnprintable
	}
	scheme := strings.ToLower(parts.Scheme)
	host := parts.Hostname()
	if (scheme != "http" && scheme != "https") || host == "" {
		return apiRequestUnprintable
	}
	var b strings.Builder
	b.WriteString(scheme)
	b.WriteString("://")
	b.WriteString(host)
	if p := parts.Port(); p != "" {
		b.WriteString(":")
		b.WriteString(p)
	}
	for _, seg := range strings.Split(parts.Path, "/") {
		b.WriteString("/")
		if seg != "" {
			b.WriteString(apiRequestPlaceholder)
		}
	}
	if parts.RawQuery != "" {
		b.WriteString("?")
		b.WriteString(apiRequestPlaceholder)
	}
	if parts.Fragment != "" {
		b.WriteString("#")
		b.WriteString(apiRequestPlaceholder)
	}
	return b.String()
}
