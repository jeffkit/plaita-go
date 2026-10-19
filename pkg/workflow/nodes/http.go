package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/spf13/cast"

	"github.com/jeffkit/plaita-go/internal/errs"
	"github.com/jeffkit/plaita-go/internal/httputil"
	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/internal/types"
	"github.com/jeffkit/plaita-go/pkg/delegate"
	"github.com/jeffkit/plaita-go/pkg/discovery"
	"github.com/jeffkit/plaita-go/pkg/expression"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func init() {
	workflow.RegisterNodeType(
		types.NodeTypeHTTP,
		func(ctx context.Context) workflow.Node { return new(HTTP) },
	)
}

const (
	responseCtxKey string = "RESPONSE"
	headerCtxKey   string = "HEADERS"
	statusCtxKey   string = "STATUS"

	// RESPONSE sub-content keys
	responseDataKey       string = "data"
	responseStatusKey     string = "status"
	responseStatusTextKey string = "statusText"
	responseHeadersKey    string = "headers"
)

// Transport guardrails (issue #13), mirroring plaita/plaita/node/http.py:
const (
	// defaultRequestTimeoutSecs mirrors request_timeout=30.0 — each request
	// (including every redirect hop) is bounded, so an unresponsive peer can
	// never pin a worker forever.
	defaultRequestTimeoutSecs = 30.0
	// defaultMaxRedirects mirrors max_redirects=5.
	defaultMaxRedirects = 5
	// defaultMaxResponseBytes mirrors DEFAULT_MAX_RESPONSE_BYTES: a 10MiB
	// ceiling on the response body kept in memory.
	defaultMaxResponseBytes = 10 * 1024 * 1024

	// httpBlockPrivateEnv is the operator-level SSRF switch
	// (PLAITA_HTTP_BLOCK_PRIVATE): once set, private/special targets are
	// rejected even when the node does not declare blockPrivateNetworks —
	// node declaration can only be stricter, never looser.
	httpBlockPrivateEnv = "PLAITA_HTTP_BLOCK_PRIVATE"
	// httpMaxResponseBytesEnv overrides the response body ceiling
	// (PLAITA_HTTP_MAX_RESPONSE_BYTES).
	httpMaxResponseBytesEnv = "PLAITA_HTTP_MAX_RESPONSE_BYTES"
	// httpCookiesEnv opts the process into cookie persistence
	// (PLAITA_HTTP_COOKIES=1) — plaita keeps cookies OFF by default so
	// Set-Cookie state cannot leak across flows / tenants sharing a process.
	httpCookiesEnv = "PLAITA_HTTP_COOKIES"
)

// HTTP is workflow plain http node
type HTTP struct {
	BaseNode
	Method        string            `json:"method,omitempty"`      // http method
	ContentType   string            `json:"contentType,omitempty"` // default: application/json
	RawURL        string            `json:"url,omitempty"`         // raw url string
	RawQuery      any               `json:"query,omitempty"`       // raw query string
	RawHeaders    any               `json:"headers,omitempty"`     // raw headers string
	RawBody       any               `json:"body,omitempty"`        // raw body
	RawAddressing *RawAddressing    `json:"addressing,omitempty"`  // raw addressing config
	RawDelegate   *RawDelegateParam `json:"delegate,omitempty"`    // raw delegate config

	// Transport guardrails (plaita parity; see the constants above).
	// Note pkg/ir fail-closed rejects flows that DECLARE the SSRF fields —
	// these back the engine-direct JSON path and the operator switch.
	RequestTimeoutSecs   float64  `json:"requestTimeout,omitempty"`       // per-request (per-hop) timeout, default 30s
	MaxRedirects         *int     `json:"maxRedirects,omitempty"`         // redirect follow cap, default 5
	AllowedHosts         []string `json:"allowedHosts,omitempty"`         // host allowlist: exact / *.suffix / CIDR
	DeniedHosts          []string `json:"deniedHosts,omitempty"`          // host denylist: same formats
	BlockPrivateNetworks bool     `json:"blockPrivateNetworks,omitempty"` // resolve & reject private/special targets
}

type httpExecutor struct {
	URL        *url.URL
	Method     string
	Query      map[string]any
	Body       any
	Headers    http.Header
	Addressing *discovery.Addressing
	Delegate   *DelegateParam

	Policy             urlPolicy
	RequestTimeoutSecs float64
	MaxRedirects       *int
	c                  *http.Client
}

// RawAddressing is http node addressing config (support plaita expression)
type RawAddressing struct {
	Name   string `json:"name"`
	Params any    `json:"params"`
}

// RawDelegateParam is http node delegate param (support plaita expression)
type RawDelegateParam struct {
	Name   string `json:"name,omitempty"`
	Params any    `json:"params,omitempty"`
}

// DelegateParam is http node delegate param
type DelegateParam struct {
	Name   string          `json:"name,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
}

// Empty check if delegate param is empty
func (d *DelegateParam) Empty() bool {
	return d == nil || d.Name == ""
}

// UnmarshalJSON marshal json data to http node
func (h *HTTP) UnmarshalJSON(data []byte) error {
	t := &struct {
		BaseNode
		URL                  string            `json:"url"`
		Method               string            `json:"method,omitempty"`
		Query                any               `json:"query,omitempty"`
		Body                 any               `json:"body,omitempty"`
		Headers              any               `json:"headers,omitempty"`
		Addressing           *RawAddressing    `json:"addressing,omitempty"`
		Delegate             *RawDelegateParam `json:"delegate,omitempty"`
		ContentType          string            `json:"contentType,omitempty"`
		RequestTimeoutSecs   float64           `json:"requestTimeout,omitempty"`
		MaxRedirects         *int              `json:"maxRedirects,omitempty"`
		AllowedHosts         []string          `json:"allowedHosts,omitempty"`
		DeniedHosts          []string          `json:"deniedHosts,omitempty"`
		BlockPrivateNetworks bool              `json:"blockPrivateNetworks,omitempty"`
	}{
		Method:      http.MethodGet,
		Addressing:  &RawAddressing{Name: string(discovery.DomainAddr)},
		ContentType: "application/json",
	}

	if err := json.Unmarshal(data, t); err != nil {
		return fmt.Errorf("unmarshal http node: %w", err)
	}

	*h = HTTP{
		BaseNode:             t.BaseNode,
		Method:               strings.ToUpper(t.Method),
		ContentType:          t.ContentType,
		RawURL:               t.URL,
		RawQuery:             t.Query,
		RawBody:              t.Body,
		RawHeaders:           t.Headers,
		RawAddressing:        t.Addressing,
		RawDelegate:          t.Delegate,
		RequestTimeoutSecs:   t.RequestTimeoutSecs,
		MaxRedirects:         t.MaxRedirects,
		AllowedHosts:         t.AllowedHosts,
		DeniedHosts:          t.DeniedHosts,
		BlockPrivateNetworks: t.BlockPrivateNetworks,
	}
	return nil
}

// GetType returns http node type
func (h *HTTP) GetType() types.NodeType {
	return types.NodeTypeHTTP
}

// Validate validates http node
func (h *HTTP) Validate() error {
	return validation.ValidateStruct(h,
		validation.Field(&h.RawURL, validation.Required),
		validation.Field(&h.Method, validation.In(
			"",
			http.MethodGet,
			http.MethodHead,
			http.MethodPost,
			http.MethodPut,
			http.MethodDelete,
			http.MethodPatch,
			http.MethodConnect,
			http.MethodOptions,
			http.MethodTrace,
		)),
		validation.Field(&h.RawAddressing, validation.Required))
}

const (
	httpGenRequestError = 1001 // http 请求生成请求错误
	httpDoRequestError  = 1002 // http 发送请求错误
	httpNodeExecError   = 1003 // http 节点处理逻辑错误
)

// Execute executes http node
func (h *HTTP) Execute(ctx context.Context) (res *workflow.ExecuteResult, err error) {
	var httpRsp *httpResponse
	defer func() {
		err = handleHttpNodeErr(err, httpRsp)
	}()

	executor, err := h.newExecutor(ctx)
	if err != nil {
		return nil, fmt.Errorf("http node: new executor err: %w", err)
	}

	httpRsp, err = executor.handleRequest(ctx)
	if err != nil {
		return nil, fmt.Errorf("http node: %w", err)
	}

	response := map[string]any{
		responseDataKey:       httpRsp.res,
		responseStatusKey:     httpRsp.rawResponse.StatusCode,
		responseStatusTextKey: reasonPhrase(httpRsp.rawResponse),
		responseHeadersKey:    flattenHeaders(httpRsp.rawResponse.Header),
	}
	// Store response to current node context
	runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
	runtimeCtx.WithCurrentNodeContext(responseCtxKey, response)
	runtimeCtx.WithCurrentNodeContext(headerCtxKey, httpRsp.rawResponse.Header)
	runtimeCtx.WithCurrentNodeContext(statusCtxKey, httpRsp.rawResponse.StatusCode)

	// Logical result: raw parsed body by default, evaluated output when the
	// author set one (plaita _build_response_result).
	var logical any
	if h.Output == nil {
		logical = response[responseDataKey]
	} else {
		logical, err = expression.Eval(ctx, h.Output)
		if err != nil {
			return nil, fmt.Errorf("http node: eval output data: %w", err)
		}
	}

	if err := h.OutputType.ValidateValue(logical); err != nil {
		return nil, fmt.Errorf("http node: validate response body: %w", err)
	}

	// $NODE.<id> contract (plaita node_result_payload, #129/#12): the node
	// result IS the response structure {data,status,statusText,headers}, so
	// $NODE.<id>.data/.status/.statusText/.headers resolve the way the docs
	// promise. An explicit output keeps overriding the result — unchanged.
	resultValue := any(logical)
	if h.Output == nil {
		resultValue = response
	}
	return &workflow.ExecuteResult{Result: resultValue, NextNode: h.NextNode()}, nil
}

func (h *HTTP) newExecutor(ctx context.Context) (*httpExecutor, error) {
	parsedURL, err := h.parseURL(ctx)
	if err != nil {
		return nil, err
	}

	headers, err := h.parseHeader(ctx)
	if err != nil {
		return nil, err
	}

	query, err := h.parseQuery(ctx)
	if err != nil {
		return nil, err
	}

	body, err := h.parseBody(ctx)
	if err != nil {
		return nil, err
	}

	addressingParam, err := h.parseAddressing(ctx)
	if err != nil {
		return nil, err
	}

	delegateParam, err := h.parseDelegate(ctx)
	if err != nil {
		return nil, err
	}

	return &httpExecutor{
		URL:                parsedURL,
		Method:             h.Method,
		Query:              query,
		Body:               body,
		Headers:            headers,
		Addressing:         addressingParam,
		Delegate:           delegateParam,
		Policy:             h.buildURLPolicy(),
		RequestTimeoutSecs: h.RequestTimeoutSecs,
		MaxRedirects:       h.MaxRedirects,
	}, nil
}

func (h *HTTP) parseDelegate(ctx context.Context) (*DelegateParam, error) {
	if h.RawDelegate == nil {
		return &DelegateParam{}, nil
	}

	var delegateName string
	if h.RawDelegate.Name != "" {
		name, err := expression.Eval(ctx, h.RawDelegate.Name)
		if err != nil {
			return nil, fmt.Errorf("http node: eval delegate name: %w", err)
		}
		delegateName = cast.ToString(name)
	}
	var delegateParam json.RawMessage
	if h.RawDelegate.Params != nil {
		param, err := expression.Eval(ctx, h.RawDelegate.Params)
		if err != nil {
			return nil, fmt.Errorf("http node: eval delegate params: %w", err)
		}
		paramBytes, err := json.Marshal(param)
		if err != nil {
			return nil, fmt.Errorf("http node: marshal delegate params: %w", err)
		}
		delegateParam = paramBytes
	}

	return &DelegateParam{
		Name:   delegateName,
		Params: delegateParam,
	}, nil
}

func (h *HTTP) parseAddressing(ctx context.Context) (*discovery.Addressing, error) {
	if h.RawAddressing == nil {
		return &discovery.Addressing{}, nil
	}

	var addrName string
	if h.RawAddressing.Name != "" {
		name, err := expression.Eval(ctx, h.RawAddressing.Name)
		if err != nil {
			return nil, fmt.Errorf("http node: eval addressing name: %w", err)
		}
		addrName = cast.ToString(name)
	}
	var addrParam json.RawMessage
	if h.RawAddressing.Params != nil {
		param, err := expression.Eval(ctx, h.RawAddressing.Params)
		if err != nil {
			return nil, fmt.Errorf("http node: eval addressing params: %w", err)
		}
		paramBytes, err := json.Marshal(param)
		if err != nil {
			return nil, fmt.Errorf("http node: marshal addressing params: %w", err)
		}
		addrParam = paramBytes
	}
	return &discovery.Addressing{
		Name:   discovery.AddressingName(addrName),
		Params: addrParam,
	}, nil
}

func (h *HTTP) parseBody(ctx context.Context) (any, error) {
	var body any
	if h.RawBody == nil {
		return body, nil
	}
	body, err := expression.Eval(ctx, h.RawBody)
	if err != nil {
		return nil, fmt.Errorf("http node: eval body data: %w", err)
	}
	return body, err
}

func (h *HTTP) parseQuery(ctx context.Context) (map[string]any, error) {
	if h.RawQuery == nil {
		return nil, nil
	}
	parsedQuery, err := expression.Eval(ctx, h.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("http node: eval query: %w", err)
	}
	return cast.ToStringMap(parsedQuery), nil
}

func (h *HTTP) parseHeader(ctx context.Context) (http.Header, error) {
	headers := make(http.Header)
	if h.ContentType != "" {
		headers.Set("Content-Type", h.ContentType)
	}
	if h.RawHeaders == nil {
		return headers, nil
	}

	parsedHeaders, err := expression.Eval(ctx, h.RawHeaders)
	if err != nil {
		return nil, fmt.Errorf("http node: eval headers: %w", err)
	}
	headerValues := cast.ToStringMap(parsedHeaders)
	for key, value := range headerValues {
		switch v := value.(type) {
		case string:
			headers.Set(key, v)
		default:
		}
	}
	return headers, nil
}

func (h *HTTP) parseURL(ctx context.Context) (*url.URL, error) {
	parsedURL, err := expression.Eval(ctx, h.RawURL)
	if err != nil {
		return nil, fmt.Errorf("http node: eval url: %w", err)
	}

	u, err := url.Parse(cast.ToString(parsedURL))
	if err != nil {
		return nil, fmt.Errorf("http node: parse url: %w", err)
	}
	return u, nil
}

func handleHttpNodeErr(err error, httpRsp *httpResponse) error {
	if err == nil {
		return nil
	}

	if httpRsp.empty() {
		return wrapHttpNodeErr(httpGenRequestError, err.Error(), httpRsp)
	}

	if httpRsp.sendResquestFail() {
		return wrapHttpNodeErr(httpDoRequestError, err.Error(), httpRsp)
	}
	return wrapHttpNodeErr(httpNodeExecError, err.Error(), httpRsp)
}

type httpResponse struct {
	rawRequest  *http.Request
	rawResponse *http.Response
	res         any
}

func (h *httpResponse) empty() bool {
	return h == nil
}

func (h *httpResponse) sendResquestFail() bool {
	return !h.empty() && h.rawRequest != nil && h.rawResponse == nil
}

func wrapHttpNodeErr(code int, message string, rsp *httpResponse) error {
	err := &errs.Error{
		Code:    code,
		Message: message,
		Info:    nil,
	}
	if rsp.empty() {
		return err
	}

	info := &HttpNodeErrorInfo{
		Code:    code,
		Message: message,
		Request: rsp.rawRequest,
	}
	if rsp.rawResponse != nil {
		info.Response = &HttpNodeResponse{
			Status:     rsp.rawResponse.StatusCode,
			StatusText: reasonPhrase(rsp.rawResponse),
			Headers:    rsp.rawResponse.Header,
			Data:       rsp.res,
		}
	}
	err.Info = info
	return err
}

// HttpNodeErrorInfo http 节点错误信息
type HttpNodeErrorInfo struct {
	Code     int               `json:"code" structs:"code"`
	Message  string            `json:"message" structs:"message"`
	Request  *http.Request     `json:"request" structs:"request"`
	Response *HttpNodeResponse `json:"response" structs:"response"`
}

// HttpNodeResponse http 节点响应
type HttpNodeResponse struct {
	Status     int         `json:"status" structs:"status"`
	StatusText string      `json:"statusText" structs:"statusText"`
	Headers    http.Header `json:"headers" structs:"headers"`
	Data       any         `json:"data" structs:"data"`
}

// reasonPhrase extracts the reason phrase the way plaita statusText does
// (response.reason): "200 OK" → "OK".
func reasonPhrase(resp *http.Response) string {
	return strings.TrimSpace(strings.TrimPrefix(resp.Status, strconv.Itoa(resp.StatusCode)))
}

// flattenHeaders renders response headers the way plaita's dict(headers)
// does: name → single string value (duplicates comma-joined).
func flattenHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vs := range h {
		out[k] = strings.Join(vs, ", ")
	}
	return out
}

// handleRequest performs the guarded request: per-hop timeout, manual
// hop-by-hop redirects (method/body/header rules + per-hop policy + cap),
// operator/session policy checks, capped body read and lenient JSON parse.
func (h *httpExecutor) handleRequest(ctx context.Context) (*httpResponse, error) {
	if h.c == nil {
		h.c = newHTTPClient()
	}

	selector := discovery.GetSelector(h.Addressing.Name)
	if selector == nil {
		selector = discovery.DefaultSelector
	}
	registryNode, err := selector.Addr(ctx, h.URL, h.Addressing.Params)
	if err != nil {
		return nil, err
	}

	h.URL = registryNode.URL
	current := mergeQuery(h.URL, h.Query)
	body, err := h.marshalBody()
	if err != nil {
		return nil, err
	}
	method := h.Method
	headers := h.Headers

	// every hop gets its own request-time budget (plaita timeout is per
	// send); cancels are collected and released when the exchange ends
	var cancels []context.CancelFunc
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()
	nextHopCtx := func() context.Context {
		hopCtx, cancel := context.WithTimeout(ctx, h.requestTimeout())
		cancels = append(cancels, cancel)
		return hopCtx
	}
	sendHop := func(target *url.URL) (*http.Request, *http.Response, error) {
		hopCtx := nextHopCtx()
		request, berr := buildRequest(hopCtx, method, body, headers, target)
		if berr != nil {
			return nil, nil, berr
		}
		if h.Policy.active() {
			// restricted: every hop is re-checked — a 302 must not carry the
			// request into a forbidden network
			if perr := h.Policy.check(hopCtx, target); perr != nil {
				return request, nil, perr
			}
		}
		begin := time.Now()
		response, derr := h.c.Do(request)
		// report request status ignore error
		_ = selector.Report(registryNode, time.Since(begin), derr)
		if derr != nil {
			select {
			case <-ctx.Done():
				derr = ctx.Err()
			default:
			}
			return request, nil, fmt.Errorf("do request: %w", derr)
		}
		return request, response, nil
	}

	if !h.Delegate.Empty() {
		// delegate transports drive their own request shape; the guards on
		// the response (capped read / lenient parse) still apply below
		delegator := delegate.GetHTTPDelegator(h.Delegate.Name)
		if delegator == nil {
			return nil, fmt.Errorf("delegator %s not found", h.Delegate.Name)
		}
		request, berr := buildRequest(nextHopCtx(), method, body, headers, current)
		if berr != nil {
			return nil, berr
		}
		response, derr := delegator.Do(request, h.Delegate.Params)
		if derr != nil {
			return &httpResponse{rawRequest: request}, derr
		}
		return h.consumeResponse(request, response)
	}

	request, response, err := sendHop(current)
	if err != nil {
		if request == nil {
			return nil, err // build failure → 1001
		}
		// policy / transport failure → send_request_fail (1002)
		return &httpResponse{rawRequest: request}, err
	}

	// Manual hop-by-hop redirect following (plaita allow_redirects=False on
	// both paths): the client never follows automatically, so method/body
	// rewrite rules, header purges, per-hop policy checks and the
	// maxRedirects cap all stay under this node's control.
	hops := 0
	for isRedirectResponse(response) && hops < h.maxRedirects() {
		location := response.Header.Get("Location")
		response.Body.Close()
		ref, perr := url.Parse(location)
		if perr != nil {
			return &httpResponse{rawRequest: request}, fmt.Errorf("parse redirect location: %w", perr)
		}
		next := current.ResolveReference(ref)
		method = redirectMethod(response.StatusCode, method)
		if response.StatusCode != http.StatusTemporaryRedirect && response.StatusCode != http.StatusPermanentRedirect {
			body = nil // 301/302/303 drop the body
		}
		headers = redirectHeaders(response.StatusCode, headers, current, next)
		current = next
		hops++

		nextReq, nextRsp, herr := sendHop(current)
		if herr != nil {
			if nextReq == nil {
				return nil, herr // build failure → 1001
			}
			// error frame carries the request of the hop being attempted
			return &httpResponse{rawRequest: nextReq}, herr
		}
		request, response = nextReq, nextRsp
	}
	if isRedirectResponse(response) {
		response.Body.Close()
		return &httpResponse{rawRequest: request}, fmt.Errorf("Too many redirects (> %d)", h.maxRedirects())
	}

	return h.consumeResponse(request, response)
}

// consumeResponse reads the body under the size ceiling and parses it
// leniently (plaita _loads_lenient): JSON first, raw text as the fallback —
// a text/HTML body is a VALUE here, never an error (#12).
func (h *httpExecutor) consumeResponse(request *http.Request, response *http.Response) (*httpResponse, error) {
	defer response.Body.Close()
	data, err := readCappedBody(response.Body, maxResponseBytes())
	if err != nil {
		if errors.As(err, &errResponseTooLarge{}) {
			// over-limit: the response snapshot rides along → node-level
			// policy error (1003), same as plaita ResponseTooLargeError
			return &httpResponse{rawRequest: request, rawResponse: response}, err
		}
		// transport read failure: no snapshot → send_request_fail (1002)
		return &httpResponse{rawRequest: request}, err
	}

	var res any
	if perr := json.Unmarshal(data, &res); perr != nil {
		res = string(data)
	}
	return &httpResponse{
		rawRequest:  request,
		rawResponse: response,
		res:         res,
	}, nil
}

func (h *httpExecutor) requestTimeout() time.Duration {
	secs := defaultRequestTimeoutSecs
	if h.RequestTimeoutSecs > 0 {
		secs = h.RequestTimeoutSecs
	}
	return time.Duration(secs * float64(time.Second))
}

func (h *httpExecutor) maxRedirects() int {
	if h.MaxRedirects != nil {
		return *h.MaxRedirects
	}
	return defaultMaxRedirects
}

func (h *httpExecutor) marshalBody() ([]byte, error) {
	if h.Body == nil {
		return nil, nil
	}
	data, err := json.Marshal(h.Body)
	if err != nil {
		return nil, fmt.Errorf("marshal body data: %w", err)
	}
	return data, nil
}

// mergeQuery merges the node's query params into the request URL (once —
// redirect targets carry their own query string).
func mergeQuery(u *url.URL, query map[string]any) *url.URL {
	if len(query) == 0 {
		return u
	}
	merged := httputil.CloneURL(u)
	uq := merged.Query()
	for k, v := range query {
		sv, err := cast.ToStringE(v)
		if err != nil {
			continue
		}
		uq.Set(k, sv)
	}
	merged.RawQuery = uq.Encode()
	return merged
}

func buildRequest(ctx context.Context, method string, body []byte, headers http.Header, u *url.URL) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	r, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	if r.Header == nil {
		r.Header = make(http.Header)
	}
	for k, vs := range headers {
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}
	return r, nil
}

// newHTTPClient builds the guarded transport: redirects are NEVER followed
// automatically (the hop loop above owns them), and the process cookie
// policy applies — plaita keeps cookies OFF unless PLAITA_HTTP_COOKIES=1,
// so Set-Cookie state cannot leak across flows sharing the process.
func newHTTPClient() *http.Client {
	c := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	if httpCookiesEnabled() {
		c.Jar = sharedCookieJar()
	}
	return c
}

var (
	sharedJarOnce sync.Once
	sharedJarInst *cookiejar.Jar
)

func sharedCookieJar() http.CookieJar {
	sharedJarOnce.Do(func() {
		// nil Options = no public-suffix list; jars remain isolated per
		// site which is the conservative default
		jar, err := cookiejar.New(nil)
		if err != nil {
			return
		}
		sharedJarInst = jar
	})
	return sharedJarInst
}

func httpCookiesEnabled() bool {
	switch strings.TrimSpace(os.Getenv(httpCookiesEnv)) {
	case "1", "true", "yes":
		return true
	}
	return false
}

func operatorBlockPrivate() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(httpBlockPrivateEnv))) {
	case "1", "true", "yes":
		return true
	}
	return false
}

func maxResponseBytes() int {
	raw := strings.TrimSpace(os.Getenv(httpMaxResponseBytesEnv))
	if raw == "" {
		return defaultMaxResponseBytes
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return defaultMaxResponseBytes
	}
	return v
}

// errResponseTooLarge marks an over-limit body read (plaita
// ResponseTooLargeError).
type errResponseTooLarge struct{ max int }

func (e errResponseTooLarge) Error() string {
	return fmt.Sprintf("HTTP response body exceeds maxResponseBytes=%d; raise %s to allow larger bodies",
		e.max, httpMaxResponseBytesEnv)
}

// readCappedBody reads at most max bytes; one byte more → too-large error.
// The +1 read is deliberate: a single Read(max) can mistake a big body for
// EOF and silently truncate.
func readCappedBody(r io.Reader, max int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > max {
		return nil, errResponseTooLarge{max: max}
	}
	return data, nil
}

// isRedirectResponse mirrors requests' Response.is_redirect: a redirect
// status WITH a Location header (a 3xx without one is a normal response).
func isRedirectResponse(resp *http.Response) bool {
	switch resp.StatusCode {
	case http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect:
	default:
		return false
	}
	return resp.Header.Get("Location") != ""
}

// redirectMethod mirrors plaita _redirect_method (requests'
// Session.rebuild_method): 303/302 turn non-HEAD into GET; 301 turns ONLY
// POST into GET; 307/308 keep the method.
func redirectMethod(statusCode int, method string) string {
	upper := strings.ToUpper(method)
	if (statusCode == http.StatusSeeOther || statusCode == http.StatusFound) && upper != http.MethodHead {
		return http.MethodGet
	}
	if statusCode == http.StatusMovedPermanently && upper == http.MethodPost {
		return http.MethodGet
	}
	return method
}

// sensitiveRedirectHeaders mirrors plaita _SENSITIVE_REDIRECT_HEADERS:
// credential-class headers stripped when a redirect changes origin.
var sensitiveRedirectHeaders = map[string]struct{}{
	"authorization":       {},
	"cookie":              {},
	"cookie2":             {},
	"proxy-authorization": {},
	"proxy-authenticate":  {},
	"www-authenticate":    {},
}

func defaultSchemePort(scheme string) string {
	if scheme == "https" {
		return "443"
	}
	if scheme == "http" {
		return "80"
	}
	return ""
}

// redirectChangesOrigin mirrors plaita _redirect_changes_origin (requests
// Session.should_strip_auth): host change, or same host with scheme / port
// change — the single exception being the http:80 → https:443 upgrade.
func redirectChangesOrigin(from, to *url.URL) bool {
	if from.Hostname() != to.Hostname() {
		return true
	}
	oldPort, newPort := from.Port(), to.Port()
	if from.Scheme == "http" && (oldPort == "80" || oldPort == "") &&
		to.Scheme == "https" && (newPort == "443" || newPort == "") {
		return false
	}
	changedPort := oldPort != newPort
	changedScheme := from.Scheme != to.Scheme
	if !changedScheme && portIsDefault(from.Scheme, oldPort) && portIsDefault(to.Scheme, newPort) {
		return false
	}
	return changedPort || changedScheme
}

func portIsDefault(scheme, port string) bool {
	def := defaultSchemePort(scheme)
	return port == def || port == ""
}

// redirectHeaders builds the next hop's headers (plaita _redirect_request):
// host/content-length always recomputed; 301/302/303 additionally drop the
// body's content-type/transfer-encoding; cross-origin redirects strip
// credential-class headers.
func redirectHeaders(statusCode int, headers http.Header, from, to *url.URL) http.Header {
	keepBody := statusCode == http.StatusTemporaryRedirect || statusCode == http.StatusPermanentRedirect
	crossOrigin := redirectChangesOrigin(from, to)
	out := make(http.Header, len(headers))
	for k, vs := range headers {
		lk := strings.ToLower(k)
		if lk == "host" || lk == "content-length" {
			continue
		}
		if !keepBody && (lk == "content-type" || lk == "transfer-encoding") {
			continue
		}
		if crossOrigin {
			if _, sensitive := sensitiveRedirectHeaders[lk]; sensitive {
				continue
			}
		}
		out[k] = vs
	}
	return out
}

// --- SSRF guardrails (plaita 2026-09 security review P1-1) -----------------

// privateNetworks mirrors plaita _PRIVATE_NETWORKS: loopback / private /
// link-local / carrier NAT / benchmark ranges and the IPv6 counterparts.
var privateNetworks = mustParseNetworks([]string{
	"0.0.0.0/8", "10.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16",
	"172.16.0.0/12", "192.0.0.0/24", "192.168.0.0/16", "100.64.0.0/10",
	"198.18.0.0/15", "::1/128", "fc00::/7", "fe80::/10",
})

func mustParseNetworks(specs []string) []*net.IPNet {
	nets := make([]*net.IPNet, 0, len(specs))
	for _, s := range specs {
		_, ipnet, err := net.ParseCIDR(s)
		if err != nil {
			panic(fmt.Sprintf("http node: bad private network spec %q: %v", s, err))
		}
		nets = append(nets, ipnet)
	}
	return nets
}

func ipInPrivateNetworks(ip net.IP) bool {
	for _, n := range privateNetworks {
		if n.Contains(ip) {
			return true
		}
		if t4 := ip.To4(); t4 != nil && n.Contains(t4) {
			return true
		}
	}
	return false
}

// urlPolicy is the node's URL access policy: allowed/denied host lists plus
// private-network blocking. The operator switch (PLAITA_HTTP_BLOCK_PRIVATE)
// ORs into blockPrivate — a node declaration can only be stricter, never
// looser.
type urlPolicy struct {
	allowedHosts []string
	deniedHosts  []string
	blockPrivate bool
}

func (h *HTTP) buildURLPolicy() urlPolicy {
	return urlPolicy{
		allowedHosts: h.AllowedHosts,
		deniedHosts:  h.DeniedHosts,
		blockPrivate: h.BlockPrivateNetworks || operatorBlockPrivate(),
	}
}

func (p urlPolicy) active() bool {
	return len(p.allowedHosts) > 0 || len(p.deniedHosts) > 0 || p.blockPrivate
}

// check validates a request URL against the policy (plaita _host_allowed):
// scheme must be http/https; a denied entry hit rejects; a non-empty
// allowlist requires a hit; blockPrivate resolves the host and rejects
// private/special addresses — resolution included, since a literal-host
// check cannot catch a hostname resolving into the intranet.
func (p urlPolicy) check(ctx context.Context, u *url.URL) error {
	if !p.active() {
		return nil
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("URL scheme %q is not allowed (http/https only)", u.Scheme)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return fmt.Errorf("URL has no hostname")
	}

	var ips []net.IP
	if len(p.deniedHosts) > 0 || p.blockPrivate || hasCIDREntry(p.allowedHosts) {
		port := u.Port()
		if port == "" {
			port = defaultSchemePort(u.Scheme)
			if port == "" {
				port = "80"
			}
		}
		resolved, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil || len(resolved) == 0 {
			return fmt.Errorf("Cannot resolve %q to enforce HTTP policy", host)
		}
		ips = make([]net.IP, 0, len(resolved))
		for _, addr := range resolved {
			ips = append(ips, addr.IP)
		}
	}
	return p.validateHostTarget(host, ips)
}

// validateHostTarget mirrors plaita _validate_host_target order: denied
// first, then the allowlist, then private-network blocking.
func (p urlPolicy) validateHostTarget(host string, ips []net.IP) error {
	for _, entry := range p.deniedHosts {
		if hostEntryMatches(entry, host, ips) {
			return fmt.Errorf("URL host %q matches denied_hosts entry %q", host, entry)
		}
	}
	if len(p.allowedHosts) > 0 {
		matched := false
		for _, entry := range p.allowedHosts {
			if hostEntryMatches(entry, host, ips) {
				matched = true
				break
			}
		}
		if !matched {
			if ips == nil && hasCIDREntry(p.allowedHosts) {
				// a CIDR allowlist must not degrade "unresolvable" into
				// "not in the list" — fail loudly instead (#83 parity)
				return fmt.Errorf("Cannot resolve %q to verify allowed_hosts CIDR entry", host)
			}
			return fmt.Errorf("URL host %q is not in allowed_hosts", host)
		}
	}
	if p.blockPrivate {
		if ips == nil {
			return fmt.Errorf("Cannot resolve %q to verify block_private_networks policy", host)
		}
		for _, ip := range ips {
			if ipInPrivateNetworks(ip) {
				return fmt.Errorf("URL host %q resolves to private/special address %s (block_private_networks=true)", host, ip)
			}
		}
	}
	return nil
}

// hostEntryMatches mirrors plaita _host_entry_matches: exact host,
// "*.suffix" wildcard, or CIDR matched against the resolved IPs.
func hostEntryMatches(entry, host string, ips []net.IP) bool {
	entry = strings.ToLower(strings.TrimSpace(entry))
	if entry == "" {
		return false
	}
	if strings.Contains(entry, "/") {
		_, ipnet, err := net.ParseCIDR(entry)
		if err != nil {
			return false
		}
		for _, ip := range ips {
			if ipnet.Contains(ip) {
				return true
			}
			// IPv4-mapped IPv6 normalised before compare
			if t4 := ip.To4(); t4 != nil && ipnet.Contains(t4) {
				return true
			}
		}
		return false
	}
	if strings.HasPrefix(entry, "*.") {
		suffix := strings.TrimPrefix(entry, "*")
		return host == strings.TrimPrefix(entry, "*.") || strings.HasSuffix(host, suffix)
	}
	return host == entry
}

func hasCIDREntry(entries []string) bool {
	for _, e := range entries {
		if strings.Contains(e, "/") {
			return true
		}
	}
	return false
}
