package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
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
}

type httpExecutor struct {
	URL        *url.URL
	Method     string
	Query      map[string]any
	Body       any
	Headers    http.Header
	Addressing *discovery.Addressing
	Delegate   *DelegateParam
	c          *http.Client
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
		URL         string            `json:"url"`
		Method      string            `json:"method,omitempty"`
		Query       any               `json:"query,omitempty"`
		Body        any               `json:"body,omitempty"`
		Headers     any               `json:"headers,omitempty"`
		Addressing  *RawAddressing    `json:"addressing,omitempty"`
		Delegate    *RawDelegateParam `json:"delegate,omitempty"`
		ContentType string            `json:"contentType,omitempty"`
	}{
		Method:      http.MethodGet,
		Addressing:  &RawAddressing{Name: string(discovery.DomainAddr)},
		ContentType: "application/json",
	}

	if err := json.Unmarshal(data, t); err != nil {
		return fmt.Errorf("unmarshal http node: %w", err)
	}

	*h = HTTP{
		BaseNode:      t.BaseNode,
		Method:        strings.ToUpper(t.Method),
		ContentType:   t.ContentType,
		RawURL:        t.URL,
		RawQuery:      t.Query,
		RawBody:       t.Body,
		RawHeaders:    t.Headers,
		RawAddressing: t.Addressing,
		RawDelegate:   t.Delegate,
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
		responseStatusTextKey: httpRsp.rawResponse.Status,
		responseHeadersKey:    httpRsp.rawResponse.Header,
	}
	// Store response to current node context
	runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
	runtimeCtx.WithCurrentNodeContext(responseCtxKey, response)
	runtimeCtx.WithCurrentNodeContext(headerCtxKey, httpRsp.rawResponse.Header)
	runtimeCtx.WithCurrentNodeContext(statusCtxKey, httpRsp.rawResponse.StatusCode)

	// Set final Result
	var finalResult any
	if h.Output == nil {
		finalResult = response[responseDataKey]
	} else {
		finalResult, err = expression.Eval(ctx, h.Output)
		if err != nil {
			return nil, fmt.Errorf("http node: eval output data: %w", err)
		}
	}

	if err := h.OutputType.ValidateValue(finalResult); err != nil {
		return nil, fmt.Errorf("http node: validate response body: %w", err)
	}
	return &workflow.ExecuteResult{Result: finalResult, NextNode: h.NextNode()}, nil
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
		URL:        parsedURL,
		Method:     h.Method,
		Query:      query,
		Body:       body,
		Headers:    headers,
		Addressing: addressingParam,
		Delegate:   delegateParam,
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
			StatusText: rsp.rawResponse.Status,
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

func (h *httpExecutor) handleRequest(ctx context.Context) (*httpResponse, error) {
	if h.c == nil {
		h.c = &http.Client{}
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
	request, err := h.newRequest(ctx)
	if err != nil {
		return nil, err
	}

	var response *http.Response
	if !h.Delegate.Empty() {
		delegator := delegate.GetHTTPDelegator(h.Delegate.Name)
		if delegator == nil {
			return nil, fmt.Errorf("delegator %s not found", h.Delegate.Name)
		}
		response, err = delegator.Do(request, h.Delegate.Params)
		if err != nil {
			return &httpResponse{rawRequest: request}, err
		}
	} else {
		response, err = h.do(ctx, request, selector, registryNode)
		if err != nil {
			return &httpResponse{rawRequest: request}, err
		}
	}
	defer response.Body.Close()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		return &httpResponse{rawRequest: request, rawResponse: response}, fmt.Errorf("read response body: %w", err)
	}

	var res any
	if err := json.Unmarshal(data, &res); err != nil {
		return &httpResponse{rawRequest: request, rawResponse: response}, fmt.Errorf("unmarshal response body: %w", err)
	}
	return &httpResponse{
		rawRequest:  request,
		rawResponse: response,
		res:         res,
	}, nil
}

func (h *httpExecutor) do(
	ctx context.Context,
	reqeust *http.Request,
	selector discovery.Selector,
	registryNode *discovery.RegistryNode,
) (*http.Response, error) {
	begin := time.Now()
	response, err := h.c.Do(reqeust)
	cost := time.Since(begin)
	// report request status ignore error
	_ = selector.Report(registryNode, cost, err)
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
		return nil, fmt.Errorf("do request: %w", err)
	}
	return response, nil
}

func (h *httpExecutor) newRequest(ctx context.Context) (*http.Request, error) {
	var reader io.Reader
	if h.Body != nil {
		formData, err := json.Marshal(h.Body)
		if err != nil {
			return nil, fmt.Errorf("marshal body data: %w", err)
		}
		reader = bytes.NewReader(formData)
	}

	u := httputil.CloneURL(h.URL)
	if h.Query != nil {
		uq := u.Query()
		for k, v := range h.Query {
			sv, err := cast.ToStringE(v)
			if err != nil {
				continue
			}
			uq.Set(k, sv)
		}
		u.RawQuery = uq.Encode()
	}

	r, err := http.NewRequestWithContext(ctx, h.Method, u.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}

	if r.Header == nil {
		r.Header = make(http.Header)
	}
	for k, vs := range h.Headers {
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}

	return r, nil
}
