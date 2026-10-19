package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/delegate"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

func TestHTTP_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name         string
		data         string
		wantHTTPNode *HTTP
		wantErr      bool
	}{
		{
			name: "test normal unmarshal",
			data: `
			{
				"id": "http",
				"type": "http",
				"next": "end",
				"url": "http://example.com/api/v1/foo",
				"method": "GET",
				"headers": {
					"Content-Type": "application/json"
				},
				"body": {
					"key": "value",
					"key2": [1, 2]
				},
				"query": {
					"foo": "bar"
				}
			}
			`,
			wantHTTPNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Type: "http",
					Next: "end",
				},
				Method: "GET",
				RawBody: map[string]any{
					"key":  "value",
					"key2": []any{float64(1), float64(2)},
				},
				ContentType: "application/json",
				RawURL:      "http://example.com/api/v1/foo",
				RawQuery: map[string]any{
					"foo": "bar",
				},
				RawHeaders: map[string]any{
					"Content-Type": "application/json",
				},
				RawAddressing: &RawAddressing{
					Name: "domain",
				},
			},
			wantErr: false,
		},
		{
			name: "test default Content-Type",
			data: `
			{
				"id": "http",
				"type": "http",
				"next": "end",
				"url": "http://example.com/api/v1/foo",
				"method": "GET",
				"headers": {},
				"body": {
					"key": "value",
					"key2": [1, 2]
				},
				"query": {
					"foo": "bar"
				}
			}
			`,
			wantHTTPNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Type: "http",
					Next: "end",
				},
				Method: "GET",
				RawBody: map[string]any{
					"key":  "value",
					"key2": []any{float64(1), float64(2)},
				},
				ContentType: "application/json",
				RawURL:      "http://example.com/api/v1/foo",
				RawQuery: map[string]any{
					"foo": "bar",
				},
				RawHeaders: map[string]any{},
				RawAddressing: &RawAddressing{
					Name: "domain",
				},
			},
			wantErr: false,
		},
		{
			name: "test override Content-Type",
			data: `
			{
				"id": "http",
				"type": "http",
				"next": "end",
				"url": "http://example.com/api/v1/foo",
				"method": "GET",
				"headers": {"Content-Type": "form-urlencoded"},
				"body": {
					"key": "value",
					"key2": [1, 2]
				},
				"query": {
					"foo": "bar"
				}
			}
			`,
			wantHTTPNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Type: "http",
					Next: "end",
				},
				Method: "GET",
				RawBody: map[string]any{
					"key":  "value",
					"key2": []any{float64(1), float64(2)},
				},
				ContentType: "application/json",
				RawURL:      "http://example.com/api/v1/foo",
				RawQuery: map[string]any{
					"foo": "bar",
				},
				RawHeaders: map[string]any{
					"Content-Type": "form-urlencoded",
				},
				RawAddressing: &RawAddressing{
					Name: "domain",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := new(HTTP)
			if err := got.UnmarshalJSON([]byte(tt.data)); (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				require.Equal(t, tt.wantHTTPNode, got)
			}
		})
	}
}

type mockBar struct{}

// Do mock bar http request
func (b *mockBar) Do(request *http.Request, params json.RawMessage) (*http.Response, error) {
	result := map[string]any{
		"user": "nick",
	}
	jsonResult, _ := json.Marshal(result)
	return &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
		Header: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: io.NopCloser(bytes.NewBuffer(jsonResult)),
	}, nil
}

func TestHTTP_Execute(t *testing.T) {
	mux := http.NewServeMux()
	handlers := map[string]http.HandlerFunc{
		"/api/v1/foo": func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodGet {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}

			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`{"foo": "bar"}`))
		},
		"/api/v1/bar": func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(request.Body)
			t.Logf("body: %s", string(body))

			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`{"foo": "bar", "age": 20, "country": "China"}`))
		},
	}
	for path, h := range handlers {
		mux.Handle(path, h)
	}
	s := httptest.NewServer(mux)
	defer s.Close()
	host := strings.TrimPrefix(s.URL, "http://")

	delegate.RegisterHTTPDelegator("mockBar", &mockBar{})

	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		httpNode   *HTTP
		want       *workflow.ExecuteResult
		wantErr    bool
	}{
		{
			name: "test simple http node",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "foo",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Next: "end",
					OutputType: workflow.Property{
						DataType: workflow.Object,
						Properties: map[string]workflow.Property{
							"foo": {DataType: workflow.String},
						},
					},
				},
				Method: http.MethodGet,
				RawURL: fmt.Sprintf("http://%s/api/v1/foo", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
			},
			want: &workflow.ExecuteResult{
				Result: map[string]any{
					"foo": "bar",
				},
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test http node query merge",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name": "foo",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:   "http",
					Next: "end",
					OutputType: workflow.Property{
						DataType: workflow.Object,
						Properties: map[string]workflow.Property{
							"foo": {DataType: workflow.String},
						},
					},
				},
				Method: http.MethodGet,
				RawURL: fmt.Sprintf("http://%s/api/v1/foo?name=bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
			},
			want: &workflow.ExecuteResult{
				Result: map[string]any{
					"foo": "bar",
				},
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test POST http node with expr body",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$STATUS",
					OutputType: workflow.Property{
						DataType: workflow.Integer,
					},
				},
				Method: http.MethodPost,
				RawBody: map[string]any{
					"fields": "$INPUT.fields",
				},
				RawURL: fmt.Sprintf("http://%s/api/v1/bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
			},
			want: &workflow.ExecuteResult{
				Result:   200,
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test POST http node with delegate method",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$RESPONSE.data.user",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Method: http.MethodPost,
				RawBody: map[string]any{
					"fields": "$INPUT.fields",
				},
				RawAddressing: &RawAddressing{},
				RawDelegate: &RawDelegateParam{
					Name:   "mockBar",
					Params: "",
				},
				RawURL: fmt.Sprintf("http://%s/api/v1/bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
			},
			want: &workflow.ExecuteResult{
				Result:   "nick",
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test POST http node with response data",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$RESPONSE.data.foo",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Method: http.MethodPost,
				RawBody: map[string]any{
					"fields": "$INPUT.fields",
				},
				RawURL: fmt.Sprintf("http://%s/api/v1/bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
			},
			want: &workflow.ExecuteResult{
				Result:   "bar",
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test POST http node with plaita expr",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
					"requestHeaders": map[string]any{
						"Content-Type": "application/json",
					},
					"requestParams": map[string]any{
						"name": "foo",
					},
				},
				GlobalVariables: map[string]any{
					"domain":     fmt.Sprintf("http://%s", host),
					"addressing": "domain",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$RESPONSE.data.foo",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Method:  http.MethodPost,
				RawBody: "$INPUT.requestParams",
				RawURL:  `$F.concat($GLOBAL.domain, "/api/v1/bar")`,
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawHeaders: "$INPUT.requestHeaders",
				RawAddressing: &RawAddressing{
					Name: "$GLOBAL.addressing",
				},
			},
			want: &workflow.ExecuteResult{
				Result:   "bar",
				NextNode: "end",
			},
			wantErr: false,
		},
		{
			name: "test POST http node with delegate method(plaita expr)",
			runtimeCtx: &runtime.Context{
				WorkflowInput: map[string]any{
					"name":   "foo",
					"fields": "age,country",
				},
				GlobalVariables: map[string]any{
					"delegate": "mockBar",
				},
			},
			httpNode: &HTTP{
				BaseNode: BaseNode{
					ID:     "http",
					Next:   "end",
					Output: "$RESPONSE.data.user",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Method: http.MethodPost,
				RawBody: map[string]any{
					"fields": "$INPUT.fields",
				},
				RawURL: fmt.Sprintf("http://%s/api/v1/bar", host),
				RawQuery: map[string]any{
					"name": "$INPUT.name",
				},
				RawAddressing: &RawAddressing{},
				RawDelegate: &RawDelegateParam{
					Name:   "$GLOBAL.delegate",
					Params: "",
				},
			},
			want: &workflow.ExecuteResult{
				Result:   "nick",
				NextNode: "end",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.httpNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), tt.runtimeCtx))
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() got = %v, want %v", got, tt.want)
			}
		})
	}
}
