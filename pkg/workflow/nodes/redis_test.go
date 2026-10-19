package nodes

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/go-redis/redismock/v8"
)

// TestRedisNode_ExpressionFields pins issue #14: target/command/arguments
// all go through plaita expression evaluation (Python evaluates all three),
// and a dynamic target must not fail at Validate time.
func TestRedisNode_ExpressionFields(t *testing.T) {
	client, mock := redismock.NewClientMock()

	// Validate must not parse an expression target as a URL (it is resolved
	// at execute time).
	node := &RedisNode{
		BaseNode:  BaseNode{ID: "redis", Next: "end"},
		Target:    "$INPUT.redisUrl",
		Command:   "$INPUT.cmd",
		Arguments: "$NODE.up.key",
	}
	if err := node.Validate(); err != nil {
		t.Fatalf("Validate with expression target: %v", err)
	}

	node.client = client
	node.Arguments = "$INPUT.args"
	mock.ExpectGet("expr_key").SetVal("expr_value")
	got, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"redisUrl": "redis://root:123456@localhost:6379/1",
			"cmd":      "get",
			"args":     "expr_key",
		},
		ExecuteNodeResults: map[string]any{
			"up": map[string]any{"key": "ignored"},
		},
	}))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.Result != "expr_value" {
		t.Errorf("Result = %v, want expr_value", got.Result)
	}
}

// TestRedisNode_NonStringFieldRejected pins the fail-loud rule: a field that
// evaluates to a non-string must error, not be stringified into the command.
func TestRedisNode_NonStringFieldRejected(t *testing.T) {
	client, _ := redismock.NewClientMock()
	node := &RedisNode{
		BaseNode:  BaseNode{ID: "redis"},
		Target:    "redis://root:123456@localhost:6379",
		Command:   "$INPUT.cmd",
		Arguments: "k",
		client:    client,
	}
	_, err := node.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{"cmd": 42},
	}))
	if err == nil || !reflect.DeepEqual(err.Error(), "Redis node: command must evaluate to a string, got int") {
		t.Errorf("err = %v, want non-string command error", err)
	}
}

// TestRedisNode_ClientCachePerTarget pins the bounded per-target LRU
// (plaita #81): same evaluated target reuses one client, distinct targets
// get distinct clients, the cache never exceeds redisClientCacheMax.
func TestRedisNode_ClientCachePerTarget(t *testing.T) {
	node := &RedisNode{BaseNode: BaseNode{ID: "redis"}}
	first, err := node.clientFor("redis://root:123456@localhost:6379/1")
	if err != nil {
		t.Fatalf("clientFor: %v", err)
	}
	second, err := node.clientFor("redis://root:123456@localhost:6379/1")
	if err != nil {
		t.Fatalf("clientFor: %v", err)
	}
	if first != second {
		t.Errorf("same target must reuse the client")
	}
	for i := 0; i < redisClientCacheMax+2; i++ {
		if _, err := node.clientFor(fmt.Sprintf("redis://root:123456@localhost:637%d", i%10)); err != nil {
			t.Fatalf("clientFor %d: %v", i, err)
		}
	}
	if len(node.clients) != redisClientCacheMax {
		t.Errorf("cache size = %d, want %d", len(node.clients), redisClientCacheMax)
	}
}

// TestRedisNode_ValidateDoesNotBuildClient pins that load-time validation no
// longer dials/constructs a client from the raw target (#14: Validate used to
// url.Parse the target, breaking expression targets).
func TestRedisNode_ValidateDoesNotBuildClient(t *testing.T) {
	node := &RedisNode{
		BaseNode: BaseNode{ID: "redis"},
		Target:   "$INPUT.redisUrl",
		Command:  "get",
	}
	if err := node.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if node.client != nil || len(node.clients) != 0 {
		t.Errorf("Validate must not construct clients")
	}
}

func TestRedisNode_Execute(t *testing.T) {
	client, mock := redismock.NewClientMock()

	tests := []struct {
		name       string
		runtimeCtx *runtime.Context
		redisNode  *RedisNode
		want       *workflow.ExecuteResult
		wantErr    bool
		setup      func()
	}{
		{
			name:       "test execute Redis get",
			runtimeCtx: &runtime.Context{},
			redisNode: &RedisNode{
				BaseNode: BaseNode{
					ID:   "redis",
					Next: "end",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Target:    "redis://root:123456@localhost:6379/1",
				Command:   "get",
				Arguments: "test_key",
				client:    client,
			},
			want: &workflow.ExecuteResult{
				Result:   "test_value",
				NextNode: "end",
			},
			wantErr: false,
			setup: func() {
				mock.ExpectGet("test_key").SetVal("test_value")
			},
		},
		{
			name:       "test execute Redis set",
			runtimeCtx: &runtime.Context{},
			redisNode: &RedisNode{
				BaseNode: BaseNode{
					ID:   "redis",
					Next: "end",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Target:    "redis://root:123456@localhost:6379",
				Command:   "set",
				Arguments: "test_key test_value",
				client:    client,
			},
			want: &workflow.ExecuteResult{
				Result:   "OK",
				NextNode: "end",
			},
			wantErr: false,
			setup: func() {
				mock.ExpectSet("test_key", "test_value", 0).SetVal("OK")
			},
		},
		{
			name:       "test execute Redis del",
			runtimeCtx: &runtime.Context{},
			redisNode: &RedisNode{
				BaseNode: BaseNode{
					ID:   "redis",
					Next: "end",
					OutputType: workflow.Property{
						DataType: workflow.String,
					},
				},
				Target:    "redis://root:123456@localhost:6379",
				Command:   "del",
				Arguments: "test_key",
				client:    client,
			},
			want: &workflow.ExecuteResult{
				Result:   int64(1),
				NextNode: "end",
			},
			wantErr: false,
			setup: func() {
				mock.ExpectDel("test_key").SetVal(1)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 调用 setup 函数设置预期的 Redis 操作
			if tt.setup != nil {
				tt.setup()
			}

			got, err := tt.redisNode.Execute(runtime.WithWorkflowRuntimeContext(context.Background(), tt.runtimeCtx))
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
