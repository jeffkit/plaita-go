package nodes

import (
	"context"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/workflow"

	"github.com/go-redis/redismock/v8"
)

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
