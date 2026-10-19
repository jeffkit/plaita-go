package plaitaexpr

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/jeffkit/plaita-go/internal/runtime"

	"github.com/jeffkit/plaita-go/pkg/model"
)

func TestEvaluator_Eval(t *testing.T) {
	type args struct {
		context    *runtime.Context
		expr       string
		parameters map[string]any
	}
	tests := []struct {
		name    string
		args    args
		want    model.Value
		wantErr bool
	}{
		{
			name: "INPUT var",
			args: args{
				context: &runtime.Context{
					WorkflowInput: map[string]any{
						"a": 1,
					},
				},
				expr: "$INPUT.a",
			},
			want:    model.NewValue(1),
			wantErr: false,
		},
		{
			name: "INPUT var nested",
			args: args{
				context: &runtime.Context{
					WorkflowInput: map[string]any{
						"a": map[string]any{
							"b": "yes",
						},
					},
				},
				expr: "$INPUT.a.b",
			},
			want:    model.NewValue("yes"),
			wantErr: false,
		},
		{
			name: "NODE var",
			args: args{
				context: &runtime.Context{
					ExecuteNodeResults: map[string]any{
						"a": 1,
					},
				},
				expr: "$NODE.a",
			},
			want:    model.NewValue(1),
			wantErr: false,
		},
		{
			name: "ENV var",
			args: args{
				context: &runtime.Context{
					EnvVariables: map[string]any{
						"a": "10",
					},
				},
				expr: "$ENV.a",
			},
			want:    model.NewValue("10"),
			wantErr: false,
		},
		{
			name: "NODE var nested",
			args: args{
				context: &runtime.Context{
					ExecuteNodeResults: map[string]any{
						"a": map[string]any{
							"b": "yes",
						},
					},
				},
				expr: "$NODE.a.b",
			},
			want:    model.NewValue("yes"),
			wantErr: false,
		},
		{
			name: "PARENT var",
			args: args{
				context: &runtime.Context{
					ParentWorkflowContext: runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
						WorkflowInput: map[string]any{
							"a": 1,
						},
					}),
				},
				expr: "$PARENT.$INPUT.a",
			},
			want:    model.NewValue(1),
			wantErr: false,
		},
		{
			name: "PARENT var nested",
			args: args{
				context: &runtime.Context{
					ParentWorkflowContext: runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
						WorkflowInput: map[string]any{
							"a": map[string]any{
								"b": "yes",
							},
						},
					}),
				},
				expr: "$PARENT.$INPUT.a.b",
			},
			want:    model.NewValue("yes"),
			wantErr: false,
		},
		{
			name: "simple function",
			args: args{
				context: &runtime.Context{
					WorkflowInput: map[string]any{
						"a": 1,
					},
				},
				expr: "$F.add(1.0, 2.0)",
			},
			want:    model.NewValue(float64(3)),
			wantErr: false,
		},
		{
			name: "function with simple var",
			args: args{
				context: &runtime.Context{
					WorkflowInput: map[string]any{
						"a": 1.0,
					},
				},
				expr: "$F.add(1.0, $INPUT.a)",
			},
			want:    model.NewValue(float64(2)),
			wantErr: false,
		},
		{
			name: "function with NODE var",
			args: args{
				context: &runtime.Context{
					ExecuteNodeResults: map[string]any{
						"a": map[string]any{
							"b": 1.0,
						},
					},
				},
				expr: "$F.add(1.0, $NODE.a.b)",
			},
			want:    model.NewValue(float64(2)),
			wantErr: false,
		},
		{
			name: "function with ENV var",
			args: args{
				context: &runtime.Context{},
				expr:    `$F.concat("1", $ENV.a)`,
			},
			want:    model.NewValue("110"),
			wantErr: false,
		},
		{
			name: "function with PARENT var",
			args: args{
				context: &runtime.Context{
					WorkflowInput: map[string]any{
						"a": 1.0,
					},
					ParentWorkflowContext: runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
						ExecuteNodeResults: map[string]any{
							"node1": map[string]any{
								"a": 2.0,
							},
						},
					}),
				},
				expr: "$F.add(1.0, $PARENT.$NODE.node1.a)",
			},
			want:    model.NewValue(float64(3)),
			wantErr: false,
		},
		{
			name: "function nested",
			args: args{
				context: &runtime.Context{
					WorkflowInput: map[string]any{
						"a": "plaita",
					},
				},
				expr: `$F.concat("hello", ", ", $F.concat("world", "!", " ", $INPUT.a))`,
			},
			want:    model.NewValue("hello, world! plaita"),
			wantErr: false,
		},
		{
			name: "func with current var",
			args: args{
				context: &runtime.Context{
					CurrentNodeContext: map[string]any{
						"a": map[string]any{
							"b": 1.0,
						},
					},
				},
				expr: "$F.add($a.b, 1.0)",
			},
			want:    model.NewValue(2.0),
			wantErr: false,
		},
		{
			name: "function with error var",
			args: args{
				context: &runtime.Context{
					ExecuteNodeErrors: map[string]any{
						"http": map[string]any{
							"info": map[string]any{
								"statusText": "服务器异常",
							},
						},
					},
				},
				expr: "$F.concat(\"目标接口请求失败 - [\", $ERROR.http.info.statusText, \"]\")",
			},
			want:    model.NewValue("目标接口请求失败 - [服务器异常]"),
			wantErr: false,
		},
		{
			name: "function with global var",
			args: args{
				context: &runtime.Context{
					GlobalVariables: map[string]any{
						"domain": "https://tencent.com",
					},
				},
				expr: "$F.concat($GLOBAL.domain, \"/hello\")",
			},
			want:    model.NewValue("https://tencent.com/hello"),
			wantErr: false,
		},
	}
	os.Setenv("a", "10")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Evaluator{}
			ctx := runtime.WithWorkflowRuntimeContext(context.Background(), tt.args.context)
			runtimeCtx := runtime.WorkflowRuntimeContext(ctx)
			runtimeCtx.WithEnvVariablesContext()
			got, err := e.Eval(ctx, tt.args.expr, tt.args.parameters)
			if (err != nil) != tt.wantErr {
				t.Errorf("Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Eval() got = %v, want %v", got, tt.want)
			}
		})
	}
}
