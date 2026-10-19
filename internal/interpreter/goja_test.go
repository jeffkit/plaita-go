package interpreter

import (
	"context"
	"reflect"
	"testing"
)

func Test_jsInterpreter_Eval(t *testing.T) {
	tests := []struct {
		name     string
		src      string
		evalArgs *JsEvalArgs
		want     any
		wantErr  bool
	}{
		{
			name: "test simple code",
			src: `
				function getHelloWorld() {
				  return "hello world";
				}
				`,
			evalArgs: &JsEvalArgs{
				FuncName:   "getHelloWorld",
				Parameters: "",
			},
			want:    "hello world",
			wantErr: false,
		},
		{
			name: "test code with arguments",
			src: `
				function getHelloWorld(person) {
				  return "hello "+ person.name;
				}
				`,
			evalArgs: &JsEvalArgs{
				FuncName: "getHelloWorld",
				Parameters: map[string]any{
					"name": "world",
				},
			},
			want:    "hello world",
			wantErr: false,
		},
		{
			name: "test not support entrypoint type",
			src: `
				function getHelloWorld(name) {
				  return "hello "+name;
				}
				`,
			evalArgs: &JsEvalArgs{
				FuncName:   "getHelloWorld",
				Parameters: 7,
			},
			want:    "hello 7",
			wantErr: false,
		},
		{
			name: "get array type",
			src: `
				function getArray() {
				  return [1,2.0,3];
				}
				`,
			evalArgs: &JsEvalArgs{
				FuncName:   "getArray",
				Parameters: "",
			},
			want:    []any{int64(1), int64(2), int64(3)},
			wantErr: false,
		},
		{
			name: "get string array type",
			src: `
				function getArray() {
				  return ["hello","world"];
				}
				`,
			evalArgs: &JsEvalArgs{
				FuncName:   "getArray",
				Parameters: "",
			},
			want:    []any{"hello", "world"},
			wantErr: false,
		},
		{
			name: "get bool array type",
			src: `
				function getArray() {
				  return [true,false];
				}
				`,
			evalArgs: &JsEvalArgs{
				FuncName:   "getArray",
				Parameters: "",
			},
			want:    []any{true, false},
			wantErr: false,
		},
		{
			name: "get map type",
			src: `
				function getMap() {
				  return {"name": "tom", "age": 18};
				}
				`,
			evalArgs: &JsEvalArgs{
				FuncName:   "getMap",
				Parameters: "",
			},
			want:    map[string]any{"name": "tom", "age": int64(18)},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewJsInterpreter().Eval(context.Background(), tt.src, WithJsEvalArgs(tt.evalArgs))
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
