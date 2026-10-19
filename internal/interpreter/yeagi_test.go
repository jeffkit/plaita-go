package interpreter

import (
	"context"
	"reflect"
	"testing"
)

func Test_goInterpreter_Eval(t *testing.T) {
	tests := []struct {
		name     string
		src      string
		evalArgs *GoEvalArgs
		want     any
		wantErr  bool
	}{
		{
			name: "test simple code",
			src: `package foo

			import "context"

			func bar(ctx context.Context, parameters any) (any, error) {
				return "hello world", nil
			}
			`,
			evalArgs: &GoEvalArgs{
				PackageName: "foo",
				FuncName:    "bar",
				Parameters:  nil,
			},
			want:    "hello world",
			wantErr: false,
		},
		{
			name: "test code with arguments",
			src: `package foo

			import (
				"context"
				"fmt"
			)

			const prefix = "foo"

			func bar(ctx context.Context, parameters any) (any, error) {
				if v, ok := parameters.(string); ok {
					return fmt.Sprintf("%s: hello, %s", prefix, v), nil
				}
				return fmt.Sprintf("%s: hello, world", prefix), nil
			}
			`,
			evalArgs: &GoEvalArgs{
				PackageName: "foo",
				FuncName:    "bar",
				Parameters:  "plaita",
			},
			want:    "foo: hello, plaita",
			wantErr: false,
		},
		{
			name: "test not support entrypoint type",
			src: `package foo

			import (
				"context"
				"errors"
			)

			const prefix = "foo"

			func bar(ctx context.Context, parameters any) error {
				return errors.New("not support entrypoint type")
			}
			`,
			evalArgs: &GoEvalArgs{
				PackageName: "foo",
				FuncName:    "bar",
				Parameters:  "plaita",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewGoInterpreter().Eval(context.Background(), tt.src, WithGoEvalArgs(tt.evalArgs))
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
