package interpreter

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
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

// Test_jsInterpreter_Eval_reboundsContextCancel (#48): an infinite loop in
// flow-authored JS must return once the context is cancelled — pre-fix the vm
// never looked at ctx and the Eval below hung forever (the watchdog fires red).
func Test_jsInterpreter_Eval_reboundsContextCancel(t *testing.T) {
	const spin = "function run(input) { while (true) {} }"

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()
		began := time.Now()
		done := make(chan error, 1)
		go func() {
			_, err := NewJsInterpreter().Eval(ctx, spin, WithJsEvalArgs(&JsEvalArgs{FuncName: "run"}))
			done <- err
		}()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled (named cancellation)", err)
			}
			if el := time.Since(began); el > 3*time.Second {
				t.Fatalf("Eval returned after %s — interrupt fired too late", el)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Eval still running 5s after cancel: ctx-blind vm (pre-#48 negative control)")
		}
	})

	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := NewJsInterpreter().Eval(ctx, spin, WithJsEvalArgs(&JsEvalArgs{FuncName: "run"}))
			done <- err
		}()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err = %v, want context.DeadlineExceeded", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Eval still running 5s past the deadline: ctx-blind vm (pre-#48 negative control)")
		}
	})

	// A cancellation racing a trivial script is benign either way: the
	// interrupt landed first → ctx-named error, or the script completed
	// before the flag was set → plain result. Never a non-ctx error.
	t.Run("cancel racing completion stays benign", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got, err := NewJsInterpreter().Eval(ctx, "function run(input) { return 1+1 }",
			WithJsEvalArgs(&JsEvalArgs{FuncName: "run"}))
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want nil or context.Canceled", err)
		}
		if err == nil && got != int64(2) {
			t.Fatalf("got = %v, want 2", got)
		}
	})
}
