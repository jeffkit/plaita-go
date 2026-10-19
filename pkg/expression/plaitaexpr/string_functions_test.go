package plaitaexpr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

func Test_lower_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "Hello World",
	})
	result1, err1 := e.Eval(ctx1, `$F.lower($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("hello world"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "Hello World",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.lower($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("hello world"), result2)
}

func Test_upper_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "Hello World",
	})
	result1, err1 := e.Eval(ctx1, `$F.upper($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("HELLO WORLD"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "Hello World",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.upper($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("HELLO WORLD"), result2)
}

func Test_capitalize_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "hello world",
	})
	result1, err1 := e.Eval(ctx1, `$F.capitalize($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("Hello world"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "hello world",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.capitalize($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("Hello world"), result2)
}

func Test_title_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "hello world",
	})
	result1, err1 := e.Eval(ctx1, `$F.title($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("Hello World"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "hello world",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.title($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("Hello World"), result2)
}

func Test_strip_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "  hello world  ",
	})
	result1, err1 := e.Eval(ctx1, `$F.strip($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("hello world"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "  hello world  ",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.strip($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("hello world"), result2)
}

func Test_lstrip_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "  hello world  ",
	})
	result1, err1 := e.Eval(ctx1, `$F.lstrip($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("hello world  "), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "  hello world  ",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.lstrip($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("hello world  "), result2)
}

func Test_rstrip_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "  hello world  ",
	})
	result1, err1 := e.Eval(ctx1, `$F.rstrip($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("  hello world"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "  hello world  ",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.rstrip($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("  hello world"), result2)
}

func Test_replace_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "hello world",
	})
	result1, err1 := e.Eval(ctx1, `$F.replace($INPUT, " ", "-")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("hello-world"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "hello world",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.replace($INPUT.a, " ", "-")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("hello-world"), result2)
}

func Test_split_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "hello world",
	})
	result1, err1 := e.Eval(ctx1, `$F.split($INPUT, " ")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue([]any{"hello", "world"}), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "hello world",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.split($INPUT.a, " ")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue([]any{"hello", "world"}), result2)
}

func Test_join_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: []any{"hello", "world"},
	})
	result1, err1 := e.Eval(ctx1, `$F.join($INPUT, " ")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("hello world"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": []any{"hello", "world"},
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.join($INPUT.a, " ")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("hello world"), result2)
}

func Test_startswith_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "hello world",
	})
	result1, err1 := e.Eval(ctx1, `$F.startswith($INPUT, "hello")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(true), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "hello world",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.startswith($INPUT.a, "world")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(false), result2)
}

func Test_endwith_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "hello world",
	})
	result1, err1 := e.Eval(ctx1, `$F.endswith($INPUT, "world")`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(true), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "hello world",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.endswith($INPUT.a, "hello")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(false), result2)
}

func Test_concat_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "hello world",
	})
	result1, err1 := e.Eval(ctx1, `$F.concat($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("hello world"), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "hello",
			"b": "world",
			"c": "!",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.concat($INPUT.a, $INPUT.b, $INPUT.c)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("helloworld!"), result2)
}

func Test_isDigit_eval(t *testing.T) {
	e := Evaluator{}
	// single input
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: "123",
	})
	result1, err1 := e.Eval(ctx1, `$F.isDigit($INPUT)`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue(true), result1)
	// object input
	ctx2 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: map[string]any{
			"a": "hello world",
		},
	})
	result2, err2 := e.Eval(ctx2, `$F.isDigit($INPUT.a)`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue(false), result2)
}
