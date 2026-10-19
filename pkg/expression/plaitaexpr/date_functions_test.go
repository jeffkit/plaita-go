package plaitaexpr

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/jeffkit/plaita-go/internal/runtime"
	"github.com/jeffkit/plaita-go/pkg/model"
)

func Test_now_eval(t *testing.T) {
	patches := gomonkey.ApplyFuncReturn(time.Now, time.Unix(1704038400, 0))
	defer patches.Reset()
	e := &Evaluator{}
	// no format
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10,
	})
	result1, err1 := e.Eval(ctx1, `$F.now()`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("2024-01-01 00:00:00"), result1)
	// has format
	result2, err2 := e.Eval(ctx1, `$F.now("2006.01.02")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("2024.01.01"), result2)
}

func Test_today_eval(t *testing.T) {
	patches := gomonkey.ApplyFuncReturn(time.Now, time.Unix(1704038400, 0))
	defer patches.Reset()
	e := &Evaluator{}
	// no format
	ctx1 := runtime.WithWorkflowRuntimeContext(context.Background(), &runtime.Context{
		WorkflowInput: 10,
	})
	result1, err1 := e.Eval(ctx1, `$F.today()`, nil)
	assert.NoError(t, err1)
	assert.Equal(t, model.NewValue("2024-01-01"), result1)
	// has format
	result2, err2 := e.Eval(ctx1, `$F.today("2006.01.02")`, nil)
	assert.NoError(t, err2)
	assert.Equal(t, model.NewValue("2024.01.01"), result2)
}
