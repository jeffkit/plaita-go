package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecoverableErrorHandlePolicy_IsContinue(t *testing.T) {
	type fields struct {
		Strategy HandleStrategy
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "test abort",
			fields: fields{
				HandleStrategyAbort,
			},
			want: false,
		},
		{
			name: "test continue",
			fields: fields{
				HandleStrategyContinue,
			},
			want: true,
		},
		{
			name: "test continue-with",
			fields: fields{
				HandleStrategyContinueWith,
			},
			want: true,
		},
		{
			name: "test default",
			fields: fields{
				"default",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RecoverableErrorHandlePolicy{
				Strategy: tt.fields.Strategy,
			}
			assert.Equalf(t, tt.want, r.IsContinue(), "IsContinue()")
		})
	}
}
