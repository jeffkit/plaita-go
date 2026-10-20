package interpreter

// Option apply options to the interpreter.
type Option interface {
	apply(o *options)
}

type options struct {
	jsEvalArgs *JsEvalArgs
}

type funcOption struct {
	f func(o *options)
}

func newFuncOption(f func(o *options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

func (fo *funcOption) apply(o *options) {
	fo.f(o)
}

// WithJsEvalArgs sets the js eval args.
func WithJsEvalArgs(args *JsEvalArgs) Option {
	return newFuncOption(func(o *options) {
		o.jsEvalArgs = args
	})
}

func applyOptions(opts ...Option) *options {
	o := &options{}
	for _, opt := range opts {
		opt.apply(o)
	}
	return o
}
