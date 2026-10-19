package plaitaexpr

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/maja42/goval"
)

type ExpressionFunc = goval.ExpressionFunction

// Function represents a function of the expression language and is
// used by function nodes.
type Function struct {
	Name       string
	ArgTypes   []reflect.Type
	Variadic   int
	ReturnType []reflect.Type
}

// Registerer is the interface for the part of a Registry in charge of
// registering and unregistering. Users of custom registries should use
// Registerer as type for registration purposes (rather than the Registry type
// directly). In that way, they are free to use custom Registerer implementation
// (e.g. for testing purposes).
type Registerer interface {
	Register(name string, f ExpressionFunc) error
	MustRegister(name string, f ExpressionFunc)
	Unregister(name string) bool
}

// DefaultRegistry is the default function registry
var DefaultRegistry = NewRegistry()

// GetFunction returns expression function for given name, returns false if not exists
func GetFunction(name string) (ExpressionFunc, bool) {
	return DefaultRegistry.GetFunction(name)
}

// AllFunctions returns all registered functions
func AllFunctions() map[string]ExpressionFunc {
	return DefaultRegistry.functions
}

// Registry is the function registry for the expression language
type Registry struct {
	mutex     sync.Mutex
	functions map[string]ExpressionFunc
}

// NewRegistry create Registerer instance
func NewRegistry() *Registry {
	return &Registry{functions: make(map[string]ExpressionFunc)}
}

// GetFunction returns expression function for given name, returns false if not exists
func (r *Registry) GetFunction(name string) (ExpressionFunc, bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	f, ok := r.functions[name]
	return f, ok
}

// Register registers expression function for given name, returns error if given name already exists
func (r *Registry) Register(name string, f ExpressionFunc) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, ok := r.functions[name]; ok {
		return fmt.Errorf("function %q already exists", name)
	}
	r.functions[name] = f
	return nil
}

// MustRegister must register expression function for given name, panic if not
func (r *Registry) MustRegister(name string, f ExpressionFunc) {
	if err := r.Register(name, f); err != nil {
		panic(err)
	}
}

// Unregister unregisters the function that equals the name passed
// in as an argument.
// The function returns whether a function was unregistered.
func (r *Registry) Unregister(name string) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, ok := r.functions[name]; ok {
		delete(r.functions, name)
		return true
	}
	return false
}

func mustToExpressionFunc(f any) ExpressionFunc {
	rt := reflect.TypeOf(f)
	if rt.Kind() != reflect.Func {
		panic("function must be a function")
	}

	return func(args ...any) (any, error) {
		var values []reflect.Value
		for _, v := range args {
			values = append(values, reflect.ValueOf(v))
		}

		result := reflect.ValueOf(f).Call(values)
		switch len(result) {
		case 0:
			return nil, nil
		case 1:
			return result[0].Interface(), nil
		case 2:
			err, ok := result[1].Interface().(error)
			if !ok {
				return result[0].Interface(), nil
			}
			return result[0].Interface(), err
		default:
			panic("unreachable: function must return 0, 1 or 2 values")
		}
	}
}
