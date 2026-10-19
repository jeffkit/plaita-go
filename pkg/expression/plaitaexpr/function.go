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
		result, err := callFuncReflect(f, rt, args)
		if err != nil {
			return nil, err
		}
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

// callFuncReflect invokes f with args through reflect, supporting variadic
// functions (plaita's concat/or/getDictValue take optional or variadic
// arguments). A type mismatch or nil argument surfaces as an error instead
// of a reflect panic.
func callFuncReflect(f any, rt reflect.Type, args []any) ([]reflect.Value, error) {
	fn := reflect.ValueOf(f)
	// A nil argument (a plaita field-miss yields nil) becomes the zero value
	// of the target parameter type, mirroring Python passing None into the
	// function and letting the function decide.
	if !rt.IsVariadic() {
		if len(args) != rt.NumIn() {
			return nil, fmt.Errorf("function expects %d arguments, got %d", rt.NumIn(), len(args))
		}
		values := make([]reflect.Value, 0, len(args))
		for i, v := range args {
			values = append(values, argReflectValue(v, rt.In(i)))
		}
		return safeCall(fn, values)
	}

	numFixed := rt.NumIn() - 1
	if len(args) < numFixed {
		return nil, fmt.Errorf("function expects at least %d arguments, got %d", numFixed, len(args))
	}
	values := make([]reflect.Value, 0, numFixed+1)
	for i := 0; i < numFixed; i++ {
		values = append(values, argReflectValue(args[i], rt.In(i)))
	}
	elemType := rt.In(numFixed).Elem()
	rest := args[numFixed:]
	slice := reflect.MakeSlice(reflect.SliceOf(elemType), len(rest), len(rest))
	for i, a := range rest {
		slice.Index(i).Set(argReflectValue(a, elemType))
	}
	values = append(values, slice)
	return safeCallSlice(fn, values)
}

func safeCall(fn reflect.Value, values []reflect.Value) (result []reflect.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	return fn.Call(values), nil
}

func safeCallSlice(fn reflect.Value, values []reflect.Value) (result []reflect.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	return fn.CallSlice(values), nil
}

// argReflectValue converts a generic argument into a reflect value of the
// target parameter type; an untyped nil becomes the target's zero value.
func argReflectValue(v any, targetType reflect.Type) reflect.Value {
	if v == nil {
		return reflect.Zero(targetType)
	}
	rv := reflect.ValueOf(v)
	if !rv.Type().AssignableTo(targetType) && targetType.Kind() != reflect.Interface {
		// best effort: rely on safeCall's recover when reflect rejects it
		return rv
	}
	return rv
}
