package tasks

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	opentracing_log "github.com/opentracing/opentracing-go/log"

	"github.com/RichardKnop/machinery/v1/log"
)

// ErrTaskPanicked ...
var ErrTaskPanicked = errors.New("Invoking task caused a panic")

// Task wraps a signature and methods used to reflect task arguments and
// return values after invoking the task
type Task struct {
	TaskFunc   reflect.Value
	UseContext bool
	Context    context.Context
	Args       []reflect.Value
}

type ExtendForSignatureFunc func(by time.Duration, signature *Signature) error

type ExtendFunc func(time.Duration) error

type signatureCtxType struct{}
type extendFuncCtxType struct{}

var signatureCtx signatureCtxType
var extendFuncCtx extendFuncCtxType

// SignatureFromContext gets the signature from the context
func SignatureFromContext(ctx context.Context) *Signature {
	if ctx == nil {
		return nil
	}

	v := ctx.Value(signatureCtx)
	if v == nil {
		return nil
	}

	signature, _ := v.(*Signature)
	return signature
}

// ExtendFuncFromContext gets the extend function from the context
func ExtendFuncFromContext(ctx context.Context) ExtendFunc {
	if ctx == nil {
		return nil
	}

	v := ctx.Value(extendFuncCtx)
	if v == nil {
		return nil
	}

	ef, _ := v.(ExtendFunc)
	return ef
}

// NewWithSignature is the same as New but injects the signature
func NewWithSignature(taskFunc interface{}, signature *Signature, extendFunc ExtendForSignatureFunc) (*Task, error) {
	args := signature.Args
	ctx := context.Background()
	ctx = context.WithValue(ctx, signatureCtx, signature)

	var ef ExtendFunc = func(by time.Duration) error {
		return extendFunc(by, signature)
	}
	ctx = context.WithValue(ctx, extendFuncCtx, ef)

	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  ctx,
	}

	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %s", err)
	}

	return task, nil
}

// New tries to use reflection to convert the function and arguments
// into a reflect.Value and prepare it for invocation
func New(taskFunc interface{}, args []Arg) (*Task, error) {
	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  context.Background(),
	}

	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %s", err)
	}

	return task, nil
}

// Call attempts to call the task with the supplied arguments.
//
// `err` is set in the return value in two cases:
//  1. The reflected function invocation panics (e.g. due to a mismatched
//     argument list).
//  2. The task func itself returns a non-nil error.
func (t *Task) Call() (taskResults []*TaskResult, err error) {
	// retrieve the span from the task's context and finish it as soon as this function returns
	span := opentracing.SpanFromContext(t.Context)
	if span != nil {
		defer span.Finish()
	}

	defer func() {
		// Recover from panic and set err.
		if e := recover(); e != nil {
			switch e := e.(type) {
			default:
				err = ErrTaskPanicked
			case error:
				err = e
			case string:
				err = errors.New(e)
			}

			// mark the span as failed and dump the error and stack trace to the span
			if span := opentracing.SpanFromContext(t.Context); span != nil {
				opentracing_ext.Error.Set(span, true)
				span.LogFields(
					opentracing_log.Error(err),
					opentracing_log.Object("stack", string(debug.Stack())),
				)
			}

			// Print stack trace
			log.ERROR.Printf("%v stack: %s", err, debug.Stack())
		}
	}()

	args := t.Args

	if t.UseContext {
		ctxValue := reflect.ValueOf(t.Context)
		args = append([]reflect.Value{ctxValue}, args...)
	}

	// Invoke the task
	results := t.TaskFunc.Call(args)
	signature := SignatureFromContext(t.Context)
	recordTimeSinceIngestion := func(name string) {
		if signature != nil && signature.IngestionTime != nil {
			span.SetTag(
				name,
				time.Now().Sub(*signature.IngestionTime).Microseconds())
		}
	}
	recordTimeSinceIngestion("ingestion_to_processed")

	// Task must return at least a value
	if len(results) == 0 {
		return nil, ErrTaskReturnsNoValue
	}

	// Last returned value
	lastResult := results[len(results)-1]

	// If the last returned value is not nil, it has to be of error type, if that
	// is not the case, return error message, otherwise propagate the task error
	// to the caller
	if !lastResult.IsNil() {
		recordTimeSinceIngestion("ingestion_to_err")

		value := lastResult.Interface()

		// check that the result implements the standard error interface,
		// if not, return ErrLastReturnValueMustBeError error
		asError, ok := value.(error)
		if !ok {
			return nil, ErrLastReturnValueMustBeError
		}

		_, isRetryable := asError.(Retryable)

		if span != nil {
			if !isRetryable {
				span.LogFields(opentracing_log.Error(asError))
			} else {
				span.SetTag("warning", asError)
			}

			span.SetTag("can_retry", isRetryable)
			span.SetTag("did_fail", true)
		}

		// Return the standard error
		return nil, asError
	}
	if span != nil {
		span.SetTag("did_fail", false)
	}

	recordTimeSinceIngestion("ingestion_to_success")

	// Convert reflect values to task results
	taskResults = make([]*TaskResult, len(results)-1)
	for i := 0; i < len(results)-1; i++ {
		val := results[i].Interface()
		typeStr := reflect.TypeOf(val).String()
		taskResults[i] = &TaskResult{
			Type:  typeStr,
			Value: val,
		}
	}

	return taskResults, err
}

// ReflectArgs converts []TaskArg to []reflect.Value
func (t *Task) ReflectArgs(args []Arg) error {
	argValues := make([]reflect.Value, len(args))

	for i, arg := range args {
		argValue, err := ReflectValue(arg.Type, arg.Value)
		if err != nil {
			return err
		}
		argValues[i] = argValue
	}

	t.Args = argValues
	return nil
}
