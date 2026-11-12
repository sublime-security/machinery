package machinery

import (
	"context"
	"errors"
	"testing"
	"time"

	backendsiface "github.com/RichardKnop/machinery/v1/backends/iface"
	brokersiface "github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/require"
)

type tracingBackend struct {
	backendsiface.Backend
	pending func(*tasks.Signature) error
}

func (b tracingBackend) SetStatePending(s *tasks.Signature) error { return b.pending(s) }

type tracingBroker struct {
	brokersiface.Broker
	publish func(context.Context, *tasks.Signature) error
}

func (b tracingBroker) Publish(ctx context.Context, s *tasks.Signature) error {
	return b.publish(ctx, s)
}

func TestSendTaskStageTracing(t *testing.T) {
	tracer := mocktracer.New()
	old := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tracer)
	t.Cleanup(func() { opentracing.SetGlobalTracer(old) })
	for _, test := range []struct {
		name                       string
		backendError, publishError bool
		noBackend                  bool
	}{
		{name: "success"}, {name: "no backend", noBackend: true}, {name: "backend error", backendError: true}, {name: "publish error", publishError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracer.Reset()
			parent := tracer.StartSpan("caller")
			ctx, cancel := context.WithTimeout(opentracing.ContextWithSpan(t.Context(), parent), time.Minute)
			defer cancel()
			deadline, _ := ctx.Deadline()
			var order []string
			sentinel := errors.New("private error text")
			server := &Server{
				backend: tracingBackend{pending: func(s *tasks.Signature) error {
					order = append(order, "backend")
					time.Sleep(time.Millisecond)
					require.Equal(t, test.noBackend, s.NoBackend)
					if test.backendError {
						return sentinel
					}
					return nil
				}},
				broker: tracingBroker{publish: func(bctx context.Context, s *tasks.Signature) error {
					order = append(order, "publish")
					time.Sleep(time.Millisecond)
					actualDeadline, ok := bctx.Deadline()
					require.True(t, ok)
					require.Equal(t, deadline, actualDeadline)
					span := opentracing.SpanFromContext(bctx).(*mocktracer.MockSpan)
					require.Equal(t, "SendTask", span.OperationName)
					require.Equal(t, parent.(*mocktracer.MockSpan).SpanContext.TraceID, span.SpanContext.TraceID)
					var carrier tasks.Headers = s.Headers
					extracted, err := tracer.Extract(opentracing.TextMap, carrier)
					require.NoError(t, err)
					require.Equal(t, span.Context(), extracted)
					if test.publishError {
						return sentinel
					}
					return nil
				}},
				prePublishHandler: func(s *tasks.Signature) {
					order = append(order, "pre_publish")
					time.Sleep(time.Millisecond)
				},
			}
			sig := &tasks.Signature{Name: "test", UUID: "task_123", NoBackend: test.noBackend}
			result, err := server.SendTaskWithContext(ctx, sig)
			finished := tracer.FinishedSpans()
			require.Len(t, finished, 1)
			span := finished[0]
			require.Equal(t, parent.(*mocktracer.MockSpan).SpanContext.SpanID, span.ParentID)
			require.Equal(t, test.noBackend, span.Tag("machinery.no_backend"))
			require.GreaterOrEqual(t, span.Tag("machinery.backend.duration_ms").(float64), 1.0)
			if test.backendError {
				require.EqualError(t, err, "Set state pending error: private error text")
				require.Equal(t, []string{"backend"}, order)
				require.Equal(t, "backend", span.Tag("machinery.error.phase"))
				require.Nil(t, span.Tag("machinery.publish.duration_ms"))
				require.Nil(t, result)
			} else {
				require.Equal(t, []string{"backend", "pre_publish", "publish"}, order)
				require.GreaterOrEqual(t, span.Tag("machinery.pre_publish.duration_ms").(float64), 1.0)
				require.GreaterOrEqual(t, span.Tag("machinery.publish.duration_ms").(float64), 1.0)
				if test.publishError {
					require.EqualError(t, err, "Publish message error: private error text")
					require.Equal(t, "publish", span.Tag("machinery.error.phase"))
					require.Nil(t, result)
				} else {
					require.NoError(t, err)
					require.Nil(t, span.Tag("error"))
					if test.noBackend {
						require.Nil(t, result)
					} else {
						require.NotNil(t, result)
					}
				}
			}
			if test.backendError || test.publishError {
				require.Equal(t, true, span.Tag("error"))
			}
			for _, tag := range span.Tags() {
				require.NotEqual(t, "private error text", tag)
			}
			parent.Finish()
		})
	}
}

func TestSendTaskTracingMissingBackend(t *testing.T) {
	tracer := mocktracer.New()
	old := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tracer)
	t.Cleanup(func() { opentracing.SetGlobalTracer(old) })
	_, err := (&Server{}).SendTaskWithContext(t.Context(), &tasks.Signature{Name: "test"})
	require.EqualError(t, err, "Result backend required")
	require.Len(t, tracer.FinishedSpans(), 1)
	span := tracer.FinishedSpans()[0]
	require.Equal(t, true, span.Tag("error"))
	require.Equal(t, "backend", span.Tag("machinery.error.phase"))
	require.Nil(t, span.Tag("machinery.backend.duration_ms"))
}

func TestSendTaskPreservesCanceledContextAndSplitSpan(t *testing.T) {
	tracer := mocktracer.New()
	old := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tracer)
	t.Cleanup(func() { opentracing.SetGlobalTracer(old) })
	parent := tracer.StartSpan("caller")
	defer parent.Finish()
	type key struct{}
	ctx := context.WithValue(opentracing.ContextWithSpan(t.Context(), parent), key{}, "kept")
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	deadline, _ := ctx.Deadline()
	cancel()
	headers := tasks.Headers{"existing": "unchanged"}
	server := &Server{
		backend: tracingBackend{pending: func(*tasks.Signature) error { return nil }},
		broker: tracingBroker{publish: func(ctx context.Context, sig *tasks.Signature) error {
			require.Equal(t, context.Canceled, ctx.Err())
			require.Equal(t, "kept", ctx.Value(key{}))
			actual, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, deadline, actual)
			require.Equal(t, tasks.Headers{"existing": "unchanged"}, sig.Headers)
			require.Equal(t, "SendTask", opentracing.SpanFromContext(ctx).(*mocktracer.MockSpan).OperationName)
			return ctx.Err()
		}},
	}
	_, err := server.SendTaskWithContext(ctx, &tasks.Signature{Name: "test", UUID: "fixed", Headers: headers, SplitSpan: true, NoBackend: true})
	require.EqualError(t, err, "Publish message error: context canceled")
	spans := tracer.FinishedSpans()
	require.Len(t, spans, 1)
	require.Nil(t, spans[0].Tag("machinery.pre_publish.duration_ms"))
	require.Equal(t, "publish", spans[0].Tag("machinery.error.phase"))
	require.Equal(t, true, spans[0].Tag("error"))
}
