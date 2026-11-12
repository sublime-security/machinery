package sqs

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/require"
)

type publishSQS struct {
	sqsiface.SQSAPI
	send func(aws.Context, *awssqs.SendMessageInput, ...request.Option) (*awssqs.SendMessageOutput, error)
}

func (f *publishSQS) SendMessageWithContext(ctx aws.Context, in *awssqs.SendMessageInput, opts ...request.Option) (*awssqs.SendMessageOutput, error) {
	return f.send(ctx, in, opts...)
}
func publishBroker(client sqsiface.SQSAPI, endpoint string) *Broker {
	return &Broker{Broker: common.NewBroker(&config.Config{Broker: endpoint, DefaultQueue: "default"}), service: client}
}
func publishTracer(t *testing.T) *mocktracer.MockTracer {
	t.Helper()
	old := opentracing.GlobalTracer()
	tr := mocktracer.New()
	opentracing.SetGlobalTracer(tr)
	t.Cleanup(func() { opentracing.SetGlobalTracer(old) })
	return tr
}
func onlyPublishSpan(t *testing.T, tr *mocktracer.MockTracer) *mocktracer.MockSpan {
	t.Helper()
	spans := tr.FinishedSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "sqs.Publish", spans[0].OperationName)
	return spans[0]
}

func TestPublishTracingPreservesContextAndInput(t *testing.T) {
	tr := publishTracer(t)
	parent := tr.StartSpan("producer").(*mocktracer.MockSpan)
	type key struct{}
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.WithValue(opentracing.ContextWithSpan(context.Background(), parent), key{}, "value"), deadline)
	defer cancel()
	var callbacks int
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { callbacks++ }})
	sig := &tasks.Signature{UUID: "secret-uuid", Name: "secret-task", RoutingKey: "queue.fifo", BrokerMessageGroupId: "secret-group", Headers: tasks.Headers{"existing": "secret-token"}, Args: []tasks.Arg{{Value: "secret-body"}}}
	original, _ := json.Marshal(sig)
	client := &publishSQS{send: func(received aws.Context, in *awssqs.SendMessageInput, opts ...request.Option) (*awssqs.SendMessageOutput, error) {
		require.Equal(t, "value", received.Value(key{}))
		d, ok := received.Deadline()
		require.True(t, ok)
		require.Equal(t, deadline, d)
		require.Equal(t, ctx.Done(), received.Done())
		require.Equal(t, string(original), aws.StringValue(in.MessageBody))
		require.Equal(t, "http://example/queue.fifo", aws.StringValue(in.QueueUrl))
		require.Equal(t, sig.UUID, aws.StringValue(in.MessageDeduplicationId))
		require.Equal(t, sig.BrokerMessageGroupId, aws.StringValue(in.MessageGroupId))
		require.Len(t, opts, 1)
		httptrace.ContextClientTrace(received).GotConn(httptrace.GotConnInfo{Reused: true, WasIdle: true, IdleTime: time.Second})
		return &awssqs.SendMessageOutput{MessageId: aws.String("message")}, nil
	}}
	require.NoError(t, publishBroker(client, "http://example").Publish(ctx, sig))
	require.Equal(t, 1, callbacks)
	sp := onlyPublishSpan(t, tr)
	require.Equal(t, parent.SpanContext.SpanID, sp.ParentID)
	require.Equal(t, 0, sp.Tag("sqs.sdk_send_attempts")) // a custom implementation did not invoke the SDK handlers
	require.Nil(t, sp.Tag("sqs.send_handlers_sum_ms"))
	require.Nil(t, sp.Tag("sqs.http.dns.sum_ms"))
	for k, v := range sp.Tags() {
		require.NotContains(t, fmt.Sprint(k, v), "secret")
	}
	require.Empty(t, sp.Logs())
}

func TestPublishTracingFailuresBeforeSDK(t *testing.T) {
	for _, tc := range []struct {
		name      string
		signature *tasks.Signature
		phase     string
		want      string
	}{
		{"encode", &tasks.Signature{Args: []tasks.Arg{{Value: make(chan int)}}}, "encode", "JSON marshal error:"},
		{"fifo", &tasks.Signature{RoutingKey: "queue.fifo"}, "input", "please specify BrokerMessageGroupId attribute for task Signature when submitting a task to FIFO queue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := publishTracer(t)
			called := false
			client := &publishSQS{send: func(aws.Context, *awssqs.SendMessageInput, ...request.Option) (*awssqs.SendMessageOutput, error) {
				called = true
				return nil, nil
			}}
			err := publishBroker(client, "http://example").Publish(context.Background(), tc.signature)
			require.ErrorContains(t, err, tc.want)
			require.False(t, called)
			sp := onlyPublishSpan(t, tr)
			require.Equal(t, true, sp.Tag("error"))
			require.Equal(t, tc.phase, sp.Tag("sqs.error_phase"))
			require.Nil(t, sp.Tag("sqs.sdk_ms"))
			require.Equal(t, 0, sp.Tag("sqs.sdk_send_attempts"))
		})
	}
}

func TestPublishTracingPreservesSDKError(t *testing.T) {
	tr := publishTracer(t)
	expected := awserr.New("AccessDenied", "secret token and queue URL", errors.New("secret cause"))
	client := &publishSQS{send: func(aws.Context, *awssqs.SendMessageInput, ...request.Option) (*awssqs.SendMessageOutput, error) {
		return nil, expected
	}}
	require.Same(t, expected, publishBroker(client, "http://example").Publish(context.Background(), &tasks.Signature{}))
	sp := onlyPublishSpan(t, tr)
	require.Equal(t, "AccessDenied", sp.Tag("sqs.aws_error_code"))
	require.Equal(t, "sdk", sp.Tag("sqs.error_phase"))
	require.NotContains(t, fmt.Sprint(sp.Tags(), sp.Logs()), "secret")
}

const sendSuccessXML = `<SendMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><SendMessageResult><MD5OfMessageBody>d41d8cd98f00b204e9800998ecf8427e</MD5OfMessageBody><MessageId>message</MessageId></SendMessageResult><ResponseMetadata><RequestId>request</RequestId></ResponseMetadata></SendMessageResponse>`
const sendErrorXML = `<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><Error><Type>Receiver</Type><Code>InternalError</Code><Message>secret message</Message></Error><RequestId>request</RequestId></ErrorResponse>`

func realPublishClient(t *testing.T, server *httptest.Server, retries int) *awssqs.SQS {
	t.Helper()
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), Credentials: credentials.NewStaticCredentials("test", "test", ""), HTTPClient: server.Client(), MaxRetries: aws.Int(retries), SleepDelay: func(time.Duration) {}})
	require.NoError(t, err)
	return awssqs.New(sess)
}
func TestPublishTracingHTTPFreshReusedAndRetry(t *testing.T) {
	tr := publishTracer(t)
	var calls atomic.Int32
	var receivedBodies []string
	var requestMu sync.Mutex
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		form, err := url.ParseQuery(string(b))
		require.NoError(t, err)
		requestMu.Lock()
		receivedBodies = append(receivedBodies, form.Get("MessageBody"))
		requestMu.Unlock()
		require.Empty(t, r.Header.Get("ot-tracer-spanid"))
		require.Empty(t, r.Header.Get("traceparent"))
		require.Empty(t, r.Header.Get("x-datadog-trace-id"))
		w.Header().Set("Content-Type", "text/xml")
		if calls.Add(1) == 3 {
			w.WriteHeader(500)
			fmt.Fprint(w, sendErrorXML)
			return
		}
		fmt.Fprint(w, strings.Replace(sendSuccessXML, "d41d8cd98f00b204e9800998ecf8427e", fmt.Sprintf("%x", md5.Sum([]byte(form.Get("MessageBody")))), 1))
	}))
	defer server.Close()
	broker := publishBroker(realPublishClient(t, server, 1), server.URL)
	sig := &tasks.Signature{UUID: "secret", Args: []tasks.Arg{{Value: "secret"}}}
	original, _ := json.Marshal(sig)
	var existing atomic.Int32
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { existing.Add(1) }})
	for i := 0; i < 3; i++ {
		require.NoError(t, broker.Publish(ctx, sig))
	}
	spans := tr.FinishedSpans()
	require.Len(t, spans, 3)
	fresh, reused, retry := spans[0], spans[1], spans[2]
	require.Equal(t, 1, fresh.Tag("sqs.sdk_send_attempts"))
	require.Equal(t, 0, fresh.Tag("sqs.sdk_retries"))
	require.Equal(t, 1, fresh.Tag("sqs.http.tls.completed"))
	require.NotNil(t, fresh.Tag("sqs.http.tls.sum_ms"))
	require.GreaterOrEqual(t, fresh.Tag("sqs.http.connect.completed").(int), 1)
	require.Equal(t, 0, fresh.Tag("sqs.http.reused_connections"))
	require.Equal(t, 1, reused.Tag("sqs.http.reused_connections"))
	require.Equal(t, 1, reused.Tag("sqs.http.idle_connections"))
	require.Nil(t, reused.Tag("sqs.http.tls.sum_ms"))
	require.Nil(t, reused.Tag("sqs.http.connect.sum_ms"))
	require.Equal(t, 2, retry.Tag("sqs.sdk_send_attempts"))
	require.Equal(t, 1, retry.Tag("sqs.sdk_retries"))
	require.Equal(t, 2, retry.Tag("sqs.http.wrote_requests"))
	require.Equal(t, 2, retry.Tag("sqs.http.first_bytes"))
	require.Equal(t, 200, retry.Tag("http.status_code"))
	require.Nil(t, retry.Tag("error"))
	require.Equal(t, int32(4), existing.Load())
	requestMu.Lock()
	defer requestMu.Unlock()
	require.Len(t, receivedBodies, 4)
	// First call encodes before routing adjustment; later calls encode the already adjusted key.
	require.Equal(t, string(original), receivedBodies[0])
	require.Equal(t, receivedBodies[2], receivedBodies[3])
	for _, sp := range spans {
		require.GreaterOrEqual(t, sp.Tag("sqs.sdk_ms").(float64), sp.Tag("sqs.send_handlers_sum_ms").(float64))
		require.NotNil(t, sp.Tag("sqs.http.send_to_first_byte_sum_ms"))
		require.NotContains(t, fmt.Sprint(sp.Tags(), sp.Logs()), "secret")
	}
}

func TestPublishTracingHTTPErrorAndCancellation(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		tr := publishTracer(t)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500); fmt.Fprint(w, sendErrorXML) }))
		defer server.Close()
		err := publishBroker(realPublishClient(t, server, 1), server.URL).Publish(context.Background(), &tasks.Signature{})
		require.Error(t, err)
		require.Equal(t, "InternalError", err.(awserr.Error).Code())
		sp := onlyPublishSpan(t, tr)
		require.Equal(t, 2, sp.Tag("sqs.sdk_send_attempts"))
		require.Equal(t, 1, sp.Tag("sqs.sdk_retries"))
		require.Equal(t, 500, sp.Tag("http.status_code"))
		require.Equal(t, true, sp.Tag("error"))
		require.Equal(t, "InternalError", sp.Tag("sqs.aws_error_code"))
		require.NotContains(t, fmt.Sprint(sp.Tags(), sp.Logs()), "secret")
	})
	t.Run("cancel", func(t *testing.T) {
		tr := publishTracer(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release }))
		defer server.Close()
		defer close(release)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs := make(chan error, 1)
		go func() {
			errs <- publishBroker(realPublishClient(t, server, 1), server.URL).Publish(ctx, &tasks.Signature{})
		}()
		<-entered
		cancel()
		err := <-errs
		require.Error(t, err)
		require.Equal(t, request.CanceledErrorCode, err.(awserr.Error).Code())
		require.ErrorIs(t, err.(awserr.Error).OrigErr(), context.Canceled)
		sp := onlyPublishSpan(t, tr)
		require.Equal(t, 1, sp.Tag("sqs.sdk_send_attempts"))
		require.Equal(t, 0, sp.Tag("sqs.sdk_retries"))
		require.Nil(t, sp.Tag("sqs.http.send_to_first_byte_sum_ms"))
		require.Equal(t, true, sp.Tag("error"))
	})
}

func TestPublishTraceComposesAllCallbacksAndConcurrentCompletion(t *testing.T) {
	trace := &publishTrace{}
	var old atomic.Int32
	original := &httptrace.ClientTrace{GetConn: func(string) { old.Add(1) }, GotConn: func(httptrace.GotConnInfo) { old.Add(1) }, DNSStart: func(httptrace.DNSStartInfo) { old.Add(1) }, DNSDone: func(httptrace.DNSDoneInfo) { old.Add(1) }, ConnectStart: func(string, string) { old.Add(1) }, ConnectDone: func(string, string, error) { old.Add(1) }, TLSHandshakeStart: func() { old.Add(1) }, TLSHandshakeDone: func(tls.ConnectionState, error) { old.Add(1) }, WroteRequest: func(httptrace.WroteRequestInfo) { old.Add(1) }, GotFirstResponseByte: func() { old.Add(1) }}
	ctx := httptrace.WithClientTrace(httptrace.WithClientTrace(context.Background(), original), trace.clientTrace())
	cb := httptrace.ContextClientTrace(ctx)
	trace.sendStart = time.Now()
	trace.firstByteStart = trace.sendStart
	cb.GetConn("secret")
	cb.GotConn(httptrace.GotConnInfo{})
	cb.DNSStart(httptrace.DNSStartInfo{Host: "secret"})
	cb.DNSDone(httptrace.DNSDoneInfo{})
	cb.ConnectStart("tcp", "secret")
	cb.ConnectDone("tcp", "secret", nil)
	cb.TLSHandshakeStart()
	cb.TLSHandshakeDone(tls.ConnectionState{}, nil)
	cb.WroteRequest(httptrace.WroteRequestInfo{})
	cb.GotFirstResponseByte()
	require.Equal(t, int32(10), old.Load())
	tr := mocktracer.New()
	span := tr.StartSpan("test").(*mocktracer.MockSpan)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb.ConnectStart("tcp", "secret")
			cb.ConnectDone("tcp", "secret", nil)
			trace.finish(span)
		}()
	}
	wg.Wait()
	trace.finish(span)
	span.Finish()
	// Late callbacks after finalization never access the finished span.
	before := span.Tags()
	cb.ConnectStart("tcp", "secret")
	cb.ConnectDone("tcp", "secret", nil)
	require.Equal(t, before, span.Tags())
	require.Equal(t, 51, span.Tag("sqs.http.connect.completed"))
	require.NotContains(t, fmt.Sprint(span.Tags()), "secret")
}
func TestBoundedAWSErrorCode(t *testing.T) {
	require.Equal(t, "other", boundedAWSErrorCode(strings.Repeat("a", 129)))
	require.Equal(t, "other", boundedAWSErrorCode("https://secret"))
	require.Equal(t, "AWS.SimpleQueueService.NonExistentQueue", boundedAWSErrorCode("AWS.SimpleQueueService.NonExistentQueue"))
}

func TestPublishTracingRealDNSAndSDKPreSendFailure(t *testing.T) {
	t.Run("dns", func(t *testing.T) {
		tr := publishTracer(t)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			form, _ := url.ParseQuery(string(b))
			fmt.Fprint(w, strings.Replace(sendSuccessXML, "d41d8cd98f00b204e9800998ecf8427e", fmt.Sprintf("%x", md5.Sum([]byte(form.Get("MessageBody")))), 1))
		}))
		defer server.Close()
		server.URL = strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
		require.NoError(t, publishBroker(realPublishClient(t, server, 0), server.URL).Publish(context.Background(), &tasks.Signature{}))
		sp := onlyPublishSpan(t, tr)
		require.GreaterOrEqual(t, sp.Tag("sqs.http.dns.completed").(int), 1)
		require.NotNil(t, sp.Tag("sqs.http.dns.sum_ms"))
		require.Nil(t, sp.Tag("sqs.http.tls.sum_ms"))
	})
	t.Run("pre_send", func(t *testing.T) {
		tr := publishTracer(t)
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
		defer server.Close()
		client := realPublishClient(t, server, 0)
		expected := awserr.New("ValidationError", "secret details", nil)
		client.Handlers.Validate.PushBack(func(r *request.Request) { r.Error = expected })
		require.Same(t, expected, publishBroker(client, server.URL).Publish(context.Background(), &tasks.Signature{}))
		sp := onlyPublishSpan(t, tr)
		require.Equal(t, int32(0), calls.Load())
		require.Equal(t, 0, sp.Tag("sqs.sdk_send_attempts"))
		require.Equal(t, 0, sp.Tag("sqs.sdk_retries"))
		require.Nil(t, sp.Tag("sqs.send_handlers_sum_ms"))
		require.Equal(t, "ValidationError", sp.Tag("sqs.aws_error_code"))
	})
}

func TestPublishTraceIncompleteAndErrorPhases(t *testing.T) {
	trace := &publishTrace{}
	cb := trace.clientTrace()
	cb.GetConn("secret")
	cb.DNSStart(httptrace.DNSStartInfo{})
	cb.DNSDone(httptrace.DNSDoneInfo{Err: errors.New("secret")})
	cb.ConnectStart("tcp", "secret")
	cb.ConnectStart("tcp", "secret")
	cb.ConnectDone("tcp", "secret", errors.New("secret"))
	cb.TLSHandshakeStart()
	cb.TLSHandshakeDone(tls.ConnectionState{}, errors.New("secret"))
	cb.WroteRequest(httptrace.WroteRequestInfo{Err: errors.New("secret")})
	sp := mocktracer.New().StartSpan("test").(*mocktracer.MockSpan)
	trace.finish(sp)
	require.Nil(t, sp.Tag("sqs.http.get_conn.sum_ms"))
	require.Nil(t, sp.Tag("sqs.http.connect.sum_ms"))
	require.Equal(t, 1, sp.Tag("sqs.http.dns.errors"))
	require.Equal(t, 1, sp.Tag("sqs.http.connect.errors"))
	require.Equal(t, 1, sp.Tag("sqs.http.tls.errors"))
	require.Equal(t, 1, sp.Tag("sqs.http.write_errors"))
	require.NotContains(t, fmt.Sprint(sp.Tags()), "secret")
}

func BenchmarkPublishTracingNoop(b *testing.B) {
	debug := log.DEBUG
	log.SetDebug(stdlog.New(io.Discard, "", 0))
	defer log.SetDebug(debug)
	old := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(opentracing.NoopTracer{})
	defer opentracing.SetGlobalTracer(old)
	client := &publishSQS{send: func(ctx aws.Context, in *awssqs.SendMessageInput, opts ...request.Option) (*awssqs.SendMessageOutput, error) {
		r := &request.Request{}
		for _, opt := range opts {
			opt(r)
		}
		r.Handlers.Send.Run(r)
		cb := httptrace.ContextClientTrace(ctx)
		cb.GetConn("unused")
		cb.GotConn(httptrace.GotConnInfo{Reused: true})
		cb.WroteRequest(httptrace.WroteRequestInfo{})
		cb.GotFirstResponseByte()
		r.Handlers.CompleteAttempt.Run(r)
		r.Handlers.Complete.Run(r)
		// Preserve the production log path, but keep the output deterministic.
		return &awssqs.SendMessageOutput{MessageId: aws.String("message")}, nil
	}}
	broker := publishBroker(client, "http://example")
	sig := &tasks.Signature{RoutingKey: "default"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := broker.Publish(context.Background(), sig); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPublishTraceCustomSendHandlerStopsBeforeEnd(t *testing.T) {
	trace := &publishTrace{}
	r := &request.Request{}
	r.Handlers.Send.PushBack(func(r *request.Request) { r.Error = errors.New("secret") })
	r.Handlers.Send.AfterEachFn = request.HandlerListStopOnError
	trace.requestOption(r)
	r.Handlers.Send.Run(r)
	r.Handlers.CompleteAttempt.Run(r)
	r.Handlers.Complete.Run(r)
	sp := mocktracer.New().StartSpan("test").(*mocktracer.MockSpan)
	trace.finish(sp)
	require.Equal(t, 1, sp.Tag("sqs.sdk_send_attempts"))
	require.Equal(t, 0, sp.Tag("sqs.send_handlers_completed"))
	require.Nil(t, sp.Tag("sqs.send_handlers_sum_ms"))
	require.Nil(t, sp.Tag("http.status_code"))
}

func TestPublishTracingHTTPDeadline(t *testing.T) {
	tr := publishTracer(t)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer server.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := publishBroker(realPublishClient(t, server, 1), server.URL).Publish(ctx, &tasks.Signature{})
	require.Error(t, err)
	require.Equal(t, request.CanceledErrorCode, err.(awserr.Error).Code())
	require.ErrorIs(t, err.(awserr.Error).OrigErr(), context.DeadlineExceeded)
	sp := onlyPublishSpan(t, tr)
	require.Equal(t, true, sp.Tag("error"))
	require.Nil(t, sp.Tag("sqs.http.send_to_first_byte_sum_ms"))
	require.Nil(t, sp.Tag("http.status_code"))
}

func TestPublishTracingSDKIncludesDelayedBodyReads(t *testing.T) {
	tr := publishTracer(t)
	const bodyDelay = 40 * time.Millisecond
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		body := strings.Replace(sendSuccessXML, "d41d8cd98f00b204e9800998ecf8427e", fmt.Sprintf("%x", md5.Sum([]byte(r.Form.Get("MessageBody")))), 1)
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(bodyDelay)
		_, err := io.WriteString(w, body)
		require.NoError(t, err)
	}))
	defer server.Close()
	require.NoError(t, publishBroker(realPublishClient(t, server, 0), server.URL).Publish(t.Context(), &tasks.Signature{}))
	span := onlyPublishSpan(t, tr)
	sdkMS := span.Tag("sqs.sdk_ms").(float64)
	sendMS := span.Tag("sqs.send_handlers_sum_ms").(float64)
	require.GreaterOrEqual(t, sdkMS-sendMS, milliseconds(bodyDelay)*0.75)
	require.Equal(t, 1, span.Tag("sqs.http.first_bytes"))
	require.Equal(t, 200, span.Tag("http.status_code"))
	require.Nil(t, span.Tag("error"))
}
