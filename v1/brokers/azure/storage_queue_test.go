package azure_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/brokers/azure"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validDLQTaskBody is a minimal valid task JSON used in DLQ tests that need a
// decodable message without exercising task processing logic.
const validDLQTaskBody = `{"UUID":"dlq-test-uuid","Name":"unknown-dlq-task"}`

type countingProcessor struct{ count atomic.Int32 }

func (p *countingProcessor) Process(_ *tasks.Signature, _ tasks.ExtendForSignatureFunc) error {
	p.count.Add(1)
	return nil
}
func (p *countingProcessor) CustomQueue() string     { return "" }
func (p *countingProcessor) PreConsumeHandler() bool { return true }

func TestNew_ImplementsInterfaces(t *testing.T) {
	t.Parallel()

	cnf := &config.Config{
		DefaultQueue: "test_queue",
		Azure:        &config.AzureConfig{},
	}
	broker := azure.NewTestBroker()
	assert.IsType(t, broker, azure.New(cnf))

	// Compile-time check that *Broker satisfies RetrySameMessage.
	var asRetry iface.RetrySameMessage
	asRetry = broker
	assert.NotNil(t, asRetry)
}

func TestInitializePool(t *testing.T) {
	broker := azure.NewTestBroker()
	concurrency := 9
	pool := make(chan struct{}, concurrency)
	broker.InitializePoolForTest(pool, concurrency)
	assert.Len(t, pool, concurrency)
}

func TestStopReceiving(t *testing.T) {
	broker := azure.NewTestBroker()
	go broker.StopReceivingForTest()
	stopReceivingChan := broker.GetStopReceivingChanForTest()
	assert.NotNil(t, <-stopReceivingChan)
}

func TestConsumeOne_EmptyMessages(t *testing.T) {
	broker := azure.NewTestBroker()
	err := broker.ConsumeOneForTest(azqueue.DequeueMessagesResponse{}, nil)
	assert.ErrorContains(t, err, "received empty message")
}

type testV2Token struct{ returnFn func() }

func (t *testV2Token) Return() { t.returnFn() }

type testV2Pool struct {
	iface.ResizeablePool
	tokenCh chan iface.Token
}

func (tp *testV2Pool) PoolWithToken() <-chan iface.Token { return tp.tokenCh }

func newTestV2Pool(capacity int) *testV2Pool {
	p, _ := common.NewResizablePool(capacity)
	tp := &testV2Pool{
		ResizeablePool: p,
		tokenCh:        make(chan iface.Token),
	}
	go func() {
		for {
			<-p.Pool()
			tp.tokenCh <- &testV2Token{returnFn: p.Return}
		}
	}()
	return tp
}

func testStartConsumingProcessesTask(t *testing.T, pool iface.ResizeablePool) {
	t.Helper()

	const taskName = "sc-test-task"

	var dequeueCount int32
	client := &azure.MockClient{
		DequeueFunc: func(_ context.Context, _ *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error) {
			if atomic.AddInt32(&dequeueCount, 1) == 1 {
				return azqueue.DequeueMessagesResponse{
					Messages: []*azqueue.DequeuedMessage{{
						MessageID:   new("sc-msg-id"),
						PopReceipt:  new("sc-pop-receipt"),
						MessageText: new(`{"UUID":"sc-test-uuid","Name":"sc-test-task"}`),
					}},
				}, nil
			}
			return azqueue.DequeueMessagesResponse{}, nil
		},
		DeleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	broker := azure.NewTestBroker()
	broker.SetRegisteredTaskNames([]string{taskName})
	broker.SetMockClientForTest(client)

	server, err := machinery.NewServer(&config.Config{
		Broker:        "eager",
		DefaultQueue:  "test_queue",
		ResultBackend: "eager",
	})
	require.NoError(t, err)

	output := make(chan struct{}, 10)
	err = server.RegisterTask(taskName, func(_ context.Context) error {
		output <- struct{}{}
		return nil
	})
	require.NoError(t, err)

	wk := server.NewWorker("sc-worker", 0)

	go broker.StartConsuming("sc-tag", pool, wk)

	select {
	case <-output:
		broker.StopConsuming()
	case <-time.After(5 * time.Second):
		t.Fatal("task was not processed within timeout")
	}
}

func TestStartConsuming_V1_ProcessesTask(t *testing.T) {
	pool, _ := common.NewResizablePool(1)
	testStartConsumingProcessesTask(t, pool)
}

func TestStartConsuming_V2_ProcessesTask(t *testing.T) {
	testStartConsumingProcessesTask(t, newTestV2Pool(1))
}

// TestStopConsuming_CancelsBlockedReceive verifies StopConsuming cancels the receive
// context so an in-flight DequeueMessage unblocks instead of holding shutdown until the
// call returns on its own.
func TestStopConsuming_CancelsBlockedReceive(t *testing.T) {
	entered := make(chan struct{})
	ctxErr := make(chan error, 1)
	var once sync.Once
	client := &azure.MockClient{
		DequeueFunc: func(ctx context.Context, _ *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error) {
			once.Do(func() { close(entered) })
			<-ctx.Done()
			select {
			case ctxErr <- ctx.Err():
			default:
			}
			return azqueue.DequeueMessagesResponse{}, ctx.Err()
		},
	}

	broker := azure.NewTestBroker()
	broker.SetMockClientForTest(client)

	server, err := machinery.NewServer(&config.Config{
		Broker:        "eager",
		DefaultQueue:  "test_queue",
		ResultBackend: "eager",
	})
	require.NoError(t, err)
	wk := server.NewWorker("cancel-worker", 0)

	go broker.StartConsuming("cancel-tag", newTestV2Pool(1), wk) //nolint:errcheck // test, return value not needed

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("DequeueMessage was never entered")
	}

	done := make(chan struct{})
	go func() {
		broker.StopConsuming()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StopConsuming did not return; blocked dequeue was not cancelled")
	}

	select {
	case err := <-ctxErr:
		require.ErrorIs(t, err, context.Canceled)
	default:
		t.Fatal("dequeue did not observe a cancelled context")
	}
}

// TestDeleteOne_UsesBoundedContext verifies deleteOne bounds its DeleteMessage call with a
// deadline. The undecodable message at DequeueCount 15 drives the decode-failure delete path.
func TestDeleteOne_UsesBoundedContext(t *testing.T) {
	var deleteHadDeadline bool
	client := &azure.MockClient{
		DeleteFunc: func(ctx context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			_, deleteHadDeadline = ctx.Deadline()
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	broker := azure.NewTestBroker()
	broker.SetMockClientForTest(client)

	err := broker.ConsumeOneForTest(dlqTestDelivery(15, "not valid json"), nil)
	require.NoError(t, err)
	assert.True(t, deleteHadDeadline, "deleteOne must bound its DeleteMessage call with a deadline")
}

// dlqTestDelivery builds a single-message DequeueMessagesResponse for DLQ tests.
func dlqTestDelivery(dequeueCount int64, body string) azqueue.DequeueMessagesResponse {
	msgID := "test-msg-id"
	popReceipt := "test-pop-receipt"
	return azqueue.DequeueMessagesResponse{
		Messages: []*azqueue.DequeuedMessage{{
			MessageID:    &msgID,
			PopReceipt:   &popReceipt,
			MessageText:  &body,
			DequeueCount: &dequeueCount,
		}},
	}
}

func TestConsumeOne_DLQ_ThresholdTransition(t *testing.T) {
	t.Parallel()

	var dlqEnqueueCalls, sourceDeleteCalls atomic.Int32

	dlqMock := &azure.MockClient{
		EnqueueFunc: func(_ context.Context, _ string, _ *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			dlqEnqueueCalls.Add(1)
			return azqueue.EnqueueMessagesResponse{}, nil
		},
	}
	sourceMock := &azure.MockClient{
		DeleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			sourceDeleteCalls.Add(1)
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	broker := azure.NewTestBroker()
	broker.SetRegisteredTaskNames([]string{"unknown-dlq-task"})
	broker.SetMockClientForTest(sourceMock)
	broker.SetDLQClientForTest(dlqMock, 10, time.Hour)

	processor := &countingProcessor{}

	// DequeueCount=10: at threshold, must NOT redrive — processes normally.
	broker.ConsumeOneForTest(dlqTestDelivery(10, validDLQTaskBody), processor)
	assert.Equal(t, int32(0), dlqEnqueueCalls.Load(), "DequeueCount=10 must not trigger DLQ")
	assert.Equal(t, int32(1), processor.count.Load(), "task should be processed normally at threshold")
	assert.Equal(t, int32(1), sourceDeleteCalls.Load(), "source deleted after normal processing")

	// DequeueCount=11: over threshold, must redrive.
	err := broker.ConsumeOneForTest(dlqTestDelivery(11, validDLQTaskBody), processor)
	require.NoError(t, err)
	assert.Equal(t, int32(1), dlqEnqueueCalls.Load(), "DequeueCount=11 must trigger DLQ")
	assert.Equal(t, int32(1), processor.count.Load(), "message must not be processed when redriven to DLQ")
	assert.Equal(t, int32(2), sourceDeleteCalls.Load(), "source deleted from DLQ path")
}

func TestConsumeOne_DLQ_AboveThreshold_Redrives(t *testing.T) {
	t.Parallel()

	var dlqEnqueueCalls, sourceDeleteCalls atomic.Int32
	var capturedContent string
	var capturedTTL int32

	dlqMock := &azure.MockClient{
		EnqueueFunc: func(_ context.Context, content string, opts *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			dlqEnqueueCalls.Add(1)
			capturedContent = content
			if opts != nil && opts.TimeToLive != nil {
				capturedTTL = *opts.TimeToLive
			}
			return azqueue.EnqueueMessagesResponse{}, nil
		},
	}
	sourceMock := &azure.MockClient{
		DeleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			sourceDeleteCalls.Add(1)
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	broker := azure.NewTestBroker()
	broker.SetMockClientForTest(sourceMock)
	broker.SetDLQClientForTest(dlqMock, 10, time.Hour)

	processor := &countingProcessor{}
	body := validDLQTaskBody
	err := broker.ConsumeOneForTest(dlqTestDelivery(11, body), processor)

	require.NoError(t, err)
	assert.Equal(t, int32(1), dlqEnqueueCalls.Load())
	assert.Equal(t, body, capturedContent, "DLQ should receive the original message body")
	assert.Equal(t, int32(time.Hour.Seconds()), capturedTTL, "DLQ enqueue should use the configured TTL")
	assert.Equal(t, int32(0), processor.count.Load(), "message must not be processed when redriven to DLQ")
	assert.Equal(t, int32(1), sourceDeleteCalls.Load())
}

func TestConsumeOne_DLQ_EnqueueFails_RetrySucceeds(t *testing.T) {
	// Not parallel: broker state is mutated between the two consumeOne calls.
	var dlqEnqueueCalls, sourceDeleteCalls atomic.Int32

	sourceMock := &azure.MockClient{
		DeleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			sourceDeleteCalls.Add(1)
			return azqueue.DeleteMessageResponse{}, nil
		},
	}
	failingDLQ := &azure.MockClient{
		EnqueueFunc: func(_ context.Context, _ string, _ *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			dlqEnqueueCalls.Add(1)
			return azqueue.EnqueueMessagesResponse{}, errors.New("DLQ unavailable")
		},
	}

	broker := azure.NewTestBroker()
	broker.SetMockClientForTest(sourceMock)
	broker.SetDLQClientForTest(failingDLQ, 10, time.Hour)

	processor := &countingProcessor{}
	delivery := dlqTestDelivery(11, validDLQTaskBody)

	// First attempt: DLQ enqueue fails — consumer must survive.
	err := broker.ConsumeOneForTest(delivery, processor)
	require.NoError(t, err, "consumer must survive DLQ enqueue failure")
	assert.Equal(t, int32(1), dlqEnqueueCalls.Load(), "DLQ enqueue was attempted")
	assert.Equal(t, int32(0), sourceDeleteCalls.Load(), "source must not be deleted when DLQ fails")
	assert.Equal(t, int32(0), processor.count.Load(), "message must not be processed while DLQ redrive is in progress")

	// Simulate visibility-timeout redelivery: swap in a working DLQ client.
	workingDLQ := &azure.MockClient{
		EnqueueFunc: func(_ context.Context, _ string, _ *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			dlqEnqueueCalls.Add(1)
			return azqueue.EnqueueMessagesResponse{}, nil
		},
	}
	broker.SetDLQClientForTest(workingDLQ, 10, time.Hour)

	err = broker.ConsumeOneForTest(delivery, processor)
	require.NoError(t, err)
	assert.Equal(t, int32(2), dlqEnqueueCalls.Load(), "DLQ enqueue retried on second attempt")
	assert.Equal(t, int32(1), sourceDeleteCalls.Load(), "source deleted after successful DLQ enqueue")
	assert.Equal(t, int32(0), processor.count.Load(), "message must not be processed when redriven to DLQ")
}

func TestConsumeOne_DLQ_DeleteFails_RetryEnqueuesDuplicate(t *testing.T) {
	// Not parallel: broker state is mutated between the two consumeOne calls.
	var dlqEnqueueCalls, sourceDeleteCalls atomic.Int32

	dlqMock := &azure.MockClient{
		EnqueueFunc: func(_ context.Context, _ string, _ *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			dlqEnqueueCalls.Add(1)
			return azqueue.EnqueueMessagesResponse{}, nil
		},
	}
	failingSourceMock := &azure.MockClient{
		DeleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			sourceDeleteCalls.Add(1)
			return azqueue.DeleteMessageResponse{}, errors.New("source delete failed")
		},
	}

	broker := azure.NewTestBroker()
	broker.SetMockClientForTest(failingSourceMock)
	broker.SetDLQClientForTest(dlqMock, 10, time.Hour)

	processor := &countingProcessor{}
	delivery := dlqTestDelivery(11, validDLQTaskBody)

	// First attempt: DLQ enqueue OK, source delete fails — consumer must survive.
	err := broker.ConsumeOneForTest(delivery, processor)
	require.NoError(t, err, "consumer must survive source delete failure")
	assert.Equal(t, int32(1), dlqEnqueueCalls.Load(), "DLQ enqueue succeeded")
	assert.Equal(t, int32(1), sourceDeleteCalls.Load(), "source delete was attempted")
	assert.Equal(t, int32(0), processor.count.Load(), "message must not be processed while DLQ redrive is in progress")

	// Simulate visibility-timeout redelivery: swap in a working source mock.
	workingSourceMock := &azure.MockClient{
		DeleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			sourceDeleteCalls.Add(1)
			return azqueue.DeleteMessageResponse{}, nil
		},
	}
	broker.SetMockClientForTest(workingSourceMock)

	err = broker.ConsumeOneForTest(delivery, processor)
	require.NoError(t, err)
	// DLQ receives a duplicate — this is the documented at-least-once semantic.
	assert.Equal(t, int32(2), dlqEnqueueCalls.Load(), "DLQ receives a duplicate on retry (at-least-once)")
	assert.Equal(t, int32(2), sourceDeleteCalls.Load(), "source successfully deleted on retry")
	assert.Equal(t, int32(0), processor.count.Load(), "message must not be processed when redriven to DLQ")
}

func TestConsumeOne_DLQ_Disabled_IgnoresDequeueCount(t *testing.T) {
	t.Parallel()

	var sourceDeleteCalls atomic.Int32

	sourceMock := &azure.MockClient{
		DeleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			sourceDeleteCalls.Add(1)
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	broker := azure.NewTestBroker() // no DLQ configured
	broker.SetRegisteredTaskNames([]string{"unknown-dlq-task"})
	broker.SetMockClientForTest(sourceMock)

	processor := &countingProcessor{}
	// Very high DequeueCount, but DLQ is not configured — normal processing path is taken.
	broker.ConsumeOneForTest(dlqTestDelivery(100, validDLQTaskBody), processor)

	assert.Equal(t, int32(1), processor.count.Load(), "task was processed normally")
	assert.Equal(t, int32(1), sourceDeleteCalls.Load(), "source deleted after normal processing")
}


