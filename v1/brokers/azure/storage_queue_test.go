package azure_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/brokers/azure"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	taskMsg := `{"UUID":"sc-test-uuid","Name":"sc-test-task"}`
	msgID := "sc-msg-id"
	popReceipt := "sc-pop-receipt"

	var dequeueCount int32
	client := &azure.MockClient{
		DequeueFunc: func(_ context.Context, _ *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error) {
			if atomic.AddInt32(&dequeueCount, 1) == 1 {
				return azqueue.DequeueMessagesResponse{
					Messages: []*azqueue.DequeuedMessage{{
						MessageID:   &msgID,
						PopReceipt:  &popReceipt,
						MessageText: &taskMsg,
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

	go broker.StartConsuming("sc-tag", pool, wk) //nolint:errcheck

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
