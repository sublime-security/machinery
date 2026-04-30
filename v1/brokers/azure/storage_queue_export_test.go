package azure

import (
	"context"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
)

func NewTestBroker() *Broker {
	cnf := &config.Config{
		DefaultQueue: "test_queue",
		Azure:        &config.AzureConfig{},
	}
	return &Broker{
		Broker:            common.NewBroker(cnf),
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
		cfg:               *cnf.Azure,
		queueName:         cnf.DefaultQueue,
		newQueueClient:    func(name string) queueClient { return nil },
	}
}

func (b *Broker) SetQueueClientFactoryForTest(fn func(string) queueClient) {
	b.newQueueClient = fn
}

// MockClient is an exported queueClient implementation for use in external test packages.
type MockClient struct {
	EnqueueFunc func(ctx context.Context, content string, opts *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error)
	DequeueFunc func(ctx context.Context, opts *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error)
	UpdateFunc  func(ctx context.Context, messageID, popReceipt, content string, opts *azqueue.UpdateMessageOptions) (azqueue.UpdateMessageResponse, error)
	DeleteFunc  func(ctx context.Context, messageID, popReceipt string, opts *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error)
}

func (m *MockClient) EnqueueMessage(ctx context.Context, content string, opts *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
	if m.EnqueueFunc != nil {
		return m.EnqueueFunc(ctx, content, opts)
	}
	return azqueue.EnqueueMessagesResponse{}, nil
}

func (m *MockClient) DequeueMessage(ctx context.Context, opts *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error) {
	if m.DequeueFunc != nil {
		return m.DequeueFunc(ctx, opts)
	}
	return azqueue.DequeueMessagesResponse{}, nil
}

func (m *MockClient) UpdateMessage(ctx context.Context, messageID, popReceipt, content string, opts *azqueue.UpdateMessageOptions) (azqueue.UpdateMessageResponse, error) {
	if m.UpdateFunc != nil {
		return m.UpdateFunc(ctx, messageID, popReceipt, content, opts)
	}
	return azqueue.UpdateMessageResponse{}, nil
}

func (m *MockClient) DeleteMessage(ctx context.Context, messageID, popReceipt string, opts *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, messageID, popReceipt, opts)
	}
	return azqueue.DeleteMessageResponse{}, nil
}

func (b *Broker) SetMockClientForTest(c *MockClient) {
	b.newQueueClient = func(string) queueClient { return c }
}

func (b *Broker) InitializePoolForTest(pool chan struct{}, concurrency int) {
	b.initializePool(pool, concurrency)
}

func (b *Broker) StopReceivingForTest() {
	b.stopReceiving()
}

func (b *Broker) GetStopReceivingChanForTest() chan int {
	return b.stopReceivingChan
}

func (b *Broker) ConsumeOneForTest(delivery azqueue.DequeueMessagesResponse, taskProcessor iface.TaskProcessor) error {
	return b.consumeOne(delivery, taskProcessor)
}
