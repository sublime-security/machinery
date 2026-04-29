package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

// mockQueueClient implements queueClient for tests.
type mockQueueClient struct {
	enqueueFunc func(ctx context.Context, content string, opts *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error)
	dequeueFunc func(ctx context.Context, opts *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error)
	updateFunc  func(ctx context.Context, messageID, popReceipt, content string, opts *azqueue.UpdateMessageOptions) (azqueue.UpdateMessageResponse, error)
	deleteFunc  func(ctx context.Context, messageID, popReceipt string, opts *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error)
}

func (m *mockQueueClient) EnqueueMessage(ctx context.Context, content string, opts *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
	if m.enqueueFunc != nil {
		return m.enqueueFunc(ctx, content, opts)
	}
	return azqueue.EnqueueMessagesResponse{}, nil
}

func (m *mockQueueClient) DequeueMessage(ctx context.Context, opts *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error) {
	if m.dequeueFunc != nil {
		return m.dequeueFunc(ctx, opts)
	}
	return azqueue.DequeueMessagesResponse{}, nil
}

func (m *mockQueueClient) UpdateMessage(ctx context.Context, messageID, popReceipt, content string, opts *azqueue.UpdateMessageOptions) (azqueue.UpdateMessageResponse, error) {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, messageID, popReceipt, content, opts)
	}
	return azqueue.UpdateMessageResponse{}, nil
}

func (m *mockQueueClient) DeleteMessage(ctx context.Context, messageID, popReceipt string, opts *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, messageID, popReceipt, opts)
	}
	return azqueue.DeleteMessageResponse{}, nil
}

// mockTaskProcessor implements iface.TaskProcessor for tests.
type mockTaskProcessor struct {
	processFunc func(sig *tasks.Signature, extendFunc tasks.ExtendForSignatureFunc) error
}

func (m *mockTaskProcessor) Process(sig *tasks.Signature, extendFunc tasks.ExtendForSignatureFunc) error {
	if m.processFunc != nil {
		return m.processFunc(sig, extendFunc)
	}
	return nil
}

func (m *mockTaskProcessor) CustomQueue() string    { return "" }
func (m *mockTaskProcessor) PreConsumeHandler() bool { return true }

func makeDelivery(msgText, msgID, popReceipt string) azqueue.DequeueMessagesResponse {
	return azqueue.DequeueMessagesResponse{
		Messages: []*azqueue.DequeuedMessage{{
			MessageID:   &msgID,
			PopReceipt:  &popReceipt,
			MessageText: &msgText,
		}},
	}
}

func TestBadRequestErrRegex(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input string
		match bool
	}{
		{"400", true},
		{"401", true},
		{"403", true},
		{"404", true},
		{"499", true},
		{"200", false},
		{"301", false},
		{"500", false},
		{"503", false},
		{"some 400 error", true},
		{"some 500 error", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.match, badRequestErrRegex.MatchString(tc.input))
		})
	}
}

func TestConsumeOne_ValidMessage_Success(t *testing.T) {
	sig := &tasks.Signature{Name: "test_task", UUID: "abc123"}
	msgText, _ := json.Marshal(sig)

	var deletedID string
	client := &mockQueueClient{
		deleteFunc: func(_ context.Context, messageID, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			deletedID = messageID
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	broker := NewTestBroker()
	broker.SetRegisteredTaskNames([]string{"test_task"})
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	err := broker.consumeOne(makeDelivery(string(msgText), "msg-id", "pop-receipt"), &mockTaskProcessor{})
	assert.NoError(t, err)
	assert.Equal(t, "msg-id", deletedID)
}

func TestConsumeOne_InvalidJSON(t *testing.T) {
	deleted := false
	client := &mockQueueClient{
		deleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			deleted = true
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	broker := NewTestBroker()
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	err := broker.consumeOne(makeDelivery("not valid json", "msg-id", "pop-receipt"), nil)
	assert.Error(t, err)
	assert.True(t, deleted)
}

func TestConsumeOne_UnregisteredTask(t *testing.T) {
	deleted := false
	client := &mockQueueClient{
		deleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			deleted = true
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	sig := &tasks.Signature{Name: "unknown_task", UUID: "abc123"}
	msgText, _ := json.Marshal(sig)

	broker := NewTestBroker()
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	err := broker.consumeOne(makeDelivery(string(msgText), "msg-id", "pop-receipt"), nil)
	assert.ErrorContains(t, err, "is not registered")
	assert.False(t, deleted)
}

func TestConsumeOne_IgnoreWhenNotRegistered(t *testing.T) {
	deleted := false
	client := &mockQueueClient{
		deleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			deleted = true
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	sig := &tasks.Signature{Name: "unknown_task", UUID: "abc123", IgnoreWhenTaskNotRegistered: true}
	msgText, _ := json.Marshal(sig)

	broker := NewTestBroker()
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	err := broker.consumeOne(makeDelivery(string(msgText), "msg-id", "pop-receipt"), nil)
	assert.ErrorContains(t, err, "is not registered")
	assert.True(t, deleted)
}

func TestConsumeOne_StopTaskDeletion(t *testing.T) {
	deleted := false
	client := &mockQueueClient{
		deleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			deleted = true
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	tp := &mockTaskProcessor{
		processFunc: func(_ *tasks.Signature, _ tasks.ExtendForSignatureFunc) error {
			return errs.ErrStopTaskDeletion
		},
	}

	sig := &tasks.Signature{Name: "test_task", UUID: "abc123"}
	msgText, _ := json.Marshal(sig)

	broker := NewTestBroker()
	broker.SetRegisteredTaskNames([]string{"test_task"})
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	err := broker.consumeOne(makeDelivery(string(msgText), "msg-id", "pop-receipt"), tp)
	assert.NoError(t, err)
	assert.False(t, deleted)
}

func TestPublish_NoDelay(t *testing.T) {
	var capturedContent string
	var capturedOpts *azqueue.EnqueueMessageOptions
	msgID := "msg-id"

	client := &mockQueueClient{
		enqueueFunc: func(_ context.Context, content string, opts *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			capturedContent = content
			capturedOpts = opts
			return azqueue.EnqueueMessagesResponse{
				Messages: []*azqueue.EnqueuedMessage{{MessageID: &msgID}},
			}, nil
		},
	}

	broker := NewTestBroker()
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	sig := &tasks.Signature{Name: "test_task", UUID: "abc123"}
	err := broker.Publish(context.Background(), sig)
	assert.NoError(t, err)

	var decoded tasks.Signature
	assert.NoError(t, json.Unmarshal([]byte(capturedContent), &decoded))
	assert.Equal(t, "test_task", decoded.Name)
	assert.Nil(t, capturedOpts.VisibilityTimeout)
}

func TestPublish_TTL(t *testing.T) {
	var capturedOpts *azqueue.EnqueueMessageOptions
	msgID := "msg-id"

	client := &mockQueueClient{
		enqueueFunc: func(_ context.Context, _ string, opts *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			capturedOpts = opts
			return azqueue.EnqueueMessagesResponse{
				Messages: []*azqueue.EnqueuedMessage{{MessageID: &msgID}},
			}, nil
		},
	}

	broker := NewTestBroker()
	broker.cfg.TTL = time.Hour
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	sig := &tasks.Signature{Name: "test_task", UUID: "abc123"}
	err := broker.Publish(context.Background(), sig)
	assert.NoError(t, err)
	assert.Equal(t, int32(3600), *capturedOpts.TimeToLive)
}

func TestPublish_WithDelay(t *testing.T) {
	var capturedOpts *azqueue.EnqueueMessageOptions
	msgID := "msg-id"

	client := &mockQueueClient{
		enqueueFunc: func(_ context.Context, _ string, opts *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			capturedOpts = opts
			return azqueue.EnqueueMessagesResponse{
				Messages: []*azqueue.EnqueuedMessage{{MessageID: &msgID}},
			}, nil
		},
	}

	broker := NewTestBroker()
	broker.cfg.TTL = time.Hour
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	sig := &tasks.Signature{Name: "test_task", UUID: "abc123", Delay: 30 * time.Second}
	err := broker.Publish(context.Background(), sig)
	assert.NoError(t, err)
	assert.Equal(t, int32(30), *capturedOpts.VisibilityTimeout)
}

func TestPublish_DelayExceedsMax(t *testing.T) {
	var capturedOpts *azqueue.EnqueueMessageOptions
	msgID := "msg-id"

	client := &mockQueueClient{
		enqueueFunc: func(_ context.Context, _ string, opts *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			capturedOpts = opts
			return azqueue.EnqueueMessagesResponse{
				Messages: []*azqueue.EnqueuedMessage{{MessageID: &msgID}},
			}, nil
		},
	}

	broker := NewTestBroker()
	broker.cfg.TTL = time.Hour
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	sig := &tasks.Signature{Name: "test_task", UUID: "abc123", Delay: maxDelay + time.Second}
	err := broker.Publish(context.Background(), sig)
	assert.NoError(t, err)
	assert.Equal(t, int32(maxDelay.Seconds()), *capturedOpts.VisibilityTimeout)
}

func TestPublish_EnqueueError(t *testing.T) {
	client := &mockQueueClient{
		enqueueFunc: func(_ context.Context, _ string, _ *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error) {
			return azqueue.EnqueueMessagesResponse{}, fmt.Errorf("network error")
		},
	}

	broker := NewTestBroker()
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	sig := &tasks.Signature{Name: "test_task", UUID: "abc123"}
	err := broker.Publish(context.Background(), sig)
	assert.ErrorContains(t, err, "network error")
}

func TestReceiveMessage(t *testing.T) {
	msgID := "msg-id"
	popReceipt := "pop-receipt"
	msgText := "hello"

	var capturedOpts *azqueue.DequeueMessageOptions
	client := &mockQueueClient{
		dequeueFunc: func(_ context.Context, opts *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error) {
			capturedOpts = opts
			return azqueue.DequeueMessagesResponse{
				Messages: []*azqueue.DequeuedMessage{{
					MessageID:   &msgID,
					PopReceipt:  &popReceipt,
					MessageText: &msgText,
				}},
			}, nil
		},
	}

	broker := NewTestBroker()
	broker.cfg.VisibilityTimeout = 30 * time.Second
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	result, err := broker.receiveMessage()
	assert.NoError(t, err)
	assert.Len(t, result.Messages, 1)
	assert.Equal(t, "msg-id", *result.Messages[0].MessageID)
	assert.Equal(t, int32(30), *capturedOpts.VisibilityTimeout)
}

func TestReceiveMessage_Error(t *testing.T) {
	client := &mockQueueClient{
		dequeueFunc: func(_ context.Context, _ *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error) {
			return azqueue.DequeueMessagesResponse{}, fmt.Errorf("dequeue error")
		},
	}

	broker := NewTestBroker()
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	_, err := broker.receiveMessage()
	assert.ErrorContains(t, err, "dequeue error")
}

func TestDeleteOne(t *testing.T) {
	var deletedID, deletedReceipt string
	client := &mockQueueClient{
		deleteFunc: func(_ context.Context, messageID, popReceipt string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			deletedID = messageID
			deletedReceipt = popReceipt
			return azqueue.DeleteMessageResponse{}, nil
		},
	}

	broker := NewTestBroker()
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	msgID := "msg-id"
	popReceipt := "pop-receipt"
	err := broker.deleteOne(&azqueue.DequeuedMessage{MessageID: &msgID, PopReceipt: &popReceipt})
	assert.NoError(t, err)
	assert.Equal(t, "msg-id", deletedID)
	assert.Equal(t, "pop-receipt", deletedReceipt)
}

func TestDeleteOne_Error(t *testing.T) {
	client := &mockQueueClient{
		deleteFunc: func(_ context.Context, _, _ string, _ *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error) {
			return azqueue.DeleteMessageResponse{}, fmt.Errorf("delete error")
		},
	}

	broker := NewTestBroker()
	broker.SetQueueClientFactoryForTest(func(string) queueClient { return client })

	msgID := "msg-id"
	popReceipt := "pop-receipt"
	err := broker.deleteOne(&azqueue.DequeuedMessage{MessageID: &msgID, PopReceipt: &popReceipt})
	assert.ErrorContains(t, err, "delete error")
}
