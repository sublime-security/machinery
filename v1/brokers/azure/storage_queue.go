package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

const (
	maxDelay = time.Minute * 7 // Max supported Visibility Timeout
	// decodeFailureDeleteThreshold is the DequeueCount at which an undecodable
	// message is deleted. Set above the default DLQ threshold (10) so that
	// queues with a DLQ still route there first.
	decodeFailureDeleteThreshold int64 = 15
	// lifecycleCallTimeout bounds post-receive calls (delete, DLQ, retry) so a wedged
	// request can't pin a concurrency slot for the container's life and starve the pool.
	// 30s clears the azcore retry budget (~9s) and stays under the visibility timeout.
	lifecycleCallTimeout = 30 * time.Second
	// receiveCallTimeout bounds a wedged DequeueMessage so a hung receive can't hold a
	// concurrency slot. Derived from consumeCtx, so shutdown still cancels it at once.
	receiveCallTimeout = 30 * time.Second
)

type queueClient interface {
	EnqueueMessage(ctx context.Context, content string, o *azqueue.EnqueueMessageOptions) (azqueue.EnqueueMessagesResponse, error)
	DequeueMessage(ctx context.Context, o *azqueue.DequeueMessageOptions) (azqueue.DequeueMessagesResponse, error)
	UpdateMessage(ctx context.Context, messageID string, popReceipt string, content string, o *azqueue.UpdateMessageOptions) (azqueue.UpdateMessageResponse, error)
	DeleteMessage(ctx context.Context, messageID string, popReceipt string, o *azqueue.DeleteMessageOptions) (azqueue.DeleteMessageResponse, error)
}

// Broker represents an Azure Storage Queue broker
type Broker struct {
	common.Broker
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	stopReceivingChan chan int
	cfg               config.AzureConfig
	queueName         string
	newQueueClient    func(string) queueClient
	dlqClient         queueClient   // nil ⇒ DLQ disabled
	maxReceives       int64
	dlqTTL            time.Duration

	// consumeCtx is cancelled by StopConsuming so an in-flight DequeueMessage returns
	// at once and shutdown stops pulling work it can't finish in the ~30s grace.
	consumeCtx    context.Context
	cancelConsume context.CancelFunc
}

// New creates new Broker instance
func New(cnf *config.Config) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	// StartConsuming replaces this with the per-session cancellable context.
	b.consumeCtx, b.cancelConsume = context.WithCancel(context.Background())
	b.cfg = *cnf.Azure
	b.queueName = cnf.DefaultQueue
	b.newQueueClient = func(name string) queueClient {
		return cnf.Azure.Client.NewQueueClient(name)
	}
	if cnf.Azure.DLQ != nil {
		b.dlqClient = cnf.Azure.DLQ
		b.maxReceives = cnf.Azure.MaxReceives
		if b.maxReceives <= 0 {
			b.maxReceives = 10
		}
		b.dlqTTL = cnf.Azure.DLQTTL
		if b.dlqTTL <= 0 {
			b.dlqTTL = 30 * 24 * time.Hour
		}
	}

	return b
}

var badRequestErrRegex = regexp.MustCompile(`4\d\d`)

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency iface.ResizeablePool, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, taskProcessor)

	b.consumeCtx, b.cancelConsume = context.WithCancel(context.Background())
	// Release the context on every return path; StopConsuming also cancels eagerly to unblock an in-flight receive.
	defer b.cancelConsume()
	b.stopReceivingChan = make(chan int)
	b.receivingWG.Add(1)

	log.DEBUG.Printf("[*] Waiting for messages on queue: %s. To exit press CTRL+C\n", b.queueName)

	if v2, ok := concurrency.(iface.ResizeablePoolV2); ok {
		defer b.receivingWG.Done()
		tokenPool := v2.PoolWithToken()
		errorsChan := make(chan error)
		defer func() {
			b.processingWG.Wait()
			close(errorsChan)
		}()

		for {
			select {
			case workerError := <-errorsChan:
				return b.GetRetry(), workerError
			case <-b.stopReceivingChan:
				return false, nil
			case <-b.consumeCtx.Done():
				// Exit on cancellation alone, without relying on a stopReceivingChan signal.
				return false, nil
			case token := <-tokenPool:
				output, err := b.receiveMessage()
				if err == nil && len(output.Messages) > 0 {
					b.processingWG.Add(1)
					go func() {
						if err := b.consumeOne(output, taskProcessor); err != nil {
							errorsChan <- err
						}
						token.Return()
						b.processingWG.Done()
					}()
				} else {
					// A cancelled consumeCtx surfaces as a receive error, so b.consumeCtx.Err() != nil implies err != nil.
					if err != nil && b.consumeCtx.Err() == nil {
						log.ERROR.Printf("Queue consume error on %s: %s", b.queueName, err)
						if badRequestErrRegex.MatchString(err.Error()) {
							time.Sleep(30 * time.Second)
						}
					} else if err == nil {
						// No messages, prevent fast looping
						time.Sleep(100 * time.Millisecond)
					}
					// On shutdown cancellation, skip the log; the next pass takes the consumeCtx.Done() case.
					token.Return()
				}
			}
		}
	}

	// V1 path
	deliveries := make(chan azqueue.DequeueMessagesResponse)

	go func() {
		defer b.receivingWG.Done()

		pool := concurrency.Pool()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopReceivingChan:
				close(deliveries)

				return
			case <-b.consumeCtx.Done():
				// Exit on cancellation alone, without relying on a stopReceivingChan signal.
				close(deliveries)

				return
			case <-pool:
				output, err := b.receiveMessage()
				if err == nil && len(output.Messages) > 0 {
					deliveries <- output
				} else {
					// A cancelled consumeCtx surfaces as a receive error, so b.consumeCtx.Err() != nil implies err != nil.
					if err != nil && b.consumeCtx.Err() == nil {
						log.ERROR.Printf("Queue consume error on %s: %s", b.queueName, err)
						if badRequestErrRegex.MatchString(err.Error()) {
							time.Sleep(30 * time.Second)
						}
					} else if err == nil {
						// No messages, prevent fast looping
						time.Sleep(100 * time.Millisecond)
					}
					// On shutdown cancellation, skip the log; the next pass takes the consumeCtx.Done() case.
					concurrency.Return()
				}
			}
		}
	}()

	if err := b.consume(deliveries, taskProcessor, concurrency); err != nil {
		return b.GetRetry(), err
	}

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()

	// Cancel before signalling stop so an in-flight DequeueMessage unblocks and the loop sees stopReceivingChan.
	b.cancelConsume()

	b.stopReceiving()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()
}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check that signature.RoutingKey is set, if not switch to DefaultQueue
	b.AdjustRoutingKey(signature)

	messageBody := string(msg)
	enqueueOptions := &azqueue.EnqueueMessageOptions{}

	// Check the delay signature field, if it is set and it is in the future,
	// and is not a fifo queue, set a delay in seconds for the task.
	if signature.Delay > 0 {
		if signature.Delay > maxDelay {
			log.ERROR.Printf("max visibility timeout exceeded sending %s. defaulting to max.", signature.Name)
			signature.Delay = maxDelay
		}
		delaysS := int32(signature.Delay.Seconds())
		enqueueOptions.VisibilityTimeout = &delaysS
	}

	ttlSeconds := int32(b.cfg.TTL.Seconds())
	enqueueOptions.TimeToLive = &ttlSeconds

	result, err := b.newQueueClient(signature.RoutingKey).EnqueueMessage(ctx, messageBody, enqueueOptions)

	if err != nil {
		log.ERROR.Printf("Error when sending a message: %v", err)
		return err
	} else if len(result.Messages) != 1 {
		err := fmt.Errorf("unexpected result message count %d", len(result.Messages))
		log.ERROR.Printf("%v", err)
		return err
	}

	log.DEBUG.Printf("Sending a message successfully, the messageId is %v", *result.Messages[0].MessageID)
	return nil
}

func (b *Broker) extend(by time.Duration, signature *tasks.Signature) error {
	b.AdjustRoutingKey(signature)

	delayS := int32(by.Seconds())

	_, err := b.newQueueClient(signature.RoutingKey).UpdateMessage(
		context.Background(),
		signature.AzureMessageID,
		signature.AzurePopReceipt,
		signature.AzureMessageContent,
		&azqueue.UpdateMessageOptions{VisibilityTimeout: &delayS})
	if err != nil {
		log.ERROR.Printf("ignoring error attempting to extend visibility timeout. will re-attempt after default period. task %s (%s): %s", signature.UUID, signature.Name, err)
	}

	return nil
}

func (b *Broker) RetryMessage(signature *tasks.Signature) {
	b.AdjustRoutingKey(signature)

	delayS := int32(signature.Delay.Seconds())

	// If this call times out, the message redelivers on its existing visibility timeout rather than this backoff.
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleCallTimeout)
	defer cancel()
	_, err := b.newQueueClient(signature.RoutingKey).UpdateMessage(
		ctx,
		signature.AzureMessageID,
		signature.AzurePopReceipt,
		signature.AzureMessageContent,
		&azqueue.UpdateMessageOptions{VisibilityTimeout: &delayS})
	if err != nil {
		log.ERROR.Printf("ignoring error attempting to change visibility timeout. will re-attempt after default period. task %s (%s): %s", signature.UUID, signature.Name, err)
	}
}

// consume is a method which keeps consuming deliveries from a channel, until there is an error or a stop signal
func (b *Broker) consume(deliveries <-chan azqueue.DequeueMessagesResponse, taskProcessor iface.TaskProcessor, concurrency iface.ResizeablePool) error {
	errorsChan := make(chan error)

	for {
		whetherContinue, err := b.consumeDeliveries(deliveries, taskProcessor, concurrency, errorsChan)
		if err != nil {
			return err
		}
		if whetherContinue == false {
			return nil
		}
	}
}

// consumeOne is a method consumes a delivery. If a delivery was consumed successfully, it will be deleted from AWS SQS
func (b *Broker) consumeOne(delivery azqueue.DequeueMessagesResponse, taskProcessor iface.TaskProcessor) error {
	if len(delivery.Messages) == 0 {
		log.ERROR.Printf("received an empty message, the delivery was %v", delivery)
		return fmt.Errorf("received empty message, the delivery is %v", delivery)
	}

	msg := delivery.Messages[0]

	if b.dlqClient != nil && msg.DequeueCount != nil && *msg.DequeueCount > b.maxReceives {
		if err := b.dlqOne(msg); err != nil {
			log.ERROR.Printf("error enqueueing message %s to DLQ: %s", *msg.MessageID, err)
			return nil // leave on source; visibility timeout will retry
		}
		if err := b.deleteOne(msg); err != nil {
			log.ERROR.Printf("error deleting message %s from source after DLQ enqueue: %s", *msg.MessageID, err)
			return nil // duplicate in DLQ on next attempt; do not exit consume loop
		}
		log.INFO.Printf("moved message %s to DLQ after %d receives", *msg.MessageID, *msg.DequeueCount)
		return nil
	}

	sig := new(tasks.Signature)
	decoder := json.NewDecoder(strings.NewReader(*msg.MessageText))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
		if msg.DequeueCount != nil && *msg.DequeueCount >= decodeFailureDeleteThreshold {
			if delErr := b.deleteOne(msg); delErr != nil {
				log.ERROR.Printf("error when deleting the delivery. delivery is %v, Error=%s", delivery, delErr)
			}
		}
		// Never return an error — doing so would kill the consumer loop.
		return nil
	}

	sig.ReceivedAt = time.Now()
	sig.AzureMessageContent = *msg.MessageText

	if sig.UUID == "" {
		// No UUID means the "task" was injected outside of machinery and we can't track state since there's no ID
		// to track against.
		sig.NoBackend = true
	}

	if msg.PopReceipt != nil {
		sig.AzurePopReceipt = *msg.PopReceipt
	}

	if msg.DequeueCount != nil {
		sig.AttemptCount = int(*msg.DequeueCount) - 1 // receive count includes this attempt, but AttemptCount goes from 0
	}

	sig.IngestionTime = msg.InsertionTime

	// If the task is not registered, invoke the hook (if set) to decide whether
	// to keep or drop the message. Never return an error — doing so would kill
	// the consumer loop.
	if !b.IsTaskRegistered(sig.Name) {
		if h := b.GetUnknownTaskHandler(); h != nil {
			keep, retryIn := h(sig)
			if !keep {
				b.deleteOne(msg)
			} else if retryIn > 0 {
				if retryIn > maxDelay {
					retryIn = maxDelay
				}
				sig.RoutingKey = b.queueName
				sig.Delay = retryIn
				b.RetryMessage(sig)
			}
		} else if sig.IgnoreWhenTaskNotRegistered {
			b.deleteOne(msg)
		}
		return nil
	}

	// Always set the routing key based on the processor. This ensures it's set to the queue it's pulled off of, even
	// if the message was originally on another queue (it can be moved automatically to a DLQ).
	sig.RoutingKey = b.queueName

	err := taskProcessor.Process(sig, b.extend)
	if err != nil {
		// stop task deletion in case we want to send messages to dlq in sqs or retry from visibility timeout
		if err == errs.ErrStopTaskDeletion {
			return nil
		}
		return err
	}
	// Delete message after successfully consuming and processing the message
	if err = b.deleteOne(msg); err != nil {
		log.ERROR.Printf("error when deleting the delivery. delivery is %v, Error=%s", delivery, err)
	}
	return err
}

// dlqOne enqueues a message to the configured DLQ.
func (b *Broker) dlqOne(msg *azqueue.DequeuedMessage) error {
	// If this call times out, the message stays on the source queue and is retried.
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleCallTimeout)
	defer cancel()
	ttlSeconds := int32(b.dlqTTL.Seconds())
	_, err := b.dlqClient.EnqueueMessage(
		ctx,
		*msg.MessageText,
		&azqueue.EnqueueMessageOptions{TimeToLive: &ttlSeconds},
	)
	return err
}

// deleteOne is a method delete a delivery from AWS SQS
func (b *Broker) deleteOne(message *azqueue.DequeuedMessage) error {
	// If this call times out, the completed task's message redelivers — callers must already
	// tolerate reprocessing, since the queue doesn't guarantee exactly-once delivery.
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleCallTimeout)
	defer cancel()
	_, err := b.newQueueClient(b.queueName).DeleteMessage(ctx, *message.MessageID, *message.PopReceipt, nil)
	if err != nil {
		return err
	}
	return nil
}

// receiveMessage is a method receives a message from specified queue url
func (b *Broker) receiveMessage() (azqueue.DequeueMessagesResponse, error) {
	ctx, cancel := context.WithTimeout(b.consumeCtx, receiveCallTimeout)
	defer cancel()
	visibilityTimeoutS := int32(b.cfg.VisibilityTimeout.Seconds())
	result, err := b.newQueueClient(b.queueName).DequeueMessage(ctx, &azqueue.DequeueMessageOptions{VisibilityTimeout: &visibilityTimeoutS})
	if err != nil {
		return azqueue.DequeueMessagesResponse{}, err
	}

	return result, nil
}

// initializePool is a method which initializes concurrency pool
func (b *Broker) initializePool(pool chan struct{}, concurrency int) {
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}
}

// consumeDeliveries is a method consuming deliveries from deliveries channel
func (b *Broker) consumeDeliveries(deliveries <-chan azqueue.DequeueMessagesResponse, taskProcessor iface.TaskProcessor, concurrency iface.ResizeablePool, errorsChan chan error) (bool, error) {
	select {
	case err := <-errorsChan:
		return false, err
	case d := <-deliveries:

		b.processingWG.Add(1)

		// Consume the task inside a goroutine so multiple tasks
		// can be processed concurrently
		go func() {
			if err := b.consumeOne(d, taskProcessor); err != nil {
				errorsChan <- err
			}

			b.processingWG.Done()

			// give worker back to pool
			concurrency.Return()
		}()
	case <-b.GetStopChan():
		return false, nil
	}
	return true, nil
}

// stopReceiving signals the receive loop to stop. The consumeCtx.Done() case keeps
// this from blocking if the loop already exited on cancellation.
func (b *Broker) stopReceiving() {
	select {
	case b.stopReceivingChan <- 1:
	case <-b.consumeCtx.Done():
	}
}
