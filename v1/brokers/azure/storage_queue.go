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
)

// Broker represents a AWS SQS broker
// There are examples on: https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sqs-example-create-queue.html
type Broker struct {
	common.Broker
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	stopReceivingChan chan int
	cfg               config.AzureConfig
	queueName         string
}

// New creates new Broker instance
func New(cnf *config.Config) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.cfg = *cnf.Azure
	b.queueName = cnf.DefaultQueue

	return b
}

var badRequestErrRegex = regexp.MustCompile(`4[\d][\d]`)

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency iface.ResizeablePool, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, taskProcessor)

	deliveries := make(chan azqueue.DequeueMessagesResponse)

	b.stopReceivingChan = make(chan int)
	b.receivingWG.Add(1)

	go func() {
		defer b.receivingWG.Done()

		log.INFO.Printf("[*] Waiting for messages on queue: %s. To exit press CTRL+C\n", b.queueName)

		pool := concurrency.Pool()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopReceivingChan:
				close(deliveries)

				return
			case <-pool:
				output, err := b.receiveMessage()
				if err == nil && len(output.Messages) > 0 {
					deliveries <- output

				} else {
					if err != nil {
						log.ERROR.Printf("Queue consume error on %s: %s", b.queueName, err)
						if badRequestErrRegex.MatchString(err.Error()) {
							time.Sleep(30 * time.Second)
						}
					}
					//return back to pool right away
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

	result, err := b.cfg.Client.NewQueueClient(signature.RoutingKey).EnqueueMessage(ctx, messageBody, enqueueOptions)

	if err != nil {
		log.ERROR.Printf("Error when sending a message: %v", err)
		return err
	} else if len(result.Messages) != 1 {
		err := fmt.Errorf("unexpected result message count %d", len(result.Messages))
		log.ERROR.Printf("%v", err)
		return err
	}

	log.INFO.Printf("Sending a message successfully, the messageId is %v", *result.Messages[0].MessageID)
	return nil
}

func (b *Broker) extend(by time.Duration, signature *tasks.Signature) error {
	b.AdjustRoutingKey(signature)

	delayS := int32(by.Seconds())

	_, err := b.cfg.Client.NewQueueClient(signature.RoutingKey).UpdateMessage(
		context.Background(),
		signature.AzureMessageID,
		signature.AzurePopReceipt,
		signature.AzureMessageContent,
		&azqueue.UpdateMessageOptions{VisibilityTimeout: &delayS})
	if err != nil {
		log.ERROR.Printf("ignoring error attempting to change visibility timeout. will re-attempt after default period. task %s (%s)", signature.UUID, signature.Name)
	}

	return nil
}

func (b *Broker) RetryMessage(signature *tasks.Signature) {
	b.AdjustRoutingKey(signature)

	delayS := int32(signature.Delay.Seconds())

	_, err := b.cfg.Client.NewQueueClient(signature.RoutingKey).UpdateMessage(
		context.Background(),
		signature.AzureMessageID,
		signature.AzurePopReceipt,
		signature.AzureMessageContent,
		&azqueue.UpdateMessageOptions{VisibilityTimeout: &delayS})
	if err != nil {
		log.ERROR.Printf("ignoring error attempting to change visibility timeout. will re-attempt after default period. task %s (%s)", signature.UUID, signature.Name)
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

	sig := new(tasks.Signature)
	decoder := json.NewDecoder(strings.NewReader(*msg.MessageText))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
		// if the unmarshal fails, remove the delivery from the queue
		if delErr := b.deleteOne(msg); delErr != nil {
			log.ERROR.Printf("error when deleting the delivery. delivery is %v, Error=%s", delivery, delErr)
		}
		return err
	}

	sig.ReceivedAt = time.Now()
	sig.AzureMessageContent = *msg.MessageText

	if msg.PopReceipt != nil {
		sig.AzurePopReceipt = *msg.PopReceipt
	}

	if msg.DequeueCount != nil {
		sig.AttemptCount = int(*msg.DequeueCount) - 1 // receive count includes this attempt, but AttemptCount goes from 0
	}

	sig.IngestionTime = msg.InsertionTime

	// If the task is not registered return an error
	// and leave the message in the queue
	if !b.IsTaskRegistered(sig.Name) {
		if sig.IgnoreWhenTaskNotRegistered {
			b.deleteOne(msg)
		}
		return fmt.Errorf("task %s is not registered", sig.Name)
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

// deleteOne is a method delete a delivery from AWS SQS
func (b *Broker) deleteOne(message *azqueue.DequeuedMessage) error {
	_, err := b.cfg.Client.NewQueueClient(b.queueName).DeleteMessage(context.Background(), *message.MessageID, *message.PopReceipt, nil)

	if err != nil {
		return err
	}
	return nil
}

// receiveMessage is a method receives a message from specified queue url
func (b *Broker) receiveMessage() (azqueue.DequeueMessagesResponse, error) {
	visibilityTimeoutS := int32(b.cfg.VisibilityTimeout.Seconds())
	result, err := b.cfg.Client.NewQueueClient(b.queueName).DequeueMessage(context.Background(), &azqueue.DequeueMessageOptions{VisibilityTimeout: &visibilityTimeoutS})
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

// stopReceiving is a method sending a signal to stopReceivingChan
func (b *Broker) stopReceiving() {
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
}
