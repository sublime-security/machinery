package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

const (
	maxAWSSQSDelay             = time.Minute * 15 // Max supported SQS delay is 15 min: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
	maxAWSSQSVisibilityTimeout = time.Hour * 12   // Max supported SQS visibility timeout is 12 hours: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
)

// Broker represents a AWS SQS broker
// There are examples on: https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sqs-example-create-queue.html
type Broker struct {
	common.Broker
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	stopReceivingChan chan int
	sess              *session.Session
	service           sqsiface.SQSAPI
	queueUrl          *string
}

// New creates new Broker instance
func New(cnf *config.Config) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	if cnf.SQS != nil && cnf.SQS.Client != nil {
		// Use provided *SQS client
		b.service = cnf.SQS.Client
	} else {
		// Initialize a session that the SDK will use to load credentials from the shared credentials file, ~/.aws/credentials.
		// See details on: https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html
		// Also, env AWS_REGION is also required
		b.sess = session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		}))
		b.service = awssqs.New(b.sess)
	}

	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency iface.ResizeablePool, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, taskProcessor)
	qURL := b.getQueueURL(taskProcessor)
	//save it so that it can be used later when attempting to delete task
	b.queueUrl = qURL

	b.stopReceivingChan = make(chan int)
	b.receivingWG.Add(1)

	defer b.receivingWG.Done()

	log.INFO.Printf("[*] Waiting for messages on queue: %s. To exit press CTRL+C\n", *qURL)

	pool := concurrency.Pool()
	log.INFO.Printf("concurrency=%#v pool=%#v", concurrency, pool)

	errorsChan := make(chan error)
	defer close(errorsChan)

	for {
		log.INFO.Printf("StartConsuming is in select loop")
		select {
		case workerError := <-errorsChan:
			log.INFO.Printf("Oh no, an error :(")
			return b.GetRetry(), workerError
		// A way to stop this goroutine from b.StopConsuming
		case <-b.stopReceivingChan:
			// If someone called b.stopReceivingChannel, they are trying to shut down the process
			// so we don't want to retry the StartConsuming call
			return false, nil
		case <-pool:
			log.INFO.Printf("StartConsuming pulled from pool=%#v, calling receiveMessage", pool)
			output, err := b.receiveMessage(qURL)
			log.INFO.Printf("StartConsuming got a receiveMessage")
			if err == nil && len(output.Messages) > 0 {
				b.processingWG.Add(1)
				go func() {
					consumeError := b.consumeOne(output, taskProcessor)
					concurrency.Return()
					b.processingWG.Done()
					if consumeError != nil {
						errorsChan <- consumeError
					}
				}()
			} else {
				if err != nil {
					log.ERROR.Printf("Queue consume error on %s: %s", *qURL, err)

					// Avoid repeating this
					if strings.Contains(err.Error(), "AWS.SimpleQueueService.NonExistentQueue") {
						time.Sleep(30 * time.Second)
					}
				}
				//return back to pool right away
				log.INFO.Println("Returning to concurrency=%#v", concurrency)
				concurrency.Return()
				log.INFO.Println("Returned to concurrency=%#v", concurrency)
			}
		}
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

	MsgInput := &awssqs.SendMessageInput{
		MessageBody: aws.String(string(msg)),
		QueueUrl:    aws.String(b.GetConfig().Broker + "/" + signature.RoutingKey),
	}

	// if this is a fifo queue, there needs to be some additional parameters.
	if strings.HasSuffix(signature.RoutingKey, ".fifo") {
		// Use Machinery's signature Task UUID as SQS Message Group ID.
		MsgDedupID := signature.UUID
		MsgInput.MessageDeduplicationId = aws.String(MsgDedupID)

		// Do not Use Machinery's signature Group UUID as SQS Message Group ID, instead use BrokerMessageGroupId
		MsgGroupID := signature.BrokerMessageGroupId
		if MsgGroupID == "" {
			return fmt.Errorf("please specify BrokerMessageGroupId attribute for task Signature when submitting a task to FIFO queue")
		}
		MsgInput.MessageGroupId = aws.String(MsgGroupID)
	}

	// Check the delay signature field, if it is set and it is in the future,
	// and is not a fifo queue, set a delay in seconds for the task.
	if signature.Delay > 0 && !strings.HasSuffix(signature.RoutingKey, ".fifo") {
		if signature.Delay > maxAWSSQSDelay {
			log.ERROR.Printf("max AWS SQS delay exceeded sending %s. defaulting to max.", signature.Name)
			signature.Delay = maxAWSSQSDelay
		}
		MsgInput.DelaySeconds = aws.Int64(int64(signature.Delay.Seconds()))
	}

	result, err := b.service.SendMessageWithContext(ctx, MsgInput)

	if err != nil {
		log.ERROR.Printf("Error when sending a message: %v", err)
		return err

	}
	log.INFO.Printf("Sending a message successfully, the messageId is %v", *result.MessageId)
	return nil

}

func (b *Broker) extend(by time.Duration, signature *tasks.Signature) error {
	b.AdjustRoutingKey(signature)

	by = restrictVisibilityTimeoutDelay(by, signature.ReceivedAt)

	visibilityInput := &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(b.GetConfig().Broker + "/" + signature.RoutingKey),
		ReceiptHandle:     &signature.SQSReceiptHandle,
		VisibilityTimeout: aws.Int64(int64(by.Seconds())),
	}

	_, err := b.service.ChangeMessageVisibility(visibilityInput)
	return err
}

func (b *Broker) RetryMessage(signature *tasks.Signature) {
	b.AdjustRoutingKey(signature)

	signature.Delay = restrictVisibilityTimeoutDelay(signature.Delay, signature.ReceivedAt)

	visibilityInput := &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(b.GetConfig().Broker + "/" + signature.RoutingKey),
		ReceiptHandle:     &signature.SQSReceiptHandle,
		VisibilityTimeout: aws.Int64(int64(signature.Delay.Seconds())),
	}

	_, err := b.service.ChangeMessageVisibility(visibilityInput)
	if err != nil {
		log.ERROR.Printf("ignoring error attempting to change visibility timeout. will re-attempt after default period. task %s (%s)", signature.UUID, signature.Name)
	}
}

func restrictVisibilityTimeoutDelay(delay time.Duration, receivedAt time.Time) time.Duration {
	if delay > maxAWSSQSVisibilityTimeout {
		log.ERROR.Printf("attempted to retry a message with invalid delay: %s. using max.", delay.String())
		delay = maxAWSSQSVisibilityTimeout
	} else if delay < 0 {
		delay = 0
	}

	// Messages can process a max of 12 hours, and attempting to set the visibility timeout beyond that will
	// result in an error.
	runningTime := time.Since(receivedAt)
	if timeOverMax := (maxAWSSQSVisibilityTimeout - time.Minute) - (runningTime + delay); timeOverMax < 0 {
		delay += timeOverMax

		if delay < 0 {
			delay = 0
		}
	}

	return delay
}

// consume is a method which keeps consuming deliveries from a channel, until there is an error or a stop signal
func (b *Broker) consume(deliveries <-chan *awssqs.ReceiveMessageOutput, taskProcessor iface.TaskProcessor, concurrency iface.ResizeablePool) error {
	log.INFO.Printf("consume(%#v)", deliveries)
	errorsChan := make(chan error)

	for {
		whetherContinue, err := b.consumeDeliveries(deliveries, taskProcessor, concurrency, errorsChan)
		log.INFO.Printf("consume(%#v) whetherContinue=%t err=%#v", deliveries, whetherContinue, err)
		if err != nil {
			return err
		}
		if whetherContinue == false {
			return nil
		}
	}
}

// consumeOne is a method consumes a delivery. If a delivery was consumed successfully, it will be deleted from AWS SQS
func (b *Broker) consumeOne(delivery *awssqs.ReceiveMessageOutput, taskProcessor iface.TaskProcessor) error {
	if len(delivery.Messages) == 0 {
		log.ERROR.Printf("received an empty message, the delivery was %v", delivery)
		return errors.New("received empty message, the delivery is " + delivery.GoString())
	}

	sig := new(tasks.Signature)
	decoder := json.NewDecoder(strings.NewReader(*delivery.Messages[0].Body))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
		// if the unmarshal fails, remove the delivery from the queue
		if delErr := b.deleteOne(delivery); delErr != nil {
			log.ERROR.Printf("error when deleting the delivery. delivery is %v, Error=%s", delivery, delErr)
		}
		return err
	}

	sig.ReceivedAt = time.Now()

	if delivery.Messages[0].ReceiptHandle != nil {
		sig.SQSReceiptHandle = *delivery.Messages[0].ReceiptHandle
	}

	if delivery.Messages[0].MessageId != nil {
		sig.SQSMessageID = *delivery.Messages[0].MessageId
	}

	if receiveCount := delivery.Messages[0].Attributes[awssqs.MessageSystemAttributeNameApproximateReceiveCount]; receiveCount != nil {
		if rc, err := strconv.ParseInt(*receiveCount, 10, 64); err == nil {
			sig.AttemptCount = int(rc) - 1 // SQS receive count includes this attempt, but AttemptCount goes from 0
		}
	}

	sentTimeSinceEpochMilliString := delivery.Messages[0].Attributes[awssqs.MessageSystemAttributeNameSentTimestamp]
	if sentTimeSinceEpochMilliString != nil {
		if i, err := strconv.ParseInt(*sentTimeSinceEpochMilliString, 10, 64); err == nil {
			t := time.UnixMilli(i)
			sig.IngestionTime = &t
		}
	}

	estimateFirstReceivedMilliString := delivery.Messages[0].Attributes[awssqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp]
	if estimateFirstReceivedMilliString != nil {
		if i, err := strconv.ParseInt(*estimateFirstReceivedMilliString, 10, 64); err == nil {
			t := time.UnixMilli(i)
			sig.FirstReceived = &t
		}
	}

	// If the task is not registered return an error
	// and leave the message in the queue
	if !b.IsTaskRegistered(sig.Name) {
		if sig.IgnoreWhenTaskNotRegistered {
			b.deleteOne(delivery)
		}
		return fmt.Errorf("task %s is not registered", sig.Name)
	}

	// Always set the routing key based on the processor. This ensures it's set to the queue it's pulled off of, even
	// if the message was originally on another queue (it can be moved automatically to a DLQ).
	sig.RoutingKey = b.getQueueName(taskProcessor)

	err := taskProcessor.Process(sig, b.extend)
	if err != nil {
		// stop task deletion in case we want to send messages to dlq in sqs or retry from visibility timeout
		if err == errs.ErrStopTaskDeletion {
			return nil
		}
		return err
	}
	// Delete message after successfully consuming and processing the message
	if err = b.deleteOne(delivery); err != nil {
		log.ERROR.Printf("error when deleting the delivery. delivery is %v, Error=%s", delivery, err)
	}
	return err
}

// deleteOne is a method delete a delivery from AWS SQS
func (b *Broker) deleteOne(delivery *awssqs.ReceiveMessageOutput) error {
	qURL := b.defaultQueueURL()
	_, err := b.service.DeleteMessage(&awssqs.DeleteMessageInput{
		QueueUrl:      qURL,
		ReceiptHandle: delivery.Messages[0].ReceiptHandle,
	})

	if err != nil {
		return err
	}
	return nil
}

// defaultQueueURL is a method returns the default queue url
func (b *Broker) defaultQueueURL() *string {
	if b.queueUrl != nil {
		return b.queueUrl
	} else {
		return aws.String(b.GetConfig().Broker + "/" + b.GetConfig().DefaultQueue)
	}

}

// receiveMessage is a method receives a message from specified queue url
func (b *Broker) receiveMessage(qURL *string) (*awssqs.ReceiveMessageOutput, error) {
	var waitTimeSeconds int
	var visibilityTimeout *int
	if b.GetConfig().SQS != nil {
		waitTimeSeconds = b.GetConfig().SQS.WaitTimeSeconds
		visibilityTimeout = b.GetConfig().SQS.VisibilityTimeout
	} else {
		waitTimeSeconds = 0
	}
	input := &awssqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(awssqs.MessageSystemAttributeNameSentTimestamp),
			aws.String(awssqs.MessageSystemAttributeNameApproximateReceiveCount),
		},
		MessageAttributeNames: []*string{
			aws.String(awssqs.QueueAttributeNameAll),
		},
		QueueUrl:            qURL,
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(int64(waitTimeSeconds)),
	}
	if visibilityTimeout != nil {
		input.VisibilityTimeout = aws.Int64(int64(*visibilityTimeout))
	}
	log.INFO.Printf("SQS Receive on %s", *qURL)
	result, err := b.service.ReceiveMessage(input)
	log.INFO.Printf("SQS Receive result on %s: %d %v", *qURL, len(result.Messages), err)
	if err != nil {
		return nil, err
	}
	return result, err
}

// initializePool is a method which initializes concurrency pool
func (b *Broker) initializePool(pool chan struct{}, concurrency int) {
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}
}

// consumeDeliveries is a method consuming deliveries from deliveries channel
func (b *Broker) consumeDeliveries(deliveries <-chan *awssqs.ReceiveMessageOutput, taskProcessor iface.TaskProcessor, concurrency iface.ResizeablePool, errorsChan chan error) (bool, error) {
	log.INFO.Printf("consumeDeliveries(%#v)", deliveries)
	select {
	case err := <-errorsChan:
		log.INFO.Println("consumeDeliveries returning false, err=%#v", err)
		return false, err
	case d := <-deliveries:
		log.INFO.Printf("consumeDeliveries got a deliveries")
		b.processingWG.Add(1)
		log.INFO.Printf("consumeDeliveries added to p.processingWG")

		// Consume the task inside a goroutine so multiple tasks
		// can be processed concurrently
		go func() {
			log.INFO.Printf("consumeDeliveries inner goroutine started")

			err := b.consumeOne(d, taskProcessor)

			// give worker back to pool
			log.INFO.Printf("Calling concurrency(%#v).Return()", concurrency)
			concurrency.Return()
			log.INFO.Printf("consumeDeliveries inner goroutine finished")

			b.processingWG.Done()

			if err != nil {
				log.INFO.Printf("Before sending %#v to %#v", err, errorsChan)
				errorsChan <- err
				log.INFO.Printf("After sending %#v to %#v", err, errorsChan)
			}
		}()
	case <-b.GetStopChan():
		log.INFO.Printf("consumeDeliveries returning false, nil")
		return false, nil
	}
	log.INFO.Printf("consumeDeliveries returning true, nil")
	return true, nil
}

// continueReceivingMessages is a method returns a continue signal
func (b *Broker) continueReceivingMessages(qURL *string, deliveries chan *awssqs.ReceiveMessageOutput) (bool, error) {
	log.INFO.Printf("continueReceivingMessages(qURL=%s, deliveries=%#v)", *qURL, deliveries)
	select {
	// A way to stop this goroutine from b.StopConsuming
	case <-b.stopReceivingChan:
		return false, nil
	default:
		output, err := b.receiveMessage(qURL)
		log.INFO.Printf("continueReceivingMessages received messages len %d", len(output.Messages))
		if err != nil {
			return true, err
		}
		if len(output.Messages) == 0 {
			return true, nil
		}
		log.INFO.Printf("continueReceivingMessages spawning goroutine to put output on deliveries %#v", deliveries)
		go func() {
			log.INFO.Printf("continueReceivingMessages goroutine putting output on deliveries %#v", deliveries)
			deliveries <- output
			log.INFO.Printf("continueReceivingMessages goroutine put output on deliveries %#v", deliveries)
		}()
	}
	return true, nil
}

// stopReceiving is a method sending a signal to stopReceivingChan
func (b *Broker) stopReceiving() {
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
}

// getQueueURL is a method returns that returns queueURL first by checking if custom queue was set and usign it
// otherwise using default queueName from config
func (b *Broker) getQueueURL(taskProcessor iface.TaskProcessor) *string {
	queueName := b.getQueueName(taskProcessor)

	return aws.String(b.GetConfig().Broker + "/" + queueName)
}

func (b *Broker) getQueueName(taskProcessor iface.TaskProcessor) string {
	queueName := b.GetConfig().DefaultQueue
	if taskProcessor.CustomQueue() != "" {
		queueName = taskProcessor.CustomQueue()
	}
	return queueName
}
