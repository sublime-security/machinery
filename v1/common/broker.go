package common

import (
	"errors"
	"sync"

	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/retry"
	"github.com/RichardKnop/machinery/v1/tasks"
)

type registeredTaskNames struct {
	sync.RWMutex
	items []string
}

// Broker represents a base broker structure
type Broker struct {
	cnf                 *config.Config
	registeredTaskNames registeredTaskNames
	retry               bool
	retryFunc           func(chan int)
	retryStopChan       chan int
	stopChan            chan int
}

// NewBroker creates new Broker instance
func NewBroker(cnf *config.Config) Broker {
	return Broker{
		cnf:           cnf,
		retry:         true,
		stopChan:      make(chan int),
		retryStopChan: make(chan int),
	}
}

// GetConfig returns config
func (b *Broker) GetConfig() *config.Config {
	return b.cnf
}

// GetRetry ...
func (b *Broker) GetRetry() bool {
	return b.retry
}

// GetRetryFunc ...
func (b *Broker) GetRetryFunc() func(chan int) {
	return b.retryFunc
}

// GetRetryStopChan ...
func (b *Broker) GetRetryStopChan() chan int {
	return b.retryStopChan
}

// GetStopChan ...
func (b *Broker) GetStopChan() chan int {
	return b.stopChan
}

// Publish places a new message on the default queue
func (b *Broker) Publish(signature *tasks.Signature) error {
	return errors.New("Not implemented")
}

// SetRegisteredTaskNames sets registered task names
func (b *Broker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames.Lock()
	defer b.registeredTaskNames.Unlock()
	b.registeredTaskNames.items = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (b *Broker) IsTaskRegistered(name string) bool {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	for _, registeredTaskName := range b.registeredTaskNames.items {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// GetDelayedTasks returns a slice of task.Signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks() ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming is a common part of StartConsuming method
func (b *Broker) StartConsuming(consumerTag string, taskProcessor iface.TaskProcessor) {
	if b.retryFunc == nil {
		b.retryFunc = retry.Closure()
	}

}

// StopConsuming is a common part of StopConsuming
func (b *Broker) StopConsuming() {
	// Do not retry from now on
	b.retry = false
	// Stop the retry closure earlier
	select {
	case b.retryStopChan <- 1:
		log.WARNING.Print("Stopping retry closure.")
	default:
	}
	// Notifying the stop channel stops consuming of messages
	close(b.stopChan)
	log.WARNING.Print("Stop channel")
}

// GetRegisteredTaskNames returns registered tasks names
func (b *Broker) GetRegisteredTaskNames() []string {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	items := b.registeredTaskNames.items
	return items
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (b *Broker) AdjustRoutingKey(s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}

	s.RoutingKey = b.GetConfig().DefaultQueue
}

type resizableCapacity struct {
	changes chan change

	pool chan struct{}
}

type change struct {
	isReturned bool
	isTaken    bool
	updatedCap *int

	changeDone chan struct{}
}

func NewResizablePool(startingCapacity int) (iface.ResizeablePool, func()) {
	rc := &resizableCapacity{
		changes: make(chan change),
		pool:    make(chan struct{}),
	}

	var (
		// A separate routine is used to push any available capacity to the pool, when available. This channel allows
		// cancelling the last function when available capacity changes (both to avoid leaking resources and so that
		// capacity available at one point that wasn't used isn't given when capacity is lowered.
		lastGiverCancel chan struct{}

		capacity int
		taken    int

		// Used to cancel the background routine/release resources
		cancelChan = make(chan struct{}, 1)
	)

	// Use a routine to coordinate changes (either returned capacity or changed capacity)
	// This allows the different paths to both ultimately control a single channel that can be used as a normal
	// capacity pool channel.
	go func() {
		for {
			select {
			case <-cancelChan:
				if lastGiverCancel != nil {
					lastGiverCancel <- struct{}{}
				}

				return
			case c := <-rc.changes:
				// Kill the function that might be giving capacity -- a new function will be started if any is available
				if lastGiverCancel != nil {
					lastGiverCancel <- struct{}{}
					lastGiverCancel = nil
				}

				// Calculate what's available based on the change
				if c.isReturned {
					taken--
				}
				if c.isTaken {
					taken++
				}
				if c.updatedCap != nil {
					capacity = *c.updatedCap
				}
				canGive := capacity - taken

				if canGive > 0 {
					// Launch a cancellable function which will populate the pool when there's a listener
					// This must be a separate routine to ensure changes can occur even if there's no listener on the
					// pool.
					lastGiverCancel = make(chan struct{}, 1)

					go func() {
						// If both <-lastGiverCancel and rc.pool <- there will be a 50/50 chance of either
						// occurring. The select above reduces the odds of this happening, but it is possible to
						// give 1 when lastGiverCancel has an element if rc.pool is eligible at the "same" time.
						select {
						case <-lastGiverCancel:
							return
						case rc.pool <- struct{}{}:
							rc.changes <- change{isTaken: true}
						}
					}()
				}

				if c.changeDone != nil {
					c.changeDone <- struct{}{}
				}
			}
		}
	}()

	rc.SetCapacity(startingCapacity)

	return rc, func() {
		cancelChan <- struct{}{}
	}
}

func (p *resizableCapacity) Return() {
	p.changes <- change{isReturned: true}
}

func (p *resizableCapacity) Pool() <-chan struct{} {
	return p.pool
}

func (p *resizableCapacity) SetCapacity(desiredCap int) <-chan struct{} {
	capacityAccepted := make(chan struct{}, 1)

	p.changes <- change{updatedCap: &desiredCap, changeDone: capacityAccepted}

	return capacityAccepted
}
