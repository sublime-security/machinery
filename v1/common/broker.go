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
	giverEnded bool
}

func NewResizablePool(startingCapacity int) (iface.ResizeablePool, func()) {
	rc := &resizableCapacity{
		changes: make(chan change),
		pool:    make(chan struct{}),
	}

	var (
		// A separate routine is used to push any available capacity to the pool, when available. This channel allows
		// cancelling the last function if capacity is now longer available.
		cancelLastGiver chan struct{}
		giverRunning    bool

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
				if cancelLastGiver != nil {
					cancelLastGiver <- struct{}{}
				}

				return
			case c := <-rc.changes:
				giverEnded := false

				var changeDoneChannels []chan struct{}
				countChange := func(c change) {
					if c.giverEnded {
						giverEnded = true
					}

					if c.isReturned {
						taken--
					}
					if c.isTaken {
						taken++
					}
					if c.updatedCap != nil {
						capacity = *c.updatedCap
					}

					if c.changeDone != nil {
						changeDoneChannels = append(changeDoneChannels, c.changeDone)
					}
				}

				countChange(c)
				for {
					select {
					case c := <-rc.changes:
						countChange(c)
						continue
					default:
					}

					break
				}

				giverRunning = giverRunning && !giverEnded

				// Calculate what's available based on all the changes
				canGive := capacity - taken

				giverWanted := canGive > 0

				// Call at the end of the iteration or anytime we continue
				deferIter := func() {
					for _, c := range changeDoneChannels {
						c <- struct{}{}
					}
				}

				if !giverWanted {
					// Cancel if available (it might be the first run, or several runs since a giver was launched)
					// giverRunning will be updated on end.
					if cancelLastGiver != nil {
						cancelLastGiver <- struct{}{}
						cancelLastGiver = nil
					}
					deferIter()
					continue
				}

				// No update necessary, avoid race conditions on trying to cancel the giver/create a new one to do the
				// same thing. If the last giver is *just* finishing, it will be queueing a change which will bring
				// everything back to the correct state.
				if giverRunning {
					deferIter()
					continue
				}

				// Launch a cancellable function, the "giver", which will populate the pool when there's a listener
				// This must be a separate routine to ensure changes can occur even if there's no listener on the
				// pool.
				giverRunning = true
				cancelLastGiver = make(chan struct{}, 1)

				go func() {
					// If both <-cancelLastGiver and rc.pool <- there will be a 50/50 chance of either
					// occurring. This could lead to a capacity being given even after capacity is reduced, but only
					// in a pretty rare race condition and is materially no different than setCapacity being called
					// a moment later.
					select {
					case <-cancelLastGiver:
						rc.changes <- change{giverEnded: true}
						return
					case rc.pool <- struct{}{}:
						rc.changes <- change{isTaken: true, giverEnded: true}
					}
				}()

				deferIter()
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
