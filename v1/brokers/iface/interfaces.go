package iface

import (
	"context"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency ResizeablePool, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(ctx context.Context, task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	GetDelayedTasks() ([]*tasks.Signature, error)
	AdjustRoutingKey(s *tasks.Signature)
}

type ResizeablePool interface {
	// Return a single entity to the pool. Similar to pool <- struct{}{}
	Return()

	Pool() <-chan struct{}

	// SetCapacity Returns a channel which will receive a value when the change has been fully accepted. Most users should ignore
	// this.
	SetCapacity(int) <-chan struct{}
}

// Token represents a unit of concurrency taken from a ResizeablePool.
// Calling Return() releases the unit back to the originating pool.
type Token interface {
	Return()
}

// ResizeablePoolV2 extends ResizeablePool with token-based capacity management.
// Brokers type-assert the concurrency argument to this interface to obtain
// self-returning Tokens instead of raw slots, enabling pools that route each
// Return() back to the specific sub-pool the slot originated from.
type ResizeablePoolV2 interface {
	ResizeablePool
	PoolWithToken() <-chan Token
}

type RetrySameMessage interface {
	Broker

	// RetryMessage Does not return an error because, at least with current use case, all errors should just be ignored
	RetryMessage(s *tasks.Signature)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature, extendFunc tasks.ExtendForSignatureFunc) error
	CustomQueue() string
	PreConsumeHandler() bool
}
