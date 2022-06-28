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

	SetCapacity(int)
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
