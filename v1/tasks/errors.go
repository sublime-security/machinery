package tasks

import (
	"fmt"
	"time"
)

// ErrRetryTaskLater ...
type ErrRetryTaskLater struct {
	name, msg string
	retryIn   time.Duration
}

// RetryIn returns time.Duration from now when task should be retried
func (e ErrRetryTaskLater) RetryIn() time.Duration {
	return e.retryIn
}

// Error implements the error interface
func (e ErrRetryTaskLater) Error() string {
	return fmt.Sprintf("Task error: %s Will retry in: %s", e.msg, e.retryIn)
}

// NewErrRetryTaskLater returns new ErrRetryTaskLater instance
func NewErrRetryTaskLater(msg string, retryIn time.Duration) ErrRetryTaskLater {
	return ErrRetryTaskLater{msg: msg, retryIn: retryIn}
}

// Retriable is interface that retriable errors should implement
type Retriable interface {
	RetryIn() time.Duration
	error
}

// ErrKeepAndRetryTaskLater ...
type ErrKeepAndRetryTaskLater struct {
	msg     string
	err     error
	retryIn time.Duration
}

// RetryIn returns time.Duration from now when task should be retried
func (e ErrKeepAndRetryTaskLater) RetryIn() time.Duration {
	return e.retryIn
}

// Error implements the error interface
func (e ErrKeepAndRetryTaskLater) Error() string {
	return fmt.Sprintf("Task error: %v (%s). Will retry in: %s. Will attempt to re-process same message.", e.err, e.msg, e.retryIn)
}

// NewErrKeepAndRetryTaskLater returns new ErrKeepAndRetryTaskLater instance
func NewErrKeepAndRetryTaskLater(err error, msg string, retryIn time.Duration) ErrKeepAndRetryTaskLater {
	return ErrKeepAndRetryTaskLater{err: err, msg: msg, retryIn: retryIn}
}
