package common_test

import (
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestIsTaskRegistered(t *testing.T) {
	t.Parallel()

	broker := common.NewBroker(new(config.Config))
	broker.SetRegisteredTaskNames([]string{"foo", "bar"})

	assert.True(t, broker.IsTaskRegistered("foo"))
	assert.False(t, broker.IsTaskRegistered("bogus"))
}

func TestAdjustRoutingKey(t *testing.T) {
	t.Parallel()

	var (
		s      *tasks.Signature
		broker common.Broker
	)

	t.Run("with routing key", func(t *testing.T) {
		s = &tasks.Signature{RoutingKey: "routing_key"}
		broker = common.NewBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.AdjustRoutingKey(s)
		assert.Equal(t, "routing_key", s.RoutingKey)
	})

	t.Run("without routing key", func(t *testing.T) {
		s = new(tasks.Signature)
		broker = common.NewBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.AdjustRoutingKey(s)
		assert.Equal(t, "queue", s.RoutingKey)
	})
}

func TestGetRegisteredTaskNames(t *testing.T) {
	t.Parallel()

	broker := common.NewBroker(new(config.Config))
	fooTasks := []string{"foo", "bar", "baz"}
	broker.SetRegisteredTaskNames(fooTasks)
	assert.Equal(t, fooTasks, broker.GetRegisteredTaskNames())
}

func TestStopConsuming(t *testing.T) {
	t.Parallel()

	t.Run("stop consuming", func(t *testing.T) {
		broker := common.NewBroker(&config.Config{
			DefaultQueue: "queue",
		})
		broker.StartConsuming("", &machinery.Worker{})
		broker.StopConsuming()
		select {
		case <-broker.GetStopChan():
		default:
			assert.Fail(t, "still blocking")
		}
	})
}

func TestResizable(t *testing.T) {
	capacity := 2
	rs := common.NewResizableWithStartingCapacity(capacity)

	var endValidations []func()

	acquireAllSlots := func() {
		st := time.Now()

		for i := 0; i < capacity; i++ {
			if i%2 == 0 {
				rs.Take()
			} else {
				lease, cancel := rs.Lease()
				defer cancel()

				<-lease
			}
		}

		assert.Less(t, time.Since(st), time.Millisecond)
	}
	acquireAllSlots()

	takeBlocks := func() {
		var didTake bool

		go func() {
			rs.Take()
			didTake = true
		}()

		time.Sleep(time.Millisecond)
		assert.False(t, didTake)

		rs.Return()
		time.Sleep(time.Millisecond)
		assert.True(t, didTake)
	}
	takeBlocks()

	leaseBlocks := func() {
		var didTake bool

		go func() {
			take, cancel := rs.Lease()
			defer cancel()
			<-take
			didTake = true
		}()

		time.Sleep(time.Millisecond)
		assert.False(t, didTake)

		rs.Return()
		time.Sleep(time.Millisecond)
		assert.True(t, didTake)
	}
	leaseBlocks()

	addAndUseWithTake := func() {
		var didTake bool

		go func() {
			rs.Take()
			didTake = true
		}()

		time.Sleep(time.Millisecond)
		assert.False(t, didTake)

		capacity++
		rs.SetCapacity(capacity)
		time.Sleep(time.Millisecond)
		assert.True(t, didTake)
	}
	addAndUseWithTake()

	addAndUseWithLease := func() {
		var didTake bool

		go func() {
			take, cancel := rs.Lease()
			defer cancel()
			<-take
			didTake = true
		}()

		time.Sleep(time.Millisecond)
		assert.False(t, didTake)

		capacity++
		rs.SetCapacity(capacity)
		time.Sleep(time.Millisecond)
		assert.True(t, didTake)
	}
	addAndUseWithLease()

	// Remove capacity and cancel
	removeCapacity := func() {
		var didTake bool

		var take <-chan struct{}
		var cancel func()
		go func() {
			take, cancel = rs.Lease()
			<-take
			didTake = true
		}()

		time.Sleep(time.Millisecond)
		assert.False(t, didTake)

		capacity--
		rs.SetCapacity(capacity)

		cancel()
		time.Sleep(time.Millisecond)

		// Cancel doesn't allow taking (unvalidated but the go routine should exit allowing take to be collected)
		assert.False(t, didTake)

		endValidations = append(endValidations, func() {
			assert.False(t, didTake)
		})
	}
	removeCapacity()
	removeCapacity()

	assert.Equal(t, 2, capacity)

	returnAllCapacity := func(count int) {
		st := time.Now()

		for i := 0; i < count; i++ {
			rs.Return()
		}

		assert.Less(t, time.Since(st), time.Millisecond)
	}
	// Capacity is 2, but 2 additional slots are held from before capacity was reduced.
	returnAllCapacity(4)

	acquireAllSlots()
	removeCapacity()
	returnAllCapacity(1)
	addAndUseWithLease()
	removeCapacity()
	returnAllCapacity(1)
	addAndUseWithTake()

	for _, f := range endValidations {
		f()
	}
}
