package common_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

func TestResizablePool(t *testing.T) {
	expectedCapacity := 2
	rs, cancel := common.NewResizablePool(expectedCapacity)

	pool := rs.Pool()

	tryAcquire := func(didTake *bool) func() {
		cc := make(chan struct{}, 1)

		go func() {
			select {
			case <-pool:
				*didTake = true
			case <-cc:
				return
			}
		}()

		return func() {
			cc <- struct{}{}
		}
	}

	acquireAll := func() {
		st := time.Now()

		for i := 0; i < expectedCapacity; i++ {
			<-pool
		}

		assert.Less(t, time.Since(st), time.Millisecond)
	}
	acquireAll()

	acquireBlockTillReturn := func() {
		var didTake bool

		cc := tryAcquire(&didTake)

		time.Sleep(time.Millisecond)
		assert.False(t, didTake)

		rs.Return()
		time.Sleep(time.Millisecond)
		assert.True(t, didTake)

		cc()
	}
	acquireBlockTillReturn()

	acquireBlocksTillAdd := func() {
		var didTake bool

		cc := tryAcquire(&didTake)

		time.Sleep(time.Millisecond)
		assert.False(t, didTake)

		expectedCapacity++
		rs.SetCapacity(expectedCapacity)
		time.Sleep(time.Millisecond)
		assert.True(t, didTake)

		cc()
	}
	acquireBlocksTillAdd()

	require.Equal(t, 3, expectedCapacity)

	// Remove capacity
	removeCapacity := func() {
		var didTake bool

		cc := tryAcquire(&didTake)

		time.Sleep(time.Millisecond)
		assert.False(t, didTake)

		expectedCapacity--
		rs.SetCapacity(expectedCapacity)

		time.Sleep(time.Millisecond)
		assert.False(t, didTake)

		cc()
	}
	removeCapacity()
	removeCapacity()

	require.Equal(t, 1, expectedCapacity)

	returnAllCapacity := func(count int) {
		st := time.Now()

		for i := 0; i < count; i++ {
			rs.Return()
		}

		assert.Less(t, time.Since(st), time.Millisecond)
	}
	// Capacity is 1, but 2 additional slots are held from before capacity was reduced.
	returnAllCapacity(3)

	acquireAll()
	removeCapacity()
	returnAllCapacity(1)
	removeCapacity()
	returnAllCapacity(1)
	acquireBlocksTillAdd()

	cancel()
}
