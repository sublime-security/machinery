package common_test

import (
	"fmt"
	"sync"
	"sync/atomic"
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

func TestResizablePoolConcurrency(t *testing.T) {
	// TODO: Add a rolling down function
	rollingCapFunc := func(changeItr int) int {
		return changeItr % 5
	}

	desc := "rolling"

	var rollingResults Results
	launchTestResizablePoolConcurrency(t, desc, time.Millisecond, rollingCapFunc, &rollingResults)
	launchTestResizablePoolConcurrency(t, desc, 3*time.Millisecond, rollingCapFunc, &rollingResults)
	launchTestResizablePoolConcurrency(t, desc, 77*time.Millisecond, rollingCapFunc, &rollingResults)
	launchTestResizablePoolConcurrency(t, desc, 333*time.Millisecond, rollingCapFunc, &rollingResults)
	launchTestResizablePoolConcurrency(t, desc, time.Second, rollingCapFunc, &rollingResults)
	launchTestResizablePoolConcurrency(t, desc, time.Second+time.Millisecond, rollingCapFunc, &rollingResults)
	launchTestResizablePoolConcurrency(t, desc, time.Second+100*time.Millisecond, rollingCapFunc, &rollingResults)

	launchTestResizablePoolConcurrency(t, desc, time.Second/2, rollingCapFunc, &rollingResults)
	launchTestResizablePoolConcurrency(t, desc, time.Second/2+time.Millisecond, rollingCapFunc, &rollingResults)
	launchTestResizablePoolConcurrency(t, desc, time.Second/2+100*time.Millisecond, rollingCapFunc, &rollingResults)

	var constantResults Results

	for i := 0; i < 2; i++ {
		cap := i * 2
		constantCapFunc := func(int) int {
			return i * 2
		}

		desc = fmt.Sprintf("Constant cap at %d", cap)

		launchTestResizablePoolConcurrency(t, desc, time.Second, constantCapFunc, &constantResults)
		launchTestResizablePoolConcurrency(t, desc, time.Second+time.Millisecond, constantCapFunc, &constantResults)
		launchTestResizablePoolConcurrency(t, desc, time.Second+100*time.Millisecond, constantCapFunc, &constantResults)

		launchTestResizablePoolConcurrency(t, desc, time.Second/2, constantCapFunc, &constantResults)
		launchTestResizablePoolConcurrency(t, desc, time.Second/2+time.Millisecond, constantCapFunc, &constantResults)
		launchTestResizablePoolConcurrency(t, desc, time.Second/2+100*time.Millisecond, constantCapFunc, &constantResults)
	}

	fmt.Printf(`
Rolling Results.
WorstAboveCap %v
WorstNotAtCap %v
Avg %v
`, rollingResults.WorstAboveCap(), rollingResults.WorstNotAtCap(), rollingResults.Average())

	fmt.Printf(`
Constant Results.
WorstAboveCap %v
WorstNotAtCap %v
Avg %v
`, constantResults.WorstAboveCap(), constantResults.WorstNotAtCap(), constantResults.Average())

	combined := append(rollingResults, constantResults...)
	fmt.Printf(`
Combined Results.
WorstAboveCap %v
WorstNotAtCap %v
Avg %v
`, combined.WorstAboveCap(), combined.WorstNotAtCap(), combined.Average())
}

func launchTestResizablePoolConcurrency(
	t *testing.T,
	desc string,
	capChangeSleep time.Duration,
	capChangeFunc func(int) int,
	resultCollection *Results) {
	var resultChan = make(chan ResizablePoolConcurrencyResult, 1)

	name := fmt.Sprintf("%s. Sleep %s", desc, capChangeSleep.String())
	t.Run(name, func(t *testing.T) {
		//t.Parallel()
		resultChan <- testResizablePoolConcurrency(t, capChangeSleep, capChangeFunc)
	})

	result := <-resultChan
	result.Meta = name
	*resultCollection = append(*resultCollection, result)
}

func testResizablePoolConcurrency(t *testing.T, capChangeSleep time.Duration, capChangeFunc func(int) int) ResizablePoolConcurrencyResult {
	consumerCount := 100

	rs, cancel := common.NewResizablePool(0)

	var consumerCancelChans []chan struct{}

	var concurrency int64

	type CapRecord struct {
		Actual    int64
		LastCap   int64
		NewestCap int64
	}

	var desiredAndCapLock sync.Mutex
	var capRecords []CapRecord
	var lastCap, newestCap int64

	for i := 0; i < consumerCount; i++ {
		cc := make(chan struct{}, 1)
		consumerCancelChans = append(consumerCancelChans, cc)

		go func() {
			p := rs.Pool()

			for {
				select {
				case <-cc:
					return
				case <-p:
				}

				oldVal := atomic.AddInt64(&concurrency, 1)
				require.GreaterOrEqual(t, oldVal, int64(0)) // Test problem if this fails

				act := atomic.LoadInt64(&concurrency)
				last := atomic.LoadInt64(&lastCap)
				newest := atomic.LoadInt64(&newestCap)
				desiredAndCapLock.Lock()
				capRecords = append(capRecords, CapRecord{
					Actual:    act,
					LastCap:   last,
					NewestCap: newest,
				})
				desiredAndCapLock.Unlock()

				// TODO: variable timer + maybe variable delay on launch? The latter may cause below cap?

				// "work"
				select {
				case <-cc:
					return
				case <-time.NewTimer(time.Second).C:
				}

				preReturn := time.Now()
				rs.Return()
				assert.Less(t, time.Since(preReturn), time.Millisecond)

				atomic.AddInt64(&concurrency, -1)
			}
		}()
	}

	for i := 0; i < int(time.Second*30/capChangeSleep); i++ {
		newCap := capChangeFunc(i)
		atomic.StoreInt64(&newestCap, int64(newCap))

		preSet := time.Now()
		// Block on the capacity being set correctly before setting lastCap to the newCap. This ensures the test isn't
		// racing against setting the capacity.
		<-rs.SetCapacity(newCap)
		assert.Less(t, time.Since(preSet), time.Millisecond)

		// fmt.Printf("Setting capacity to %d. Iteration %d \n", newCap, i)

		atomic.StoreInt64(&lastCap, int64(newCap))

		time.Sleep(capChangeSleep)
	}

	// End of test cleanup
	for _, cc := range consumerCancelChans {
		cc <- struct{}{}
	}
	cancel()

	var result ResizablePoolConcurrencyResult
	for _, cr := range capRecords {
		result.Total++

		minCap := cr.LastCap
		if cr.NewestCap < minCap {
			minCap = cr.NewestCap
		}
		maxCap := cr.LastCap
		if cr.NewestCap > maxCap {
			maxCap = cr.NewestCap
		}

		if cr.Actual >= minCap && cr.Actual <= maxCap {
			result.CountAtCap++
		} else if cr.Actual < minCap {
			result.CountBelowCap++
		} else if cr.Actual > maxCap {
			result.CountAboveCap++
		}
	}

	fmt.Println(result.String())

	return result
}

type ResizablePoolConcurrencyResult struct {
	CountAtCap    float64
	CountBelowCap float64
	CountAboveCap float64
	Total         float64

	Meta string
}

type Results []ResizablePoolConcurrencyResult

func (result ResizablePoolConcurrencyResult) String() string {
	return fmt.Sprintf(`
%s
At capacity %.2f %%
Below capacity %.2f %%
Above capacity %.2f %%  
----
`,
		result.Meta,
		result.PerAt(),
		100*result.CountBelowCap/result.Total,
		result.PerAbove(),
	)

}

func (result ResizablePoolConcurrencyResult) PerAbove() float64 {
	return 100 * result.CountAboveCap / result.Total
}

func (result ResizablePoolConcurrencyResult) PerAt() float64 {
	return 100 * result.CountAtCap / result.Total
}

func (results Results) WorstAboveCap() *ResizablePoolConcurrencyResult {
	var worst *ResizablePoolConcurrencyResult

	for _, r := range results {
		if worst == nil || worst.PerAbove() < r.PerAbove() {
			rCpy := r
			worst = &rCpy
		}
	}

	return worst
}

func (results Results) WorstNotAtCap() *ResizablePoolConcurrencyResult {
	var worst *ResizablePoolConcurrencyResult

	for _, r := range results {
		if worst == nil || worst.PerAt() > r.PerAt() {
			rCpy := r
			worst = &rCpy
		}
	}

	return worst
}

func (results Results) Average() ResizablePoolConcurrencyResult {
	var avg ResizablePoolConcurrencyResult

	for _, r := range results {
		avg.CountAboveCap += r.CountAboveCap
		avg.CountBelowCap += r.CountBelowCap
		avg.CountAtCap += r.CountAtCap
		avg.Total += r.Total
	}

	return avg
}
