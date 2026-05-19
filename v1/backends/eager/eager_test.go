package eager_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends/eager"
	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/suite"
)

type EagerBackendTestSuite struct {
	suite.Suite

	backend iface.Backend
	st      []*tasks.Signature
	groups  []struct {
		id    string
		tasks []string
	}
}

func (s *EagerBackendTestSuite) SetupSuite() {
	// prepare common test data
	s.backend = eager.New()

	// 2 non-group state
	s.st = []*tasks.Signature{
		{UUID: "1"},
		{UUID: "2"},
		{UUID: "3"},
		{UUID: "4"},
		{UUID: "5"},
		{UUID: "6"},
	}

	for _, t := range s.st {
		s.backend.SetStatePending(t)
	}

	// groups
	s.groups = []struct {
		id    string
		tasks []string
	}{
		{"group1", []string{"1-3", "1-4"}},
		{"group2", []string{"2-1", "2-2", "2-3"}},
		{"group3", []string(nil)},
		{"group4", []string{"4-1", "4-2", "4-3", "4-4"}},
		{"group5", []string{"5-1", "5-2"}},
	}

	for _, g := range s.groups {
		for _, t := range g.tasks {
			sig := &tasks.Signature{
				UUID:           t,
				GroupUUID:      g.id,
				GroupTaskCount: len(g.tasks),
			}
			s.st = append(s.st, sig)

			// default state is pending
			s.backend.SetStatePending(sig)
		}

		s.Nil(s.backend.InitGroup(g.id, g.tasks))
	}

	// prepare for TestInitGroup
	s.Nil(s.backend.PurgeGroupMeta(s.groups[4].id))
}

//
// Test Cases
//

func (s *EagerBackendTestSuite) TestInitGroup() {
	// group 5
	{
		g := s.groups[4]
		s.Nil(s.backend.InitGroup(g.id, g.tasks))
	}

	// group3 -- nil as task list
	{
		g := s.groups[2]
		s.Nil(s.backend.InitGroup(g.id, g.tasks))
	}
}

func (s *EagerBackendTestSuite) TestGroupCompleted() {
	// group 1
	{
		// all tasks are pending
		g := s.groups[0]
		completed, err := s.backend.GroupCompleted(g.id, len(g.tasks))
		s.False(completed)
		s.Nil(err)

		// make these tasks success
		for _, id := range g.tasks {
			t := s.getTaskSignature(id)
			s.NotNil(t)
			if t == nil {
				break
			}

			s.backend.SetStateSuccess(t, nil)
		}

		completed, err = s.backend.GroupCompleted(g.id, len(g.tasks))
		s.True(completed)
		s.Nil(err)
	}

	// group 2
	{
		g := s.groups[1]

		completed, err := s.backend.GroupCompleted(g.id, len(g.tasks))
		s.False(completed)
		s.Nil(err)

		// make these tasks failure
		for _, id := range g.tasks {
			t := s.getTaskSignature(id)
			s.NotNil(t)
			if t == nil {
				break
			}

			s.backend.SetStateFailure(t, "just a test")
		}

		completed, err = s.backend.GroupCompleted(g.id, len(g.tasks))
		s.True(completed)
		s.Nil(err)
	}

	{
		// call on a not-existed group
		completed, err := s.backend.GroupCompleted("", 0)
		s.False(completed)
		s.NotNil(err)
	}
}

func (s *EagerBackendTestSuite) TestGroupTaskStates() {
	// group 4
	{
		g := s.groups[3]

		// set failure state with taskUUID as error message
		for _, id := range g.tasks {
			t := s.getTaskSignature(id)
			s.NotNil(t)
			if t == nil {
				break
			}

			s.backend.SetStateFailure(t, t.UUID)
		}

		// get states back
		ts, err := s.backend.GroupTaskStates(g.id, len(g.tasks))
		s.NotNil(ts)
		s.Nil(err)
		for _, t := range ts {
			s.Equal(t.TaskUUID, t.Error)
		}
	}

	{
		// call on a not-existed group
		ts, err := s.backend.GroupTaskStates("", 0)
		s.Nil(ts)
		s.NotNil(err)
	}
}

func (s *EagerBackendTestSuite) TestSetStatePending() {
	// task 1
	{
		t := s.st[0]

		// change this state to receiving
		s.backend.SetStateReceived(t)

		// change it back to pending
		s.backend.SetStatePending(t)

		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(tasks.StatePending, st.State)
		}
	}
}

func (s *EagerBackendTestSuite) TestSetStateReceived() {
	// task2
	{
		t := s.st[1]
		s.backend.SetStateReceived(t)
		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(tasks.StateReceived, st.State)
		}
	}
}

func (s *EagerBackendTestSuite) TestSetStateStarted() {
	// task3
	{
		t := s.st[2]
		s.backend.SetStateStarted(t)
		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(tasks.StateStarted, st.State)
		}
	}
}

func (s *EagerBackendTestSuite) TestSetStateSuccess() {
	// task4
	{
		t := s.st[3]
		taskResults := []*tasks.TaskResult{
			{
				Type:  "float64",
				Value: json.Number("300.0"),
			},
		}
		s.backend.SetStateSuccess(t, taskResults)
		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		s.NotNil(st)

		s.Equal(tasks.StateSuccess, st.State)
		s.Equal(taskResults, st.Results)
	}
}

func (s *EagerBackendTestSuite) TestSetStateFailure() {
	// task5
	{
		t := s.st[4]
		s.backend.SetStateFailure(t, "error")
		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(tasks.StateFailure, st.State)
			s.Equal("error", st.Error)
		}
	}
}

func (s *EagerBackendTestSuite) TestSetStateRetry() {
	// task6
	{
		t := s.st[5]
		s.backend.SetStateRetry(t)
		st, err := s.backend.GetState(t.UUID)
		s.Nil(err)
		if st != nil {
			s.Equal(tasks.StateRetry, st.State)
		}
	}
}

func (s *EagerBackendTestSuite) TestGetState() {
	// get something not existed -- empty string
	st, err := s.backend.GetState("")
	s.Nil(st)
	s.NotNil(err)
}

func (s *EagerBackendTestSuite) TestPurgeState() {
	// task6
	{
		t := s.st[5]
		st, err := s.backend.GetState(t.UUID)
		s.NotNil(st)
		s.Nil(err)

		// purge it
		s.Nil(s.backend.PurgeState(t.UUID))

		// should be not found
		st, err = s.backend.GetState(t.UUID)
		s.Nil(st)
		s.NotNil(err)
	}

	{
		// purge a not-existed state
		s.NotNil(s.backend.PurgeState(""))
	}
}

func (s *EagerBackendTestSuite) TestPurgeGroupMeta() {
	// group4
	{
		g := s.groups[3]
		ts, err := s.backend.GroupTaskStates(g.id, len(g.tasks))
		s.NotNil(ts)
		s.Nil(err)

		// purge group
		s.Nil(s.backend.PurgeGroupMeta(g.id))

		// should be not found
		ts, err = s.backend.GroupTaskStates(g.id, len(g.tasks))
		s.Nil(ts)
		s.NotNil(err)
	}

	{
		// purge a not-existed group
		s.NotNil(s.backend.PurgeGroupMeta(""))
	}
}

//
// internal method
//
func (s *EagerBackendTestSuite) getTaskSignature(taskUUID string) *tasks.Signature {
	for _, v := range s.st {
		if v.UUID == taskUUID {
			return v
		}
	}

	return nil
}

func TestEagerBackendMain(t *testing.T) {
	suite.Run(t, &EagerBackendTestSuite{})
}

// TestEagerBackend_ConcurrentAccess exercises concurrent reads and writes against the eager
// backend's internal maps. Prior to introducing a single mutex covering all map accesses, a
// concurrent caller mix like this would deterministically trigger Go's "concurrent map read and
// map write" fatal under -race (and frequently without). The test runs all operations the
// backend exposes — SetState*, GetState, InitGroup, GroupCompleted, GroupTaskStates, PurgeState,
// PurgeGroupMeta — so that any unprotected map access in any method is exercised.
func TestEagerBackend_ConcurrentAccess(t *testing.T) {
	backend := eager.New()

	const numGoroutines = 50
	const numIterations = 200

	// Seed some baseline state so the readers have non-empty data to scan.
	for i := range numGoroutines {
		sig := &tasks.Signature{UUID: fmt.Sprintf("seed-%d", i)}
		if err := backend.SetStatePending(sig); err != nil {
			t.Fatalf("seed SetStatePending: %v", err)
		}
	}
	if err := backend.InitGroup("seed-group", []string{"seed-0", "seed-1", "seed-2"}); err != nil {
		t.Fatalf("seed InitGroup: %v", err)
	}

	var wg sync.WaitGroup
	for g := range numGoroutines {
		wg.Go(func() {
			for i := range numIterations {
				uuid := fmt.Sprintf("task-%d-%d", g, i)
				sig := &tasks.Signature{UUID: uuid}

				// Writers: cycle through the SetState* surface.
				_ = backend.SetStatePending(sig)
				_ = backend.SetStateReceived(sig)
				_ = backend.SetStateStarted(sig)
				_ = backend.SetStateSuccess(sig, nil)

				// Readers: GetState, group-state surfaces.
				_, _ = backend.GetState(uuid)
				_, _ = backend.GetState(fmt.Sprintf("seed-%d", g))
				_, _ = backend.GroupCompleted("seed-group", 3)
				_, _ = backend.GroupTaskStates("seed-group", 3)

				// Group writers + purge: exercise the groups-map and tasks-map mutation paths.
				groupID := fmt.Sprintf("group-%d-%d", g, i)
				_ = backend.InitGroup(groupID, []string{uuid})
				_ = backend.PurgeGroupMeta(groupID)
				_ = backend.PurgeState(uuid)
			}
		})
	}
	wg.Wait()
}
