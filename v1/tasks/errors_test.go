package tasks_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestCheck(t *testing.T) {
	t.Parallel()

	var errInterface interface{}
	err := tasks.NewErrKeepAndRetryTaskLater(fmt.Errorf("blah"), "blah", time.Second)
	errInterface = err
	errValue := reflect.ValueOf(errInterface)

	retriableErrorInterface := reflect.TypeOf((*tasks.Retriable)(nil)).Elem()
	assert.True(t, errValue.Type().Implements(retriableErrorInterface))
}
