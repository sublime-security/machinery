package azure_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/RichardKnop/machinery/v1/brokers/azure"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

func TestNew_ImplementsInterfaces(t *testing.T) {
	t.Parallel()

	cnf := &config.Config{
		DefaultQueue: "test_queue",
		Azure:        &config.AzureConfig{},
	}
	broker := azure.NewTestBroker()
	assert.IsType(t, broker, azure.New(cnf))

	// Compile-time check that *Broker satisfies RetrySameMessage.
	var asRetry iface.RetrySameMessage
	asRetry = broker
	assert.NotNil(t, asRetry)
}

func TestInitializePool(t *testing.T) {
	broker := azure.NewTestBroker()
	concurrency := 9
	pool := make(chan struct{}, concurrency)
	broker.InitializePoolForTest(pool, concurrency)
	assert.Len(t, pool, concurrency)
}

func TestStopReceiving(t *testing.T) {
	broker := azure.NewTestBroker()
	go broker.StopReceivingForTest()
	stopReceivingChan := broker.GetStopReceivingChanForTest()
	assert.NotNil(t, <-stopReceivingChan)
}

func TestConsumeOne_EmptyMessages(t *testing.T) {
	broker := azure.NewTestBroker()
	err := broker.ConsumeOneForTest(azqueue.DequeueMessagesResponse{}, nil)
	assert.ErrorContains(t, err, "received empty message")
}
