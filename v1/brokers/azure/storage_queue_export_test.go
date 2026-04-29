package azure

import (
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
)

func NewTestBroker() *Broker {
	cnf := &config.Config{
		DefaultQueue: "test_queue",
		Azure:        &config.AzureConfig{},
	}
	return &Broker{
		Broker:            common.NewBroker(cnf),
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
		cfg:               *cnf.Azure,
		queueName:         cnf.DefaultQueue,
	}
}

func (b *Broker) InitializePoolForTest(pool chan struct{}, concurrency int) {
	b.initializePool(pool, concurrency)
}

func (b *Broker) StopReceivingForTest() {
	b.stopReceiving()
}

func (b *Broker) GetStopReceivingChanForTest() chan int {
	return b.stopReceivingChan
}

func (b *Broker) ConsumeOneForTest(delivery azqueue.DequeueMessagesResponse, taskProcessor iface.TaskProcessor) error {
	return b.consumeOne(delivery, taskProcessor)
}
