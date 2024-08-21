package main

import (
	"context"
	"fmt"
	"github.com/mustafatheconqueror/karaca-kafka/config"
	"github.com/mustafatheconqueror/karaca-kafka/constants"
	"github.com/mustafatheconqueror/karaca-kafka/consumer"
	"github.com/mustafatheconqueror/karaca-kafka/kafka_message"
)

func main() {
	singleConsumer()
}

func singleConsumer() {

	var consumerConfig = config.ConsumerConfig{
		Brokers:             []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		AppName:             "order.created.consumer",
		Topics:              []string{"hepsiburada.oms.order.created.v1.main"},
		AutoOffsetResetType: constants.AutoOffsetResetTypeEarliest,
	}

	var kafkaMessageBus = consumer.NewKafkaConsumer(context.Background(), consumerConfig)

	err := kafkaMessageBus.StartConsume(singleConsumerHandler)
	if err != nil {
		return
	}

	return
}

// singleConsumerHandler is the handler function that will be called for each consumed message
func singleConsumerHandler(message kafka_message.KafkaMessage) error {
	var ()
	fmt.Println(message)
	return nil
}
