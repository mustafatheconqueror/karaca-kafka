package main

import (
	"context"
	"fmt"
	karacakafkaConst "github.com/mustafatheconqueror/karaca-kafka/pkg/constants"
	karacakafka "github.com/mustafatheconqueror/karaca-kafka/pkg/kafka"
	"time"
)

func main() {
	singleConsumer()
}

func singleConsumer() {

	var consumerConfig = karacakafka.ConsumerConfig{
		Brokers:             []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		AppName:             "order.created.consumer",
		Topics:              []string{"hepsiburada.oms.order.created.v1.main"},
		AutoOffsetResetType: karacakafkaConst.AutoOffsetResetTypeEarliest,
	}

	var producerConfig = karacakafka.ProducerConfig{
		Brokers:           []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		AcknowledgeType:   karacakafkaConst.AcknowledgeTypeAll,
		CompressionType:   karacakafkaConst.CompressionTypeGzip,
		DeliveryTimeoutMs: karacakafkaConst.DefaultDeliveryTimeoutMs * time.Second,
	}

	var readerConfig = karacakafka.ReaderConfig{
		Brokers:               []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		GroupID:               "groupID",
		AutoOffsetResetType:   karacakafkaConst.AutoOffsetResetTypeEarliest,
		AllowAutoCreateTopics: false,
		EnableAutoCommit:      false,
		FetchMaxBytes:         6428800,
		SessionTimeout:        10 * time.Second,
		Debug:                 "consumer",
		ClientID:              "",
	}

	var karacaKafkaConfig = karacakafka.KaracaKafkaConfig{
		ConsumerConfig: consumerConfig,
		ReaderConfig:   readerConfig,
		ProducerConfig: producerConfig,
	}

	var kafkaMessageBus = karacakafka.NewKaracaKafkaConsumer(context.Background(), karacaKafkaConfig)
	err := kafkaMessageBus.StartConsume(singleConsumerHandler)
	if err != nil {
		return
	}
}

// singleConsumerHandler is the handler function that will be called for each consumed message
func singleConsumerHandler(message karacakafka.KafkaMessage) error {
	var ()
	fmt.Println(message)
	return nil
}
