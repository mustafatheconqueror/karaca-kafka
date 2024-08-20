package consumer

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"karaca-kafka/config"
	"karaca-kafka/kafka_admin"
	"karaca-kafka/kafka_message"
	"karaca-kafka/producer"
	"log"
	"time"
)

type handler func(message kafka_message.KafkaMessage) error

type Consumer struct {
	Context     context.Context
	AdminClient *kafka.AdminClient
	Producer    *kafka.Producer
	Consumer    *kafka.Consumer
	Config      config.ConsumerConfig
	Handler     handler
	IsClosed    bool
}

func NewKafkaConsumer(ctx context.Context, consumerConfig config.ConsumerConfig, brokers []string) *Consumer {

	var (
		consumer *kafka.Consumer
		err      error
	)

	kafkaProducer, err := producer.NewProducer(brokers)
	kafkaAdmin, err := kafka_admin.NewAdminFromProducer(kafkaProducer)

	readerConfig := config.NewKafkaProducerConfig(brokers)
	readerConfigMap := readerConfig.ToKafkaConfigMap()

	for consumer, err = kafka.NewConsumer(readerConfigMap); err != nil; {
		log.Printf("Error occurred when creating consumer: %v", err)
		time.Sleep(5 * time.Second)
	}

	return &Consumer{
		Context:     ctx,
		AdminClient: kafkaAdmin,
		Producer:    kafkaProducer,
		Consumer:    consumer,
		Config:      consumerConfig,
		IsClosed:    false,
	}
}
