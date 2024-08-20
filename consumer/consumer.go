package consumer

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"karaca-kafka/config"
	"karaca-kafka/kafka_admin"
	"karaca-kafka/kafka_message"
	"karaca-kafka/producer"
	"log"
	"time"
)

type handler func(message kafka_message.KafkaMessage) error

type Consumer interface {
	Unsubscribe()
}

type kafkaConsumer struct {
	Context     context.Context
	AdminClient *kafka.AdminClient
	Producer    *kafka.Producer
	Consumer    *kafka.Consumer
	Config      config.ConsumerConfig
	Handler     handler
	IsClosed    bool
}

func (c *kafkaConsumer) Unsubscribe() {
	fmt.Println("Kafka consumer closed")
}

func NewKafkaConsumer(ctx context.Context, consumerConfig config.ConsumerConfig, brokers []string) Consumer {

	var (
		consumer *kafka.Consumer
		err      error
	)

	kafkaProducer, err := producer.NewProducer(brokers)
	if err != nil {
		log.Printf("Error occurred when creating producer: %v", err)
	}

	kafkaAdmin, err := kafka_admin.NewAdminFromProducer(kafkaProducer)
	if err != nil {
		log.Printf("Error occurred when creating admin client: %v", err)
	}

	readerConfig := config.NewKafkaReaderConfig(brokers, consumerConfig.AppName)
	readerConfigMap := readerConfig.ToKafkaConfigMap()

	consumer, err = kafka.NewConsumer(readerConfigMap)
	if err != nil {
		log.Printf("Error occurred when creating consumer: %v", err)
		time.Sleep(5 * time.Second)
	}

	return &kafkaConsumer{
		Context:     ctx,
		AdminClient: kafkaAdmin,
		Producer:    kafkaProducer.GetKafkaProducer(),
		Consumer:    consumer,
		Config:      consumerConfig,
		IsClosed:    false,
	}
}
