package internal

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	karacakafka "github.com/mustafatheconqueror/karaca-kafka/pkg/kafka"
	"log"
	"strings"
	"time"
)

type KaracaProducer interface {
	ProduceMessage(topic string, key []byte, value []byte) error
	Close()
	GetKafkaProducer() *kafka.Producer
}

type karacaProducer struct {
	Producer *kafka.Producer
}

func (p *karacaProducer) ProduceMessage(topic string, key []byte, value []byte) error {

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}

	return p.Producer.Produce(message, nil)
}

func (p *karacaProducer) Close() {
	p.Producer.Close()
	log.Println("Kafka producer closed")
}

func (p *karacaProducer) GetKafkaProducer() *kafka.Producer {
	return p.Producer
}

func NewProducer(producerConfig karacakafka.ProducerConfig) (KaracaProducer, error) {

	var (
		producer *kafka.Producer
		err      error
	)

	kafkaProducerConfig := &kafka.ConfigMap{
		"bootstrap.servers":   strings.Join(producerConfig.Brokers, ","),
		"acks":                producerConfig.AcknowledgeType,
		"compression.type":    producerConfig.CompressionType,
		"delivery.timeout.ms": fmt.Sprintf("%d", producerConfig.DeliveryTimeoutMs.Milliseconds()),
	}

	producer, err = kafka.NewProducer(kafkaProducerConfig)
	if err != nil {
		log.Printf("Error occurred when creating producer: %v", err)
		time.Sleep(5 * time.Second)
	}

	log.Printf("Created Producer %v\n", producer)

	return &karacaProducer{
		Producer: producer,
	}, nil
}
