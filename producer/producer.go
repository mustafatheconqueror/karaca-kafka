package producer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mustafatheconqueror/karaca-kafka/config"
	"log"
	"time"
)

type Producer interface {
	ProduceMessage(topic string, key []byte, value []byte) error
	Close()
	GetKafkaProducer() *kafka.Producer
}

type kafkaProducer struct {
	Producer *kafka.Producer
}

func (p *kafkaProducer) ProduceMessage(topic string, key []byte, value []byte) error {

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}

	return p.Producer.Produce(message, nil)
}

func (p *kafkaProducer) Close() {
	p.Producer.Close()
	log.Println("Kafka producer closed")
}

func (p *kafkaProducer) GetKafkaProducer() *kafka.Producer {
	return p.Producer
}

func NewProducer(brokers []string) (Producer, error) {

	var (
		producer *kafka.Producer
		err      error
	)

	producerConfig := config.NewKafkaProducerConfig(brokers)

	kafkaConfigMap := producerConfig.ToKafkaConfigMap()

	producer, err = kafka.NewProducer(kafkaConfigMap)
	if err != nil {
		log.Printf("Error occurred when creating producer: %v", err)
		time.Sleep(5 * time.Second)
	}

	log.Printf("Created Producer %v\n", producer)

	return &kafkaProducer{
		Producer: producer,
	}, nil
}
