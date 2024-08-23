package karaca_kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"strings"
)

type KaracaProducer interface {
	Produce(topic string, key []byte, value []byte) error
	Close() error
	ListenEvents() chan kafka.Event
}

type karacaProducer struct {
	Producer *kafka.Producer
}

func (kp *karacaProducer) Produce(topic string, key []byte, value []byte) error {

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}
	err := kp.Producer.Produce(message, nil)
	if err != nil {
		log.Printf("Error occurred when producing message: %v", err)
		return err
	}

	log.Printf("Message produced to topic %s: key = %s, value = %s\n", topic, string(key), string(value))
	return nil
}

func (kp *karacaProducer) Close() error {

	log.Println("Closing Kafka producer...")

	kp.Producer.Flush(15 * 1000) // Wait for up to 15 seconds for message deliveries before closing

	kp.Producer.Close()

	log.Println("Kafka producer closed successfully.")
	return nil
}

func (kp *karacaProducer) ListenEvents() chan kafka.Event {
	return kp.Producer.Events()
}

func NewKaracaProducer(producerConfig ProducerConfig) (KaracaProducer, error) {

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
		// todo: burası hiçbir zaman error yakalamayacak çünkü kafkaproducer async şekilde error fırlatıyor her türlü producer nesnesi create edecek.
		log.Printf("Error occurred when creating new producer: %v", err)
	}

	log.Printf("Karaca Producer Created %v\n", producer)

	return &karacaProducer{
		Producer: producer,
	}, nil
}
