package producer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"karaca-kafka/config"
	"log"
	"time"
)

// NewProducer creates a new Kafka producer based on the provided configuration.
func NewProducer(brokers []string) (*kafka.Producer, error) {

	var (
		producer *kafka.Producer
		err      error
	)

	producerConfig := config.NewKafkaProducerConfig(brokers)

	kafkaConfigMap := producerConfig.ToKafkaConfigMap()

	//Todo: For ile burada döngüsel bir yapı kurgulanabilir.
	producer, err = kafka.NewProducer(kafkaConfigMap)
	if err != nil {
		log.Printf("Error occurred when creating producer: %v", err)
		//Todo: Özelleştirilmiş error bas
		time.Sleep(5 * time.Second)
	}
	//Todo: Log mekanizması inşa edebilirsin
	log.Printf("Created Producer %v\n", producer)

	return producer, err
}
