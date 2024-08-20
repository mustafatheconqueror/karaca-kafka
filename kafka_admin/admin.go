package kafka_admin

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"time"
)

// NewAdminFromProducer creates a new Kafka admin client  based on the provided producer configuration.
func NewAdminFromProducer(producer *kafka.Producer) (*kafka.AdminClient, error) {

	var (
		kafkaAdmin *kafka.AdminClient
		err        error
	)

	kafkaAdmin, err = kafka.NewAdminClientFromProducer(producer)
	if err != nil {
		log.Printf("Error occurred when creating admin client: %v", err)
		time.Sleep(5 * time.Second)
	}

	return kafkaAdmin, err
}
