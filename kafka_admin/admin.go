package kafka_admin

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"karaca-kafka/producer"
	"log"
	"time"
)

func NewAdminFromProducer(producer producer.Producer) (*kafka.AdminClient, error) {

	var (
		kafkaAdmin *kafka.AdminClient
		err        error
	)

	kafkaAdmin, err = kafka.NewAdminClientFromProducer(producer.GetKafkaProducer())
	if err != nil {
		log.Printf("Error occurred when creating admin client: %v", err)
		time.Sleep(5 * time.Second)
	}

	return kafkaAdmin, err
}
