package kafka_admin

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mustafatheconqueror/karaca-kafka/producer"
	"log"
)

type AdminClient interface {
	Close()
	GetKafkaAdmin() *kafka.AdminClient
}

type kafkaAdminClient struct {
	Admin *kafka.AdminClient
}

func (a *kafkaAdminClient) Close() {
	a.Admin.Close()
	log.Println("Kafka admin client closed")
}

func (a *kafkaAdminClient) GetKafkaAdmin() *kafka.AdminClient {
	return a.Admin
}

func NewAdminFromProducer(p producer.Producer) (AdminClient, error) {

	var (
		kafkaAdmin *kafka.AdminClient
		err        error
	)

	kafkaAdmin, err = kafka.NewAdminClientFromProducer(p.GetKafkaProducer())
	if err != nil {
		log.Printf("Error occurred when creating admin client: %v", err)
		return nil, err
	}

	log.Printf("Created Admin Client %v\n", kafkaAdmin)

	return &kafkaAdminClient{
		Admin: kafkaAdmin,
	}, nil
}
