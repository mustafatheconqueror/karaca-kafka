package internal

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
)

type KaracaAdmin interface {
	Close()
	GetKafkaAdmin() *kafka.AdminClient
}

type karacaAdmin struct {
	Admin *kafka.AdminClient
}

func (a *karacaAdmin) Close() {
	a.Admin.Close()
	log.Println("Kafka admin client closed")
}

func (a *karacaAdmin) GetKafkaAdmin() *kafka.AdminClient {
	return a.Admin
}

func NewAdminFromProducer(p KaracaProducer) (KaracaAdmin, error) {

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

	return &karacaAdmin{
		Admin: kafkaAdmin,
	}, nil
}
