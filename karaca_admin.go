package karaca_kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
)

type KaracaAdmin interface {
	Close()
}

type karacaAdmin struct {
	KafkaAdmin *kafka.AdminClient
}

func (ka *karacaAdmin) Close() {
	ka.KafkaAdmin.Close()
	log.Println("Karaca admin client closed")
}

func NewAdminFromKaracaProducer(kp KaracaProducer) (KaracaAdmin, error) {

	var (
		kafkaAdmin *kafka.AdminClient
		err        error
	)

	karacaProducer, ok := kp.(*karacaProducer)
	if !ok {
		err := fmt.Errorf("unable to cast KaracaProducer to *karacaProducer")
		log.Printf("Error: %v", err)
		return nil, err
	}

	kafkaAdmin, err = kafka.NewAdminClientFromProducer(karacaProducer.Producer)
	if err != nil {
		log.Printf("Error occurred when creating admin client: %v", err)
		return nil, err
	}

	log.Printf("Created Karaca Admin Client %v\n", kafkaAdmin)

	return &karacaAdmin{
		KafkaAdmin: kafkaAdmin,
	}, nil
}
