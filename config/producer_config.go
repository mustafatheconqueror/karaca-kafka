package config

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mustafatheconqueror/karaca-kafka/constants"
	"strings"
	"time"
)

type KafkaProducerConfig struct {
	Brokers           []string
	AcknowledgeType   string
	CompressionType   string
	DeliveryTimeoutMs time.Duration
}

func NewKafkaProducerConfig(brokers []string) *KafkaProducerConfig {
	return &KafkaProducerConfig{
		Brokers:           brokers,
		AcknowledgeType:   constants.AcknowledgeTypeAll,
		CompressionType:   constants.CompressionTypeGzip,
		DeliveryTimeoutMs: constants.DefaultDeliveryTimeoutMs * time.Second,
	}
}

// Todo: Burayı daha güzel hale getirebilirsin
func (config *KafkaProducerConfig) ToKafkaConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":   strings.Join(config.Brokers, ","),
		"acks":                config.AcknowledgeType,
		"compression.type":    config.CompressionType,
		"delivery.timeout.ms": fmt.Sprintf("%d", config.DeliveryTimeoutMs.Milliseconds()),
	}
}
