package config

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"karaca-kafka/constants"
	"strings"
	"time"
)

// KafkaProducerConfig holds the configuration values for the Kafka producer.
type KafkaProducerConfig struct {
	Brokers           []string
	AcknowledgeType   string
	CompressionType   string
	DeliveryTimeoutMs time.Duration
}

// NewKafkaProducerConfig creates a new KafkaProducerConfig with default values.
func NewKafkaProducerConfig(brokers []string) *KafkaProducerConfig {
	return &KafkaProducerConfig{
		Brokers:           brokers,
		AcknowledgeType:   constants.AcknowledgeTypeAll,
		CompressionType:   constants.CompressionTypeGzip,
		DeliveryTimeoutMs: constants.DefaultDeliveryTimeoutMs * time.Second,
	}
}

// ToKafkaConfigMap converts KafkaProducerConfig to a kafka.ConfigMap.
// Todo: Burayı daha güzel hale getirebilirsin
func (config *KafkaProducerConfig) ToKafkaConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":   strings.Join(config.Brokers, ","),
		"acks":                config.AcknowledgeType,
		"compression.type":    config.CompressionType,
		"delivery.timeout.ms": fmt.Sprintf("%d", config.DeliveryTimeoutMs.Milliseconds()),
	}
}
