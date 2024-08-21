package config

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mustafatheconqueror/karaca-kafka/constants"
	"strings"
	"time"
)

type KafkaReaderConfig struct {
	Brokers               []string
	GroupID               string
	AutoOffsetResetType   string
	AllowAutoCreateTopics bool
	EnableAutoCommit      bool
	FetchMaxBytes         int
	SessionTimeout        time.Duration
	Debug                 string
	ClientID              string
}

func NewKafkaReaderConfig(brokers []string, groupID string) *KafkaReaderConfig {
	return &KafkaReaderConfig{
		Brokers:               brokers,
		GroupID:               groupID,
		AutoOffsetResetType:   constants.AutoOffsetResetTypeEarliest,
		AllowAutoCreateTopics: false,
		EnableAutoCommit:      false,
		FetchMaxBytes:         6428800,
		SessionTimeout:        10 * time.Second,
		Debug:                 "consumer",
		ClientID:              "",
	}
}

func (config *KafkaReaderConfig) ToKafkaConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(config.Brokers, ","),
		"group.id":                 config.GroupID,
		"auto.offset.reset":        config.AutoOffsetResetType,
		"allow.auto.create.topics": fmt.Sprintf("%t", config.AllowAutoCreateTopics),
		"enable.auto.commit":       fmt.Sprintf("%t", config.EnableAutoCommit),
		"fetch.max.bytes":          fmt.Sprintf("%d", config.FetchMaxBytes),
		"session.timeout.ms":       fmt.Sprintf("%d", config.SessionTimeout.Milliseconds()),
		"debug":                    config.Debug,
		"client.id":                config.ClientID,
	}
}
