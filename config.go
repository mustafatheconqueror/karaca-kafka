package karaca_kafka

import (
	"time"
)

type KaracaKafkaConfig struct {
	ConsumerConfig ConsumerConfig
	ProducerConfig ProducerConfig
	ReaderConfig   ReaderConfig
}

type ConsumerConfig struct {
	Brokers             []string
	AppName             string
	Topics              []string
	AutoOffsetResetType string
	TopicDomainName     string
	TopicSubDomainName  string
}

type ProducerConfig struct {
	Brokers            []string
	AcknowledgeType    string
	CompressionType    string
	DeliveryTimeoutMs  time.Duration
	TopicDomainName    string
	TopicSubDomainName string
}

type ReaderConfig struct {
	Brokers               []string
	GroupID               string
	AutoOffsetResetType   string
	AllowAutoCreateTopics bool
	EnableAutoCommit      bool
	FetchMaxBytes         int
	SessionTimeout        time.Duration
	Debug                 string
	ClientID              string
	TopicDomainName       string
	TopicSubDomainName    string
}
