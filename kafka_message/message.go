package kafka_message

import "time"

type KafkaMessage struct {
	Payload       []byte
	CorrelationId string
	Timestamp     time.Time
	Headers       KafkaHeaders
	Topic         string
	Partition     int
}
