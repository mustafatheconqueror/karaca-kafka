package internal

import "time"

type KaracaKafkaMessage struct {
	Payload       []byte
	CorrelationId string
	Timestamp     time.Time
	Headers       KaracaKafkaHeaders
	Topic         string
	Partition     int
}
