package karaca_kafka

import (
	"time"
)

type KaracaMessage struct {
	Payload       []byte
	CorrelationId string
	Timestamp     time.Time
	Headers       *KaracaMessageHeader
	Topic         string
	Partition     int
}
