package kafka

import "time"

type KafkaHeaders struct {
	TimeStamp    time.Time
	UserName     string
	IdentityName string
	IdentityType string
	Version      int
	MessageType  string
}
