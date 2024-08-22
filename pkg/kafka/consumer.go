package kafka

import (
	"github.com/mustafatheconqueror/karaca-kafka/internal"
)

type MessageHandler func(message internal.KafkaMessage) error

type Consumer interface {
	StartConsume(messageHandler MessageHandler) error
}
