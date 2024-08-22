package kafka

import (
	"context"
	"github.com/mustafatheconqueror/karaca-kafka/internal"
)

type MessageHandler func(message internal.KafkaMessage) error

type Consumer interface {
	StartConsume(messageHandler MessageHandler) error
}

func NewKaracaKafkaConsumer(ctx context.Context, config KaracaKafkaConfig) internal.KaracaConsumer {
	return internal.NewKafkaConsumer(ctx, config)
}
