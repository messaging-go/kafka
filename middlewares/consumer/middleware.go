package consumer

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/messaging-go/kafka/consumer"
)

type Middleware struct {
	consumer consumer.Consumer
}

func (c Middleware) Process(ctx context.Context, item *kafka.Message, next func(ctx context.Context, item *kafka.Message) error) error {
	if item != nil {
		// I don't think this will ever happen though...
		return next(ctx, item)
	}
	msg := c.consumer.GetMessage()
	if msg == nil {
		return nil
	}
	defer c.consumer.Commit(msg)

	return next(ctx, msg)
}

func NewConsumerMiddleware(consumer consumer.Consumer) *Middleware {
	return &Middleware{
		consumer: consumer,
	}
}
