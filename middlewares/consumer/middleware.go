package consumer

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/messaging-go/kafka/consumer"
)

type Middleware struct {
	consumer consumer.Consumer
}

func NewConsumerMiddleware(consumer consumer.Consumer) *Middleware {
	return &Middleware{
		consumer: consumer,
	}
}

func (c Middleware) Process(
	ctx context.Context,
	item *kafka.Message,
	next func(ctx context.Context, item *kafka.Message) error,
) error {
	if item != nil {
		// I don't think this will ever happen though...
		return next(ctx, item)
	}

	msg := c.consumer.GetMessage()
	if msg == nil {
		return nil
	}
	// we also don't want to commit for every single messages,
	// in the next version, we'd like to offload this to the user.
	// we may use throttle, or autocommit or some other strategies.
	defer c.consumer.Commit(msg) //nolint:errcheck // we'll ignore for the first version

	return next(ctx, msg)
}
