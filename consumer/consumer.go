package consumer

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const kafkaPollDuration = time.Millisecond * 100 // kafka's example uses 100ms, and I'm going with it for now

func (c consumerImpl) GetMessage() *kafka.Message {
	// it does use a buffered channel internally, so the poll only applies when there's no next message yet.
	ev := c.kakfaConsumer.Poll(int(kafkaPollDuration.Milliseconds()))
	if ev == nil {
		return nil
	}

	return getMessageOrNil(ev)
}

func getMessageOrNil(event kafka.Event) *kafka.Message {
	switch e := event.(type) {
	case *kafka.Message:
		return e
	default:
		// Errors should generally be considered informational, the client will try to automatically recover.
		return nil
	}
}

func (c consumerImpl) Commit(message *kafka.Message) error {
	_, err := c.kakfaConsumer.CommitMessage(message)

	return err //nolint:wrapcheck // we don't have additional logic here, so we return the raw error
}

func (c consumerImpl) Close() error {
	return c.kakfaConsumer.Close() //nolint:wrapcheck // we don't have additional logic here.
}

func New(rawConsumer *kafka.Consumer, topics []string) (Consumer, error) {
	err := rawConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	return consumerImpl{kakfaConsumer: rawConsumer}, nil
}
