package consumer_test

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/messaging-go/kafka/consumer"
)

func TestNew(t *testing.T) {
	t.Parallel()
	t.Run("returns nil if there is no message to consume", func(t *testing.T) {
		t.Parallel()

		confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  "localhost",
			"group.id":           "test",
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": true,
			"session.timeout.ms": 6000,
		})
		require.NoError(t, err)

		topic := "empty_topic"
		con, err := consumer.New(confluentConsumer, []string{topic})
		require.NoError(t, err)
		require.NotNil(t, con)
		assert.Nil(t, con.GetMessage())
	})
	t.Run("can consume a message", func(t *testing.T) {
		t.Parallel()

		confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  "localhost",
			"group.id":           "test",
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": true,
			"session.timeout.ms": 6000,
		})
		require.NoError(t, err)

		topic := "test"
		con, err := consumer.New(confluentConsumer, []string{topic})
		require.NoError(t, err)
		require.NotNil(t, con)

		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers":  "localhost",
			"client.id":          "test",
			"message.timeout.ms": 1000,
		})
		require.NoError(t, err)

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic},
			Value:          []byte(`test`),
		}
		require.NoError(t, producer.Produce(msg, nil))
		producer.Flush(1000)

		for {
			message := con.GetMessage()
			if message == nil {
				continue
			}

			assert.Equal(t, "test", string(message.Value))
			assert.NoError(t, con.Commit(message))
			assert.NoError(t, con.Close())

			break
		}
	})
	t.Run("returns error if a closed consumer is used", func(t *testing.T) {
		t.Parallel()

		confluentConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  "localhost",
			"group.id":           "test",
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": true,
			"session.timeout.ms": 6000,
		})
		require.NoError(t, err)
		require.NoError(t, confluentConsumer.Close())

		topic := "empty_topic"
		con, err := consumer.New(confluentConsumer, []string{topic})
		require.Error(t, err)
		require.Nil(t, con)
	})
}
