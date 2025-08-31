package consumer_test

import (
	"context"
	"testing"

	"github.com/messaging-go/kafka/test/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/messaging-go/kafka/middlewares/consumer"
)

func TestNewConsumerMiddleware(t *testing.T) {
	t.Run("If the item is not nil, it just calls next with the item", func(t *testing.T) {
		t.Parallel()
		middleware := consumer.NewConsumerMiddleware(nil)
		dummyMessage := &kafka.Message{Key: []byte(`key`)}
		require.NoError(t, middleware.Process(t.Context(), dummyMessage, func(ctx context.Context, item *kafka.Message) error {
			require.Equal(t, []byte(`key`), item.Key)

			return nil
		}))
	})
	t.Run("If consumer returns nil, it returns a nil", func(t *testing.T) {
		t.Parallel()
		mockConsumer := mocks.NewMockConsumer(gomock.NewController(t))
		mockConsumer.EXPECT().GetMessage().Return(nil)
		middleware := consumer.NewConsumerMiddleware(mockConsumer)
		require.NoError(t, middleware.Process(t.Context(), nil, func(ctx context.Context, item *kafka.Message) error {
			panic("should not be called")
		}))
	})
	t.Run("calls next with the message if consumer returns a message", func(t *testing.T) {
		t.Parallel()
		mockConsumer := mocks.NewMockConsumer(gomock.NewController(t))
		mockConsumer.EXPECT().GetMessage().Return(&kafka.Message{Key: []byte(`key`)})
		mockConsumer.EXPECT().Commit(gomock.Any()).Return(nil)
		middleware := consumer.NewConsumerMiddleware(mockConsumer)
		require.NoError(t, middleware.Process(t.Context(), nil, func(ctx context.Context, item *kafka.Message) error {
			require.Equal(t, []byte(`key`), item.Key)

			return nil
		}))
	})
}
