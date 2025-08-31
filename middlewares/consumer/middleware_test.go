package consumer_test

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/messaging-go/kafka/middlewares/consumer"
	"github.com/messaging-go/kafka/test/mocks"
)

func TestNewConsumerMiddleware(t *testing.T) {
	t.Parallel()
	t.Run("If the item is not nil, it just calls next with the item", func(t *testing.T) {
		t.Parallel()

		middleware := consumer.NewConsumerMiddleware(nil)
		dummyMessage := &kafka.Message{Key: []byte(`key`)}
		err := middleware.Process(
			t.Context(),
			dummyMessage,
			func(ctx context.Context, item *kafka.Message) error {
				require.Equal(t, []byte(`key`), item.Key)

				return nil
			})
		require.NoError(t, err)
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
