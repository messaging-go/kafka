generate:
	go tool mockgen -destination=./test/mocks/mock_core.go -package=mocks -typed github.com/messaging-go/kafka/consumer Consumer
