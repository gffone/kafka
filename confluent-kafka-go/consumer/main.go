package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

var brokers = []string{"localhost:9092", "localhost:9093", "localhost:9094"}

const (
	topic         = "example-kafka-topic"
	consumerGroup = "example-kafka-consumer-group"
)

func customHandler(message []byte, offset kafka.Offset) error {
	logrus.Infof("Message from Kafka \"%s\", offset %d", string(message), offset)
	return nil
}

func main() {
	consumer, err := NewConsumer(brokers, customHandler, topic, consumerGroup)
	if err != nil {
		logrus.Fatal(err)
	}

	go consumer.Consume()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	err = consumer.Stop()
	if err != nil {
		logrus.Fatal(err)
	}
}
