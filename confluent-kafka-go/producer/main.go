package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
)

const (
	topic = "example-kafka-topic"
)

var brokers = []string{"localhost:9092", "localhost:9093", "localhost:9094"}

func main() {
	producer, err := NewProducer(brokers)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("example kafka message %d", i)
		if err = producer.Produce(msg, topic); err != nil {
			logrus.Error(fmt.Errorf("error producing message: %w", err))
		}
	}
}
