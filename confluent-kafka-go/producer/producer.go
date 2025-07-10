package main

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strings"
)

const (
	flushTimeout = 5000 // ms
)

var errUnknownType error = errors.New("unknown event type")

type KafkaProducer struct {
	producer *kafka.Producer
}

func NewProducer(brokers []string) (*KafkaProducer, error) {

	//https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
	config := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(brokers, ","),
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("error creating new producer: %w", err)
	}

	return &KafkaProducer{
		producer: producer,
	}, nil
}

func (p *KafkaProducer) Produce(message string, topic string) error {
	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
		Key:   nil,
	}

	eventsChan := make(chan kafka.Event)

	err := p.producer.Produce(kafkaMessage, eventsChan)
	if err != nil {
		return fmt.Errorf("error producing message: %w", err)
	}

	event := <-eventsChan
	switch ev := event.(type) {
	case *kafka.Message:
		return nil
	case kafka.Error:
		return ev
	default:
		return errUnknownType
	}
}

func (p *KafkaProducer) Close() {
	// Ждем либо отправки оставшихся сообщений, либо flushTimeout
	p.producer.Flush(flushTimeout)
	p.producer.Close()
}
