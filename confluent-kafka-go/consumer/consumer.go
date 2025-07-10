package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"strings"
)

const (
	sessionTimeout     = 7000 // ms
	readMessageTimeout = -1   // no timeout
)

type Handler func(message []byte, offset kafka.Offset) error

type Consumer struct {
	consumer *kafka.Consumer
	handler  Handler
	stop     bool
}

func NewConsumer(brokers []string, handler Handler, topic string, consumerGroup string) (*Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(brokers, ","),
		"group.id":           consumerGroup,
		"session.timeout.ms": sessionTimeout,
		// Вручную локально сохраняем оффсет после обработки сообщения
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  5000,
		// При подключении начинаем читать с самого начала
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer: consumer,
		handler:  handler,
	}, nil
}

func (c *Consumer) Consume() {
	for {
		kafkaMessage, err := c.consumer.ReadMessage(readMessageTimeout)
		if err != nil {
			logrus.Error(fmt.Errorf("error reading message: %w", err))
		}

		if kafkaMessage == nil {
			continue
		}

		err = c.handler(kafkaMessage.Value, kafkaMessage.TopicPartition.Offset)
		if err != nil {
			logrus.Error(fmt.Errorf("error handle message: %w", err))
			continue
		}

		if _, err = c.consumer.StoreMessage(kafkaMessage); err != nil {
		}
	}
}

func (c *Consumer) Stop() error {
	c.stop = true
	if _, err := c.consumer.Commit(); err != nil {
		logrus.Error(fmt.Errorf("error commiting offsets: %w", err))
	}
	return c.consumer.Close()
}
