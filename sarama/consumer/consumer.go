package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"time"
)

type ProductInfo struct {
	SKU   int64
	Price float64
	Cnt   int64
}

type OrderInfo struct {
	UserID    int64
	CreatedAt time.Time
	Products  []ProductInfo
}

type Consumer struct {
	ID int
}

func NewConsumer() *Consumer {
	return &Consumer{}
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var msg OrderInfo
		err := json.Unmarshal(message.Value, &msg)
		if err != nil {
			fmt.Println("err: ", err)
			return err
		}

		fmt.Printf("Msg %v\n", msg.UserID)

		session.MarkMessage(message, "")
	}

	return nil
}

func subscribe(ctx context.Context, topic string, consumerGroup sarama.ConsumerGroup) error {
	consumer := Consumer{}

	go func() {
		fmt.Printf("Start concumer %d, subscribe to topic %s\n", consumer.ID, topic)
		for {
			if err := consumerGroup.Consume(ctx, []string{topic}, &consumer); err != nil {
				fmt.Printf("Error on consumer %d: %v\n", consumer.ID, err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}

var brokers = []string{"localhost:9092", "localhost:9093", "localhost:9094"}

func StartConsuming(ctx context.Context) error {
	config := sarama.NewConfig()

	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerGroup, err := sarama.NewConsumerGroup(brokers, "test-consumer-group", config)
	if err != nil {
		return err
	}

	return subscribe(ctx, "test-topic", consumerGroup)
}
