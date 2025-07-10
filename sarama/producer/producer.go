package main

import (
	"github.com/IBM/sarama"
	"math/rand"
	"time"
)

var brokers = []string{"localhost:9092", "localhost:9093", "localhost:9094"}

func newSyncProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}

func newAsyncProducer() (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer(brokers, config)

	return producer, err
}

func prepareMessage(topic string, message []byte) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(message),
	}

	return msg
}

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

func generateOrder() OrderInfo {
	productCount := rand.Intn(20)

	products := make([]ProductInfo, 0, productCount)
	for i := 0; i < productCount; i++ {
		products = append(products, ProductInfo{
			SKU:   int64(rand.Intn(100)),
			Price: float64(rand.Intn(100)),
			Cnt:   int64(rand.Intn(10)),
		})

	}

	return OrderInfo{
		UserID:    int64(rand.Intn(100)),
		CreatedAt: time.Now(),
		Products:  products,
	}
}
