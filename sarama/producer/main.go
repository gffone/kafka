package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

func main() {
	syncProducer, err := newSyncProducer()
	if err != nil {
		panic(err)
	}

	asyncProducer, err := newAsyncProducer()
	if err != nil {
		panic(err)
	}

	go func() {
		for err := range asyncProducer.Errors() {
			fmt.Printf("async producer error: %s\n", err)
		}
	}()

	go func() {
		for succ := range asyncProducer.Successes() {
			fmt.Printf("message writen async, partition: %d, offset: %d\n", succ.Partition, succ.Offset)
		}
	}()

	for {
		oi := generateOrder()

		oiJSON, err := json.Marshal(&oi)
		if err != nil {
			panic(err)
		}

		msg := prepareMessage("test-topic", oiJSON)

		if rand.Int()%2 == 0 {
			partition, offset, err := syncProducer.SendMessage(msg)
			if err != nil {
				fmt.Printf("sync send message error: %s\n", err)
				fmt.Printf("partition: %d, offset: %d\n", partition, offset)
			} else {
				fmt.Printf("message writen sync, partition: %d, offset: %d\n", partition, offset)
			}
		} else {
			asyncProducer.Input() <- msg
		}

		time.Sleep(time.Millisecond * 500)

	}
}
