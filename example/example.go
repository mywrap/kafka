package main

import (
	"context"
	"log"
	"time"

	"github.com/mywrap/kafka"
)

func main() {
	brokers := "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092"

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BrokersList:  brokers,
		RequiredAcks: kafka.WaitForAll,
	})
	if err != nil {
		log.Fatal(err)
	}
	producer.SendMessage("topic1", "PING")

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: brokers,
		GroupId:          "group0",
		Offset:           kafka.OffsetEarliest,
		Topics:           "topic0,topic1",
	})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		nRecvs := 0
		for {
			msgs, err := consumer.ReadMessage(context.Background())
			if err != nil {
				log.Printf("error when consumer ReadMessage: %v\n", err)
			}
			_ = msgs[0]
			nRecvs += 1
			log.Printf("number of received messages: %v", nRecvs)
		}
	}()

	for i := 0; i < 10; i++ {
		producer.SendMessage(
			"topic0",
			"msg at "+time.Now().Format(time.RFC3339Nano),
		)
	}
	time.Sleep(30 * time.Second)
}
