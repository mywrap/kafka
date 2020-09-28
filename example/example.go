package main

import (
	"context"
	"strings"
	"time"

	"github.com/mywrap/gofast"
	"github.com/mywrap/kafka"
	"github.com/mywrap/log"
)

const brokers = "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092"
const topics = "topicAlice,topicBob,topicCharlie"

//const brokers = "10.100.50.100:9092,10.100.50.101:9092,10.100.50.102:9092"

func main() { // producer
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BrokersList:  brokers,
		RequiredAcks: kafka.WaitForAll,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, topic := range strings.Split(topics, ",") {
		producer.Produce(topic, "PING"+time.Now().Format(time.RFC3339Nano))
	}
	time.Sleep(2 * time.Second)
}

func main1() { // consumer
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: brokers,
		GroupId:          "exampleGroup" + gofast.UUIDGenNoHyphen(),
		Offset:           kafka.OffsetLatest,
		Topics:           topics,
	})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			msgs, err := consumer.Consume(context.Background())
			if err != nil {
				log.Printf("error when consumer ReadMessage: %v\n", err)
				continue
			}
			for _, msg := range msgs {
				_ = msg
			}
		}
	}()
	time.Sleep(99999 * time.Hour)
}
