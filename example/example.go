package main

import (
	"context"
	"strings"
	"time"

	"github.com/mywrap/kafka"
	"github.com/mywrap/log"
)

const brokers = "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092"

//const brokers = "10.100.50.100:9092,10.100.50.101:9092,10.100.50.102:9092"

const topics = "topicAlice,topicBob,topicCharlie"
const topic0 = "topicAlice"

func main1() { // producer
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BrokersList:  brokers,
		RequiredAcks: kafka.WaitForAll,
	})
	if err != nil {
		log.Fatal(err)
	}
	_ = strings.Split
	//produceTo := strings.Split(topics, ",")
	produceTo := []string{topic0}
	for _, topic := range produceTo {
		for i := 0; i < 3; i++ {
			producer.Produce(topic, "PING"+time.Now().Format(time.RFC3339Nano))
		}
	}
	time.Sleep(2 * time.Second)
}

func main() { // consumer
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: brokers,
		GroupId:          "exampleGroup1",
		Offset:           kafka.OffsetEarliest,
		//Topics:           topics,
		Topics: topic0,
	})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for i := 0; true; i++ {
			log.Debugf("loop Consume round %v", i)
			msgs, err := consumer.Consume(context.Background())
			if err != nil {
				log.Printf("error when consumer ReadMessage: %v\n", err)
				continue
			}
			for _, msg := range msgs {
				_ = msg // do something
			}
		}
	}()
	time.Sleep(99999 * time.Hour)
}
