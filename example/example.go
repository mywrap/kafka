package main

import (
	"time"

	"github.com/mywrap/kafka"
	"github.com/mywrap/log"
)

func main() {
	servers := "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092"
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BrokersList:  servers,
		RequiredAcks: kafka.WaitForAll,
	})
	if err != nil {
		log.Fatal(err)
	}

	if false {
		consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
			BootstrapServers: servers,
			GroupId:          "group0",
			Offset:           kafka.OffsetLatest,
			Topics:           "topic0,topic1",
		})
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // TODO: NewConsumer should be ready

		go func() {
			for {
				msg, err := consumer.ReadMessage(1 * time.Second)
				if err != nil {
					continue
				}
				_ = msg
			}
		}()
	}

	producer.SendMessage(
		"topic0",
		"msg at "+time.Now().Format(time.RFC3339Nano),
	)
	time.Sleep(500 * time.Millisecond)
}
