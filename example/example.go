package main

import (
	"time"

	"github.com/mywrap/kafka"
	"github.com/mywrap/log"
)

func main() {
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BrokersList:  "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092",
		RequiredAcks: kafka.WaitForAll,
	})
	if err != nil {
		log.Fatal(err)
	}
	producer.SendMessage("topic1", "PING")

	if true {
		consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
			BootstrapServers: "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092",
			GroupId:          "group0",
			Offset:           kafka.OffsetLatest,
			Topics:           "topic0,topic1",
		})
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond) // TODO: NewConsumer should be ready

		go func() {
			receivedCount := 0
			for {
				msg, err := consumer.ReadMessage(1 * time.Second)
				if err != nil {
					if err != kafka.ErrReadMsgTimeout {
						log.Printf("error consumer ReadMessage: %v", err)
					}
					continue
				}
				_ = msg
				receivedCount += 1
				log.Printf("receivedCount: %v", receivedCount)
			}
		}()
	}

	for i := 0; i < 10; i++ {
		producer.SendMessage(
			"topic0",
			"msg at "+time.Now().Format(time.RFC3339Nano),
		)
	}
	time.Sleep(30 * time.Second)
}
