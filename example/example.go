package main

import (
	"math/rand"
	"time"

	"github.com/mywrap/kafka"
	"github.com/mywrap/log"
)

var brokers = "127.0.0.1:9092"

func mainProducer() {
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BrokersList:  brokers,
		RequiredAcks: kafka.WaitForAll,
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if i > 0 {
			time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		}
		producer.ProduceJSON("TestTopic0", map[string]interface{}{
			"header": "header0",
			"data":   map[string]string{"key0": "val0"},
		})
	}
	time.Sleep(3 * time.Second)
	log.Debugf("producer metric: %#v", producer.MetricSuccess.GetCurrentMetric())
}

func mainConsumer() {
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: brokers,
		GroupId:          "TestGroup5",
		Offset:           kafka.OffsetEarliest,
		Topics:           "TestTopic0",
	})
	if err != nil {
		log.Fatal(err)
	}
	for {
		msgs, err := consumer.Consume()
		if err != nil {
			log.Printf("error when consumer ReadMessage: %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		for _, msg := range msgs {
			log.Debugf("msg created at %v: %v", msg.Timestamp, msg.Value)
		}
	}
}

func main() {
	//mainProducer()
	mainConsumer()
}
