package main

import (
	"time"

	"github.com/mywrap/kafka"
	"github.com/mywrap/log"
)

var brokers = "127.0.0.1:9092"

func mainProducer() {
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BrokersList:  brokers,
		RequiredAcks: kafka.WaitForLocal,
	})
	if err != nil {
		log.Fatal(err)
	}
	producer.ProduceJSON("TestTopic0", map[string]interface{}{
		"key0": "val0",
		"key1": map[string]string{"nestedKey": "nestedVal"},
	})

	// func producer.Produce is non-blocking, if main return before messages are
	// flushed, you may lose messages, so you need to call producer.Close()
	producer.Close()

	log.Printf("producer metric: %+v", producer.MetricSuccess.GetCurrentMetric())
}

func mainConsumer() {
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: brokers,
		GroupId:          "TestGroup1",
		Offset:           kafka.OffsetEarliest, // only meaningful if this GroupId has never committed an offset
		Topics:           "TestTopic0,TestTopic1",
	})
	if err != nil {
		log.Fatal(err)
	}
	for {
		msgs, err := consumer.Consume()
		if err != nil {
			log.Printf("error Consume: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		for _, msg := range msgs {
			log.Printf("consumed from %v:%v:%v message: %v",
				msg.Topic, msg.Partition, msg.Offset, msg.Value)
		}
	}
}

func main() {
	mainProducer()
	//mainConsumer()
}
