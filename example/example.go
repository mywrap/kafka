package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/mywrap/gofast"
	"github.com/mywrap/kafka"
	"github.com/mywrap/log"
	"github.com/mywrap/metric"
)

const brokers = "127.0.0.1:9092"

const topicR = "tuxedo"
const topicW = "topicW0"

func mainProducer() {
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BrokersList:  brokers,
		RequiredAcks: kafka.WaitForAll,
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_ = rand.Int63n
		//time.Sleep(time.Duration(rand.Int63n(40)) * time.Millisecond)
		producer.ProduceJSON(topicR, M{
			"messageType": "REQUEST",
			"messageId":   fmt.Sprintf("%v_%v", time.Now().UnixNano(), gofast.UUIDGen()),
			"uri":         "/api/v1/equity/order/history",
			"responseDestination": M{
				"topic": topicW, "uri": "REQUEST_RESPONSE"},
			"data": M{"key0": "val0"},
		})
	}
	time.Sleep(10 * time.Second)
}

func mainConsumer() {
	met := metric.NewMemoryMetric()
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: brokers,
		GroupId:          "exampleGroup3",
		Offset:           kafka.OffsetEarliest,
		Topics:           topicR,
		//Topics:           topicW,
	})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for i := 0; true; i++ {
			msgs, err := consumer.Consume(context.Background())
			if err != nil {
				//log.Printf("error when consumer ReadMessage: %v\n", err)
				continue
			}
			for _, msg := range msgs {
				var x XMsg
				_ = json.Unmarshal([]byte(msg.Value), &x)
				tmpIdx := strings.Index(x.MessageId, "_")
				beginT, _ := strconv.ParseInt(x.MessageId[:tmpIdx], 10, 64)
				met.Count("all")
				met.Duration("all", time.Duration(time.Now().UnixNano()-beginT))
				log.Debugf("%#v", met.GetCurrentMetric())
			}
		}
	}()
	time.Sleep(time.Hour)
}

type M map[string]interface{}
type A []interface{}

type XMsg struct{ MessageId string }

func main() {
	//mainProducer()
	mainConsumer()
}
