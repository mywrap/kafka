package kafka

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mywrap/gofast"
	"github.com/mywrap/metric"
)

// this test need a running kafka server,
// example setup: https://github.com/daominah/zookafka
func Test_Kafka(t *testing.T) {
	producer, err := NewProducer(ProducerConfig{
		BrokersList:  "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092",
		RequiredAcks: WaitForAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	newTestTopic := fmt.Sprintf("topic_%v", gofast.UUIDGenNoHyphen())
	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092",
		GroupId:          fmt.Sprintf("group_%v", gofast.UUIDGenNoHyphen()),
		Offset:           OffsetEarliest,
		Topics:           newTestTopic,
	})
	if err != nil {
		t.Fatal(err)
	}

	rMetric := metric.NewMemoryMetric()
	nReceived := 0
	go func() {
		for {
			msg, err := consumer.ReadMessage(1 * time.Second)
			if err != nil {
				if err != ErrReadMsgTimeout {
					t.Errorf("error consumer ReadMessage: %v", err)
				}
				continue
			}
			nReceived += 1
			rMetric.Count(fmt.Sprintf("%v", msg.Partition))
		}
	}()

	nMsgs := 1000
	for i := 0; i < nMsgs; i++ {
		producer.SendMessage(newTestTopic,
			"msg at "+time.Now().Format(time.RFC3339Nano))
	}
	time.Sleep(1 * time.Second) // wait for consumer

	t.Logf("consumer group: %v", consumer.groupId)
	for _, v := range rMetric.GetCurrentMetric() {
		t.Logf("key: %v, count: %v", v.Key, v.RequestCount)
	}
	if nReceived != nMsgs {
		t.Errorf("received expected: %v, real: %v", nMsgs, nReceived)
	}

	//t.Logf("%#v", producer.Metric.GetCurrentMetric())
	nSent := 0
	for _, v := range producer.Metric.GetCurrentMetric() {
		t.Logf("key: %v, count: %v", v.Key, v.RequestCount)
		if strings.Contains(v.Key, "success") {
			nSent += v.RequestCount
		}
	}
	if nSent != nMsgs {
		t.Errorf("sent expected: %v, real: %v", nMsgs, nSent)
	}

	t.Logf("time %v: sent: %v, received: %v",
		time.Now().Format(time.RFC3339), nSent, nReceived)
}
