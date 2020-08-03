package kafka

import (
	"context"
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
	topic0 := fmt.Sprintf("topic_%v", gofast.UUIDGenNoHyphen()[:8])

	// test number of successfully sent and received messages
	group0 := fmt.Sprintf("group_%v", gofast.UUIDGenNoHyphen()[:8])
	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092",
		Topics:           topic0,
		GroupId:          group0,
		Offset:           OffsetEarliest,
	})
	if err != nil {
		t.Fatal(err)
	}

	rMetric := metric.NewMemoryMetric()
	nReceived := 0
	go func() {
		for {
			msgs, err := consumer.ReadMessage(context.Background())
			if err != nil {
				t.Errorf("consumer ReadMessage: %v", err)
			}
			nReceived += len(msgs)
			for _, msg := range msgs {
				rMetric.Count(fmt.Sprintf("%v", msg.Partition))
			}
		}
	}()

	nMsgs := 1000
	for i := 0; i < nMsgs; i++ {
		producer.SendMessage(topic0,
			"msg at "+time.Now().Format(time.RFC3339Nano))
	}
	time.Sleep(1 * time.Second) // wait for consumer

	t.Logf("consumer group: %v", consumer.groupId)
	for _, v := range rMetric.GetCurrentMetric() {
		t.Logf("consumer met key: %v, count: %v", v.Key, v.RequestCount)
	}
	if nReceived != nMsgs {
		t.Errorf("received expected: %v, real: %v", nMsgs, nReceived)
	}

	//t.Logf("%#v", producer.Metric.GetCurrentMetric())
	nSent := 0
	for _, v := range producer.Metric.GetCurrentMetric() {
		t.Logf("producer met key: %v, count: %v", v.Key, v.RequestCount)
		if strings.Contains(v.Key, "success") {
			nSent += v.RequestCount
		}
	}
	if nSent != nMsgs {
		t.Errorf("sent expected: %v, real: %v", nMsgs, nSent)
	}

	t.Logf("time %v: sent: %v, received: %v",
		time.Now().Format(time.RFC3339), nSent, nReceived)

	// test re-consume with same groupId
	//group1 := fmt.Sprintf("group_%v", gofast.UUIDGenNoHyphen()[:8])
	//consumer1, err := NewConsumer(ConsumerConfig{
	//	BootstrapServers: "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092",
	//	Topics:           topic0, GroupId: group1, Offset: OffsetEarliest,
	//})
	//consumer1.ReadMessage()
}
