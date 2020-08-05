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
	// test number of successfully sent and received messages
	brokers := "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092"
	topic0 := fmt.Sprintf("topic_%v", gofast.UUIDGenNoHyphen()[:8])

	producer, err := NewProducer(ProducerConfig{
		BrokersList:  brokers,
		RequiredAcks: WaitForAll,
	})
	if err != nil {
		t.Fatal(err)
	}

	producer.Produce("", "msg to empty topic")
	time.Sleep(10 * time.Millisecond)
	metricRows := producer.Metric.GetCurrentMetric()
	if strings.Contains(metricRows[0].Key, "fail") {
		t.Errorf("expected fail because of invalid topic")
	}

	csmT0 := time.Now()
	group0 := fmt.Sprintf("group_%v", gofast.UUIDGenNoHyphen()[:8])
	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: brokers,
		Topics:           topic0,
		GroupId:          group0,
		Offset:           OffsetEarliest,
	})
	if err != nil {
		t.Fatal(err)
	}
	csmT1 := time.Now()

	producer.IsLog = false
	consumer.IsLog = false
	nMsgs := 1000
	rMetric := metric.NewMemoryMetric()
	nReceived := 0
	var csmT2 time.Time
	go func() {
		for {
			//t.Logf("about to consumer Consume")
			msgs, err := consumer.Consume(context.Background())
			if err != nil {
				t.Errorf("error when consumer Consume: %v", err)
				continue
			}
			nReceived += len(msgs)
			for _, msg := range msgs {
				rMetric.Count(fmt.Sprintf("%v:%v", msg.Topic, msg.Partition))
			}
			if nReceived == nMsgs {
				csmT2 = time.Now()
			}
			//t.Logf("nReceived: %v", nReceived)
		}
	}()

	for i := 0; i < nMsgs; i++ {
		msg := "msg at " + time.Now().Format(time.RFC3339Nano)
		if i > 8*nMsgs/10 {
			producer.ProduceWithKey(topic0, msg, "key810")
			continue
		}
		producer.Produce(topic0, msg)
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

	t.Logf("sent: %v, received: %v, durInit: %v, durRead: %v",
		nSent, nReceived, csmT1.Sub(csmT0), csmT2.Sub(csmT1))
}

func TestConsumer_Stop(t *testing.T) {
	// TODO: TestConsumer_Stop
}

func TestConsumer_Reconnect(t *testing.T) {
	// TODO: TestConsumer_Reconnect
}

func TestConsumer_CancelConsume(t *testing.T) {
	// TODO: TestConsumer_CancelConsume
}

func TestProducer_ProduceFail(t *testing.T) {
	// TODO: TestProducer_ProduceFail
}
