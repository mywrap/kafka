package kafka

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mywrap/gofast"
	"github.com/mywrap/metric"
)

// the following tests need a running kafka server,
// example setup: https://github.com/daominah/zookafka

//const brokersTest = "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092"
//const brokersTest = "10.100.50.100:9092,10.100.50.101:9092,10.100.50.102:9092"
const brokersTest = "127.0.0.1:9092"

func Test_Kafka(t *testing.T) {
	// test number of successfully sent and received messages

	topic0 := fmt.Sprintf("topic_%v", gofast.UUIDGenNoHyphen()[:8])

	producer, err := NewProducer(ProducerConfig{
		BrokersList:  brokersTest,
		RequiredAcks: WaitForAll,
	})
	if err != nil {
		t.Fatal(err)
	}

	producer.Produce("", "msg to invalid topic")
	time.Sleep(10 * time.Millisecond)
	_, nErrors := producer.getNumberOfSuccessError()
	if nErrors != 1 {
		t.Errorf("expected 1 fail but nFails = %v", nErrors)
	} else {
		t.Logf("pass test invalid topic")
	}

	csmT0 := time.Now()
	group0 := fmt.Sprintf("group_%v", gofast.UUIDGenNoHyphen()[:8])
	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: brokersTest,
		Topics:           topic0,
		GroupId:          group0,
		Offset:           OffsetEarliest,
	})
	if err != nil {
		t.Fatal(err)
	}
	csmT1 := time.Now()

	producer.conf.DisableLog = true
	const nMsgs = 1000
	rMetric := metric.NewMemoryMetric()
	nReceived := 0
	var csmT2 time.Time
	mu := sync.Mutex{}
	go func() {
		for {
			//t.Logf("about to consumer Consume")
			msgs, err := consumer.Consume()
			if err != nil {
				t.Errorf("error when consumer Consume: %v", err)
				continue
			}
			nReceived += len(msgs)
			for _, msg := range msgs {
				rMetric.Count(fmt.Sprintf("%v:%v", msg.Topic, msg.Partition))
			}
			if nReceived == nMsgs {
				mu.Lock()
				csmT2 = time.Now()
				mu.Unlock()
			}
			//t.Logf("nReceived: %v", nReceived)
		}
	}()

	for i := 0; i < nMsgs; i++ {
		msg := "msg i %03d at " + time.Now().Format(time.RFC3339Nano)
		if i > 8*nMsgs/10 {
			producer.ProduceWithKey(topic0, msg, "key810")
			continue
		}
		producer.Produce(topic0, msg)
	}
	time.Sleep(2 * time.Second) // wait for consumer

	t.Logf("consumer group: %v", group0)
	for _, v := range rMetric.GetCurrentMetric() {
		t.Logf("consumer metric key: %v, count: %v", v.Key, v.RequestCount)
	}
	if nReceived != nMsgs {
		t.Errorf("received expected: %v, real: %v", nMsgs, nReceived)
	}

	//t.Logf("%#v", producer.MetricSuccess.GetCurrentMetric())
	nSent := 0
	for _, v := range producer.MetricSuccess.GetCurrentMetric() {
		t.Logf("producer metric key: %v, count: %v", v.Key, v.RequestCount)
		if strings.Contains(v.Key, "success") {
			nSent += v.RequestCount
		}
	}
	if nSent != nMsgs {
		t.Errorf("sent expected: %v, real: %v", nMsgs, nSent)
	}

	mu.Lock()
	t.Logf("sent: %v, received: %v, durInit: %v, durRead: %v",
		nSent, nReceived, csmT1.Sub(csmT0), csmT2.Sub(csmT1))
	mu.Unlock()
}

// TODO: automatically check result of TestConsumer_Rebalance
func TestConsumer_Rebalance(t *testing.T) {
	group := "TestGroup" + gofast.UUIDGenNoHyphen()
	t.Logf("init consumer0")
	consumer0, err := NewConsumer(ConsumerConfig{
		BootstrapServers: brokersTest,
		GroupId:          group,
		Offset:           OffsetEarliest,
		Topics:           "TestTopic0,TestTopic1",
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			msgs, err := consumer0.Consume()
			if err != nil {
				t.Logf("error Consume: %v\n", err)
				time.Sleep(1 * time.Second)
				continue
			}
			for _, msg := range msgs {
				t.Logf("consumed from %v:%v:%v message: %v\n",
					msg.Topic, msg.Partition, msg.Offset, msg.Value)
			}
		}
	}()
	time.Sleep(1 * time.Second)
	t.Logf("init consumer1")
	consumer1, err := NewConsumer(ConsumerConfig{
		BootstrapServers: brokersTest,
		GroupId:          group,
		Offset:           OffsetEarliest,
		Topics:           "TestTopic0,TestTopic1",
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			msgs, err := consumer1.Consume()
			if err != nil {
				t.Logf("error when consumer ReadMessage: %v\n", err)
				time.Sleep(1 * time.Second)
				continue
			}
			for _, msg := range msgs {
				t.Logf("consumed from %v%v%v message: %v",
					msg.Topic, msg.Partition, msg.Offset, msg.Value)
			}
		}
	}()
	t.Logf("let's see group balance")
	time.Sleep(2 * time.Second)
	// each consumer should claim a fair share number of partitions
}

// test on broker with message.max.bytes=1000000
func TestProducer_Compress(t *testing.T) {
	producerComp, err := NewProducer(ProducerConfig{
		BrokersList:   brokersTest,
		RequiredAcks:  WaitForLocal,
		LogMaxLineLen: 120,
		IsCompressed:  true,
		MaxMsgBytes:   10 * 1048576, // client side limit, not important
	})
	if err != nil {
		t.Fatal(err)
	}
	producerNoComp, err := NewProducer(ProducerConfig{
		BrokersList:   brokersTest,
		RequiredAcks:  WaitForLocal,
		LogMaxLineLen: 120,
		MaxMsgBytes:   10 * 1048576, // client side limit, not important
	})
	if err != nil {
		t.Fatal(err)
	}

	msg2MB := genMessage(2000000)
	msg5MB := genMessage(5000000)
	topi2 := fmt.Sprintf("topic_%v", gofast.UUIDGenNoHyphen()[:8])
	t.Logf("producerCompress__________________________________________ ")
	producerComp.Produce(topi2, msg2MB) // expect succeed
	producerComp.Produce(topi2, msg5MB) // expect fail
	producerComp.Close()
	ns, ne := producerComp.getNumberOfSuccessError()
	if ns != 1 || ne != 1 {
		t.Logf("error producerComp nSuccesses: %v, nErrors: %v", ns, ne)
	}
	t.Logf("producerNoCompress_________________________________________")
	producerNoComp.Produce(topi2, msg2MB) // expect fail
	producerNoComp.Produce(topi2, msg5MB) // expect fail
	producerNoComp.Close()
	ns, ne = producerNoComp.getNumberOfSuccessError()
	if ns != 0 || ne != 2 {
		t.Logf("error producerComp nSuccesses: %v, nErrors: %v", ns, ne)
	}
}

// genMessage gen a message that has compressed ratio about 2.7
// (strings.Repeat has compressed ratio about 500)
func genMessage(size int) string {
	bld := strings.Builder{}
	for i := 0; true; i++ {
		bld.WriteString(fmt.Sprintf("%v", i))
		if bld.Len() >= size {
			break
		}
	}
	return bld.String()
}

func TestProducer_ProduceJSON(t *testing.T) {
	producer, err := NewProducer(ProducerConfig{
		BrokersList: brokersTest, RequiredAcks: WaitForLocal})
	if err != nil {
		t.Fatal(err)
	}
	type MsgType1 struct {
		Field0 string
		Field1 []byte
	}
	type MsgType2 struct {
		Field0 bool
		Field1 func(a ...interface{}) (n int, err error)
	}
	for _, msg := range []interface{}{
		"msg string",
		MsgType1{Field0: "I miss", Field1: []byte("no one")},
		MsgType2{Field1: fmt.Println},
	} {
		producer.ProduceJSON("topic2", msg)
	}
	producer.Close()
	nSuccesses, _ := producer.getNumberOfSuccessError()
	if nSuccesses != 2 {
		t.Errorf("nSuccesses got: %v, but want %v", nSuccesses, 2)
	}
}
