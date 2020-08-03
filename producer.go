package kafka

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mywrap/gofast"
	"github.com/mywrap/log"
	"github.com/mywrap/metric"
)

// ProducerConfig _
type ProducerConfig struct {
	// BrokersList is comma separated: "broker1:9092,broker2:9092,broker3:9092"
	BrokersList string
	// RequiredAcks is the level of acknowledgement reliability,
	// recommend value: WaitForLocal
	RequiredAcks SendMsgReliabilityLevel
}

// Producer _
type Producer struct {
	samProducer sarama.AsyncProducer
	Metric      metric.Metric
	IsLog       bool
}

// NewProducer returns a connected Producer
func NewProducer(conf ProducerConfig) (*Producer, error) {
	log.Infof("creating a producer with %#v", conf)
	// construct sarama config
	samConf := sarama.NewConfig()
	samConf.Producer.RequiredAcks = sarama.RequiredAcks(conf.RequiredAcks)
	samConf.Producer.Retry.Max = 5
	samConf.Producer.Retry.BackoffFunc = func(retries, maxRetries int) time.Duration {
		ret := 100 * time.Millisecond
		for retries > 0 {
			ret = 2 * ret
			retries--
		}
		return ret
	}
	samConf.Producer.Return.Successes = true

	// connect to kafka
	metric0 := metric.NewMemoryMetric()
	gofast.NewCron(metric0.Reset, 24*time.Hour, 17*time.Hour)
	p := &Producer{Metric: metric0, IsLog: true}
	brokers := strings.Split(conf.BrokersList, ",")
	var err error
	p.samProducer, err = sarama.NewAsyncProducer(brokers, samConf)
	if err != nil {
		return nil, fmt.Errorf("error create producer: %v", err)
	}
	log.Infof("connected to kafka cluster %v", conf.BrokersList)
	go func() {
		for err := range p.samProducer.Errors() {
			errMsg := err.Err.Error()
			if errMsg == "circuit breaker is open" {
				errMsg = "probably you did not assign topic"
			}
			metricKey := fmt.Sprintf("%v:%v_error",
				err.Msg.Topic, err.Msg.Partition)
			p.Metric.Count(metricKey)
			p.Metric.Duration(metricKey, since(err.Msg.Metadata))
			log.Infof("failed to write msgId %v to topic %v: %v",
				err.Msg.Metadata, err.Msg.Topic, errMsg)
		}
	}()
	go func() {
		for sent := range p.samProducer.Successes() {
			metricKey := fmt.Sprintf("%v:%v_success", sent.Topic, sent.Partition)
			p.Metric.Count(metricKey)
			p.Metric.Duration(metricKey, since(sent.Metadata))
			log.Condf(p.IsLog, "delivered msgId %v to topic %v:%v:%v",
				sent.Metadata, sent.Topic, sent.Partition, sent.Offset)
		}
	}()
	return p, nil
}

// SendExplicitMessage sends messages have a same key to same partition
func (p Producer) SendExplicitMessage(topic string, value string, key string) error {
	msgMeta := MsgMetadata{UniqueId: gofast.GenUUID(), SentAt: time.Now()}
	samMsg := &sarama.ProducerMessage{
		Value:    sarama.StringEncoder(value),
		Topic:    topic,
		Metadata: msgMeta,
	}
	if key != "" {
		samMsg.Key = sarama.StringEncoder(key)
	}
	var err error
	select {
	case p.samProducer.Input() <- samMsg:
		log.Condf(p.IsLog, "sending msgId %v to %v:%v: %v",
			msgMeta.UniqueId, samMsg.Topic, key, samMsg.Value)
		err = nil
	case <-time.After(30 * time.Second):
		err = ErrWriteTimeout
	}
	return err
}

// SendMessage sends message to a random partition of defaultTopic
func (p Producer) SendMessage(topic string, msg string) error {
	return p.SendExplicitMessage(topic, msg, "")
}

// Errors when produce
var (
	ErrWriteTimeout = errors.New("write message timeout")
)

// SendMsgReliabilityLevel is the level of acknowledgement reliability.
// * NoResponse: highest throughput,
// * WaitForLocal: high, but not maximum durability and high but not maximum throughput,
// * WaitForAll: no data loss,
type SendMsgReliabilityLevel sarama.RequiredAcks

// SendMsgReliabilityLevel enum
const (
	NoResponse   = SendMsgReliabilityLevel(sarama.NoResponse)
	WaitForLocal = SendMsgReliabilityLevel(sarama.WaitForLocal)
	WaitForAll   = SendMsgReliabilityLevel(sarama.WaitForAll)
)

type MsgMetadata struct {
	UniqueId string
	SentAt   time.Time
}

func since(msgMetaI interface{}) time.Duration {
	msgMeta, ok := msgMetaI.(MsgMetadata)
	if !ok {
		return 0
	}
	return time.Since(msgMeta.SentAt)
}
