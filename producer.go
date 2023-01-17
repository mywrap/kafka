package kafka

import (
	"compress/gzip"
	"context"
	"encoding/json"
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
	// level of acknowledgement reliability, default NoResponse
	RequiredAcks ProducerReliabilityLevel

	// the following configs are optional

	IsCompressed bool // if true, producer will use gzip level BestCompression
	// message size before compress in bytes, default 1000000,
	// should be set smaller than server config "message.max.bytes" after compressed
	MaxMsgBytes   int
	DisableLog    bool // default enable log on produced and delivered a message
	LogMaxLineLen int  // default no limit (can log a very large message)
}

// Producer _
type Producer struct {
	conf           ProducerConfig
	MetricSuccess  metric.Metric
	MetricError    metric.Metric
	samProducer    sarama.AsyncProducer
	ctxSuccessDone <-chan struct{} // handle closing sarama.AsyncProducer
	ctxErrorDone   <-chan struct{} // handle closing sarama.AsyncProducer
}

// NewProducer returns a connected Producer
func NewProducer(conf ProducerConfig) (*Producer, error) {
	log.Infof("creating a producer with %#v", conf)
	// construct sarama config
	samConf := sarama.NewConfig()
	kafkaVersion, _ := sarama.ParseKafkaVersion("1.1.1")
	samConf.Version = kafkaVersion
	samConf.Producer.RequiredAcks = mapReliabilityLevel(conf.RequiredAcks)
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
	if conf.IsCompressed {
		samConf.Producer.Compression = sarama.CompressionGZIP
		samConf.Producer.CompressionLevel = gzip.BestCompression
	}
	if conf.MaxMsgBytes > 0 {
		samConf.Producer.MaxMessageBytes = conf.MaxMsgBytes
	}

	// connect to kafka
	metricS := metric.NewMemoryMetric()
	gofast.NewCron(metricS.Reset, 24*time.Hour, 17*time.Hour)
	metricF := metric.NewMemoryMetric()
	gofast.NewCron(metricF.Reset, 24*time.Hour, 17*time.Hour)

	p := &Producer{conf: conf, MetricSuccess: metricS, MetricError: metricF}
	brokers := strings.Split(conf.BrokersList, ",")
	var err error
	p.samProducer, err = sarama.NewAsyncProducer(brokers, samConf)
	if err != nil {
		return nil, fmt.Errorf("error create producer: %v", err)
	}
	log.Infof("connected to kafka cluster %v", conf.BrokersList)

	ctxErrorsClosed, cclErrorsClosed := context.WithCancel(context.Background())
	p.ctxErrorDone = ctxErrorsClosed.Done()
	go func() {
		for err := range p.samProducer.Errors() {
			errMsg := err.Err.Error()
			metricKey := fmt.Sprintf("%v:%v_error", err.Msg.Topic, err.Msg.Partition)
			p.MetricError.Count(metricKey)
			p.MetricError.Duration(metricKey, since(err.Msg.Metadata))
			log.Infof("failed to produce msgId %v to topic %v: %v", err.Msg.Metadata, err.Msg.Topic, errMsg)
		}
		cclErrorsClosed()
	}()
	ctxSuccessesClosed, cclSuccessesClosed := context.WithCancel(context.Background())
	p.ctxSuccessDone = ctxSuccessesClosed.Done()
	go func() {
		for sent := range p.samProducer.Successes() {
			metricKey := fmt.Sprintf("%v:%v_success", sent.Topic, sent.Partition)
			p.MetricSuccess.Count(metricKey)
			p.MetricSuccess.Duration(metricKey, since(sent.Metadata))
			log.Condf(!p.conf.DisableLog, "delivered msgId %v to topic %v:%v:%v",
				sent.Metadata, sent.Topic, sent.Partition, sent.Offset)
		}
		cclSuccessesClosed()
	}()
	return p, nil
}

// ProduceJSON do JSON the object then sends JSONed string to Kafka servers,
// in most cases you only need this func
func (p Producer) ProduceJSON(topic string, object interface{}) {
	p.ProduceJSONWithKey(topic, object, "")
}

// ProduceJSON do JSON the object then sends JSONed string to Kafka clusters,
// messages have the same key will be sent to the same partition
func (p Producer) ProduceJSONWithKey(
	topic string, object interface{}, kafkaKey string) {
	switch v := object.(type) {
	case string:
		p.ProduceWithKey(topic, v, kafkaKey)
	case []byte:
		p.ProduceWithKey(topic, string(v), kafkaKey)
	default:
		beauty, err := json.Marshal(v)
		if err != nil {
			log.Printf("error ProduceJSON: %v, obj: %+v", err, v)
			return
		}
		p.ProduceWithKey(topic, string(beauty), kafkaKey)
	}
}

// Produce sends input msg to Kafka servers.
func (p Producer) Produce(topic string, msg string) {
	p.ProduceWithKey(topic, msg, "")
}

// ProduceWithKey guarantees that all messages with the same non-empty key will
// be sent to the same partition.
func (p Producer) ProduceWithKey(topic string, value string, key string) {
	msgMeta := msgMetadata{UniqueId: gofast.UUIDGenNoHyphen(), SentAt: time.Now()}
	samMsg := &sarama.ProducerMessage{
		Value:     sarama.StringEncoder(value),
		Topic:     topic,
		Metadata:  msgMeta,
		Timestamp: time.Now(),
	}
	if key != "" {
		samMsg.Key = sarama.StringEncoder(key)
	}
	select {
	case p.samProducer.Input() <- samMsg:
		if p.conf.LogMaxLineLen > 0 {
			log.Condf(!p.conf.DisableLog,
				"producing msgId %v to %v:%v: len %v, msg: %v",
				msgMeta.UniqueId, samMsg.Topic, key,
				len(value), truncateLog(value, p.conf.LogMaxLineLen))
		} else {
			log.Condf(!p.conf.DisableLog,
				"producing msgId %v to %v:%v: msg: %v",
				msgMeta.UniqueId, samMsg.Topic, key, value)
		}
	case <-time.After(3 * time.Minute):
		log.Printf("error: timed out send message to the Producer input channel")
	}
}

// Close shuts down the producer and waits for any buffered messages to be flushed
func (p Producer) Close() {
	p.samProducer.AsyncClose()
	<-p.ctxSuccessDone
	<-p.ctxErrorDone
}

func truncateLog(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit]
}

// return [nSuccesses, nErrors]
func (p Producer) getNumberOfSuccessError() (int, int) {
	nSuccesses := 0
	ss := p.MetricSuccess.GetCurrentMetric()
	for _, row := range ss {
		nSuccesses += row.RequestCount
	}
	nFails := 0
	fs := p.MetricError.GetCurrentMetric()
	for _, row := range fs {
		nFails += row.RequestCount
	}
	return nSuccesses, nFails
}

// ProducerReliabilityLevel is the level of acknowledgement reliability.
// * NoResponse: highest throughput.
// * WaitForLocal: high but not maximum durability and high but not maximum throughput.
// * WaitForAll: no data loss.
type ProducerReliabilityLevel string

func mapReliabilityLevel(level ProducerReliabilityLevel) sarama.RequiredAcks {
	switch level {
	case NoResponse:
		return sarama.NoResponse
	case WaitForLocal:
		return sarama.WaitForLocal
	case WaitForAll:
		return sarama.WaitForAll
	default:
		return sarama.WaitForLocal
	}
}

// ProducerReliabilityLevel enum
const (
	NoResponse   ProducerReliabilityLevel = "NoResponse"   // highest throughput
	WaitForLocal ProducerReliabilityLevel = "WaitForLocal" // high but not maximum durability and high but not maximum throughput
	WaitForAll   ProducerReliabilityLevel = "WaitForAll"   // no data loss
)

// to monitor duration for producing a message
type msgMetadata struct {
	UniqueId string
	SentAt   time.Time
}

func since(msgMetaI interface{}) time.Duration {
	msgMeta, ok := msgMetaI.(msgMetadata)
	if !ok { // unreachable
		return 0
	}
	return time.Since(msgMeta.SentAt)
}
