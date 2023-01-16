# Kafka client

This package is an easy-to-use Kafka client (written in pure Go).  

Wrapped [Shopify/sarama](https://github.com/Shopify/sarama).

## Usage

* Producer:

````go
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		BrokersList:  "127.0.0.1:9092",
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

	// Output:
	// creating a producer with kafka.ProducerConfig{BrokersList:"127.0.0.1:9092", RequiredAcks:"WaitForLocal", IsCompressed:false, MaxMsgBytes:0, DisableLog:false, LogMaxLineLen:0}
	// connected to kafka cluster 127.0.0.1:9092
	// producing msgId 195f249a90f677067babf4a30daeadb0 to TestTopic0:: msg: {"key0":"val0","key1":{"nestedKey":"nestedVal"}}
	// delivered msgId {195f249a90f677067babf4a30daeadb0 2023-01-16 22:03:55.62695678 +0700 +07 m=+0.100497028} to topic TestTopic0:0:52
````

* Consumer:

````go
    consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		BootstrapServers: "127.0.0.1:9092",
		GroupId:          "TestGroup0",
		Offset:           kafka.OffsetEarliest, // only meaningful if this GroupId has never committed an offset
		Topics:           "TestTopic0,TestTopic1",
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
			log.Printf("msg created at %v: %v", msg.Timestamp, msg.Value)
		}
	}
````

## References

* [Apache Kafka glossary](https://docs.confluent.io/kafka/introduction.html#topics).

* Origin [sarama](https://github.com/Shopify/sarama/blob/v1.36.0/examples/consumergroup/main.go) consumer example.

* [Broker configs](https://kafka.apache.org/documentation/#brokerconfigs_default.replication.factor). Some notable configs:
  
  * [min.insync.replicas](https://kafka.apache.org/documentation/#brokerconfigs_min.insync.replicas): when a producer sets acks to "all", min.insync.replicas specifies the minimum number of replicas that must acknowledge a write for the write to be considered successful
  * [message.max.bytes](https://kafka.apache.org/documentation/#brokerconfigs_message.max.bytes): max message size, default 1MiB (Producer client should know this) 
  * [num.partitions](https://kafka.apache.org/documentation/#brokerconfigs_num.partitions): the default number of log partitions per topic (Consumer client should know this) 
  * [default.replication.factor](https://kafka.apache.org/documentation/#brokerconfigs_default.replication.factor)

  Example [a production server config](https://kafka.apache.org/documentation/#prodconfig) 
