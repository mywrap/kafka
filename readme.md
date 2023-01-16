# Kafka client

This package is an easy-to-use Kafka client (written in pure Go).  

Wrapped [Shopify/sarama](https://github.com/Shopify/sarama).

[Apache Kafka glossary](https://docs.confluent.io/kafka/introduction.html#topics).

## Usage

* Producer:

````go
brokers := "192.168.99.100:9092,192.168.99.101:9092,192.168.99.102:9092"

producer, err := kafka.NewProducer(kafka.ProducerConfig{
    BrokersList:  brokers,
    RequiredAcks: kafka.WaitForAll, // consistency vs performance
})
if err != nil {
    log.Fatal(err)
}
producer.Produce("topic1", "PING")
// log: delivered msgId d151f0ab} to topic topic1:0:2
````

* Consumer:

````go
consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
    BootstrapServers: brokers,
    GroupId:          "group0",
    // only meaningful if group0 has never committed an offset
    Offset:           kafka.OffsetEarliest, 
    Topics:           "topic0,topic1",
})
if err != nil {
    log.Fatal(err)
}

ctx, cxl := context.WithCancel(context.Background())
msg, err := consumer.Consume() // auto commit offset, but not guarantee
if err != nil {
    log.Printf("error when consumer Consume: %v\n", err)
}
process(msg)
// call cxl() to stop Consume at anytime
````
