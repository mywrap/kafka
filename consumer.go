package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mywrap/log"
)

// ConsumerConfig will be used to initialize a Kafka consumer
type ConsumerConfig struct {
	// BootstrapServers is a comma separated list of host port: "broker1:9092,broker2:9092,broker3:9092"
	BootstrapServers string
	// Topics is a comma separated list of topics to subscribe: "topic0,topic1,topic2"
	Topics string
	// A Kafka consumer group is a set of consumers which cooperate to consume data.
	// Consumers in a group can be run on different host (with the same groupId).
	// The partitions of all the topics are divided "fairly" among the consumers
	// in the group. If a new group member(s) arrive and an old member(s) leave,
	// the partitions will be re-assigned to consumers in the group.
	GroupId string
	// ConsumerInitialOffset will be used ONLY if consumer group does not have a valid offset
	// committed (value must be OffsetEarliest or OffsetLatest)
	Offset ConsumerInitialOffset
}

// Message represents a message consumed from Kafka by a Consumer
type Message struct {
	Value     string // message's content
	Offset    int64  // read Kafka glossary
	Topic     string // read Kafka glossary
	Partition int32  // read Kafka glossary
	Key       string // read Kafka glossary
	Timestamp time.Time
}

// Consumer represents a Kafka consumer
// (detail https://docs.confluent.io/platform/current/clients/consumer.html#ak-consumer)
type Consumer struct {
	client  sarama.ConsumerGroup // client manages connections to one or more Kafka brokers
	handler *handlerImpl         // can be nil while the Consumer is initing or consumer group is rebalancing
	mutex   *sync.Mutex          // protect the handler
}

// NewConsumer initializes an auto reconnect Consumer (but return an error if
// the first connect fail)
func NewConsumer(conf ConsumerConfig) (*Consumer, error) {
	c := &Consumer{mutex: &sync.Mutex{}}
	log.Printf("initializing a consumer with config %+v", conf)
	samConf := sarama.NewConfig()
	samConf.Consumer.Offsets.Initial = mapOffset(conf.Offset)
	kafkaVerS := "1.1.1"
	var err error
	samConf.Version, err = sarama.ParseKafkaVersion(kafkaVerS)
	if err != nil { // unreachable
		return nil, fmt.Errorf("ParseKafkaVersion %v: %v", kafkaVerS, err)
	}
	brokers := strings.Split(conf.BootstrapServers, ",")
	topics := strings.Split(conf.Topics, ",")
	samConf.Consumer.Return.Errors = true
	c.client, err = sarama.NewConsumerGroup(brokers, conf.GroupId, samConf)
	if err != nil {
		return nil, fmt.Errorf("init sarama client: %v", err)
	}
	go func() {
		for warn := range c.client.Errors() {
			log.Printf("error in consumer life cycle: %v", warn)
		}
	}()

	runSession := func() *handlerImpl {
		c.mutex.Lock()
		c.handler = nil // c.handler will be reassign when new handler Setup finished
		c.mutex.Unlock()

		handler := &handlerImpl{
			readyChan:              make(chan bool),
			sessionDone:            make(chan bool),
			sessionDoneErr:         nil,
			consumeRequestChannels: make(map[string](chan *consumeRequest)),
			mu:                     &sync.RWMutex{},
		}
		go func() {
			log.Printf("begin sarama Consume session")
			// this function only exits if a server-side group rebalance occur
			err := c.client.Consume(context.Background(), topics, handler)
			log.Printf("end sarama Consume session: %v", err)
			if err != nil { // context deadline: unreachable
				handler.sessionDoneErr = fmt.Errorf("session ended unexpectedly: %v", err)
				close(handler.sessionDone)
			} else {
				err = errors.New("server-side group is rebalancing")
				handler.sessionDoneErr = err
				close(handler.sessionDone) // brokers rebalance
			}
		}()
		return handler
	}

	// the first runSession, will not retry on error
	{
		handler := runSession()
		select {
		case <-handler.sessionDone:
			return nil, handler.sessionDoneErr
		case <-handler.readyChan:
			c.mutex.Lock()
			c.handler = handler
			c.mutex.Unlock()
		}
	}

	// from now, consumer will auto reconnect if needed
	go func() {
		for {
			if c.handler != nil {
				<-c.handler.sessionDone
			}
			time.Sleep(1 * time.Second) // retry back-off
			newHandler := runSession()
			select {
			case <-newHandler.sessionDone:
				continue
			case <-newHandler.readyChan:
				c.mutex.Lock()
				c.handler = newHandler
				c.mutex.Unlock()
			}
		}
	}()
	log.Printf("connected to kafka cluster %v", conf.BootstrapServers)
	return c, nil
}

// Consume blocks until it receives at least 1 message (exactly 1 message is
// preferred but sometimes this returns more because Kafka partitions can return
// many messages at the same time). This func auto commits offset for the
// returned message (but not immediately to Kafka servers, so if the app crash,
// you may end up processing the same message twice).
// If this func returns an error, caller should sleep for some seconds before
// calling Consume again (consumers group is rebalancing).
func (c Consumer) Consume() ([]Message, error) {
	c.mutex.Lock()
	currentHandler := c.handler
	c.mutex.Unlock()
	if currentHandler == nil {
		return nil, ErrConsumerNotInitialized
	}

	// send the request to all partitions ConsumeClaim loops
	var consumeRequestChannels [](chan *consumeRequest)
	currentHandler.mu.RLock()
	for _, v := range currentHandler.consumeRequestChannels {
		consumeRequestChannels = append(consumeRequestChannels, v)
	}
	currentHandler.mu.RUnlock()
	if len(consumeRequestChannels) == 0 {
		return nil, ErrConsumerClaimedZeroPartitions
	}

	ret := make([]Message, 0) // consumed Kafka message(s)
	mu := &sync.Mutex{}       // protect slice ret
	wg := &sync.WaitGroup{}
	// a shared context will be passed to all ConsumeClaim loops,
	// cancel this context means a partition received a message so others stop waiting
	ctxGotMsg, cclGotMsg := context.WithCancel(context.Background())
	for i, _ := range consumeRequestChannels {
		r := &consumeRequest{ctx: ctxGotMsg, responseChan: make(chan *Message)}
		consumeReqChan := consumeRequestChannels[i]
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			select {
			case <-currentHandler.sessionDone:
				// group rebalance, just return
			case consumeReqChan <- r:
				msg := <-r.responseChan // partition's ConsumeClaim have to reply
				if msg != nil {
					cclGotMsg()
					mu.Lock()
					ret = append(ret, *msg)
					mu.Unlock()
				}
			}
		}()
	}
	// TODO: handle leaderless partition
	wg.Wait()
	cclGotMsg() // in case of all ConsumeClaim loops got 0 messages

	if len(ret) == 0 {
		return nil, ErrGroupRebalance
	}
	return ret, nil
}

// handlerImpl implements sarama.ConsumerGroupHandler,
// when "Consumer.Consume" is called, this handler tries to consume and commit
// offset for 1 Kafka message from any of assigned partitions (sometimes
type handlerImpl struct {
	readyChan      chan bool // will be closed when func handlerImpl.Setup returned
	sessionDone    chan bool
	sessionDoneErr error // reason why channel sessionDone was closed
	// consumeRequestChannels will be set in func handlerImpl.Setup:
	// each entry in this map correspond to an assigned partition (aka sarama.ConsumerGroupClaim)
	consumeRequestChannels map[string](chan *consumeRequest)
	mu                     *sync.RWMutex // protect map consumeRequestChannels
}

// consumeRequest will be created by func Consumer.Consume, one consumeRequest for one partition
type consumeRequest struct {
	ctx          context.Context
	responseChan chan *Message
}

func (h *handlerImpl) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("joined consumer group, claimed partitions %+v", session.Claims())
	h.mu.Lock()
	for topic, a := range session.Claims() {
		for _, partition := range a {
			h.consumeRequestChannels[claimKey(topic, partition)] = make(chan *consumeRequest)
		}
	}
	h.mu.Unlock()
	close(h.readyChan)
	return nil
}

func claimKey(topic string, partition int32) string { return fmt.Sprintf("%v:%v", topic, partition) }

func (h *handlerImpl) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// each assigned partition will run this func in a goroutine
func (h *handlerImpl) ConsumeClaim(
	session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	partition := claimKey(claim.Topic(), claim.Partition())
	log.Printf("begin ConsumeClaim partition %v", partition)
	defer log.Printf("end ConsumeClaim partition %v", partition)

	h.mu.RLock()
	consumeReqChan, ok := h.consumeRequestChannels[partition]
	h.mu.RUnlock()
	if !ok { // unreachable
		err := fmt.Errorf("unexpected missing claimKey: %v", partition)
		log.Printf("error %v", err)
		return err
	}

	for {
		select {
		case r := <-consumeReqChan:
			// have to reply to r.responseChan in all cases
			select {
			case samMsg, opening := <-claim.Messages():
				if !opening { // group rebalance
					log.Debugf("end ConsumeClaim checkpoint 1")
					r.responseChan <- nil
					return nil
				}
				if samMsg == nil { // unreachable
					r.responseChan <- nil
					continue
				}

				// lib sarama Note:
				// calling MarkOffset does not necessarily commit the offset to the backend
				// store immediately for efficiency reasons, and it may never be committed if
				// your application crashes. This means that you may end up processing the same
				// message twice, and your processing should ideally be idempotent.
				// TODO: immediately commit offset to Kafka server if needed, now unit tests cannot commit offset
				session.MarkMessage(samMsg, "")
				msg := &Message{
					Value:     string(samMsg.Value),
					Offset:    samMsg.Offset,
					Topic:     samMsg.Topic,
					Partition: samMsg.Partition,
					Key:       string(samMsg.Key),
					Timestamp: samMsg.Timestamp}
				r.responseChan <- msg
			case <-r.ctx.Done(): //
				r.responseChan <- nil
			}
		case <-h.sessionDone: // same as <-session.Context().Done()
			log.Debugf("end ConsumeClaim checkpoint 2")
			return nil
		}
	}
}

// ConsumerInitialOffset is the initial offset to use if no offset was previously committed
type ConsumerInitialOffset string

func mapOffset(myOffset ConsumerInitialOffset) int64 {
	switch myOffset {
	case OffsetEarliest:
		return sarama.OffsetOldest
	case OffsetLatest:
		return sarama.OffsetNewest
	default:
		return sarama.OffsetNewest
	}
}

// Initial offset IF consumer group does not have a valid committed offset
const (
	OffsetEarliest = "earliest"
	OffsetLatest   = "latest"
)

var (
	ErrConsumerNotInitialized        = errors.New("session is starting, try again later")
	ErrGroupRebalance                = errors.New("consumers group is rebalancing")
	ErrConsumerClaimedZeroPartitions = errors.New("this consumer claimed 0 partitions (other consumers claimed all partitions)")
)
