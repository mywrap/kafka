// Package kafka is an easy-to-use, pure go kafka client.
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

// ConsumerConfig should be created by NewConsumerConfig (for default values)
type ConsumerConfig struct {
	// comma separated list: broker1:9092,broker2:9092,broker3:9092
	BootstrapServers string
	// comma separated list topics to subscribe: topic0,topic1,topic2
	Topics string
	// GroupId is the Kafka's consumer group,
	// consumer processes with a same groupId get a "fair share" of Kafka's partitions
	GroupId string
	// Offset will be used ONLY if consumer group does not have a valid offset committed
	Offset Offset
}

// Message represents a message consumed from kafka
type Message struct {
	Value     string
	Offset    int64
	Topic     string
	Partition int32
	Key       string
	Timestamp time.Time
}

// Consumer must be inited by calling NewConsumer
type Consumer struct {
	// calling stopCxl stop loop of runSession calls in the constructor and
	// stop partition receiving message loops (func ConsumeClaim)
	stopCxl context.CancelFunc
	stopCtx context.Context // correspond to stopCxl
	// consumer's handler will only be changed func runSession,
	// it will be nil while a new session is initializing
	handler *handlerImpl
	mutex   *sync.Mutex // protect handler

	groupId string // just for debugging
	IsLog   bool   // whether to log when receiving a message
}

// NewConsumer init an auto reconnect Consumer (but return an error if the first
// connecting fail)
func NewConsumer(conf ConsumerConfig) (*Consumer, error) {
	log.Printf("creating a consumer with %#v", conf)
	csm := &Consumer{mutex: &sync.Mutex{}, groupId: conf.GroupId, IsLog: true}
	csm.stopCtx, csm.stopCxl = context.WithCancel(context.Background())
	samConf := sarama.NewConfig()
	samConf.Consumer.Offsets.Initial = int64(conf.Offset)
	kafkaVersion, _ := sarama.ParseKafkaVersion("1.1.1")
	samConf.Version = kafkaVersion
	brokers := strings.Split(conf.BootstrapServers, ",")
	topics := strings.Split(conf.Topics, ",")

	runSession := func() *handlerImpl {
		// close current handler
		csm.mutex.Lock()
		if csm.handler != nil && csm.handler.client != nil {
			csm.handler.client.Close()
		}
		csm.handler = nil
		csm.mutex.Unlock()

		// create new client, new handler, new session
		handler := &handlerImpl{
			readyChan:    make(chan bool),
			ssnEndedChan: make(chan bool),
			ssnEndedErr:  nil,
			readMsgChans: make(map[string](chan *partRequest)),
			consumer:     csm,
			client:       nil,
			mu:           &sync.RWMutex{},
		}
		client, err := sarama.NewConsumerGroup(brokers, conf.GroupId, samConf)
		if err != nil {
			close(handler.ssnEndedChan) // create client fail
			handler.ssnEndedErr = fmt.Errorf("create client: %v", err)
			return handler
		}
		handler.client = client
		log.Printf("consumer's sarama client created")
		go func() { // running session goroutine
			log.Printf("started consuming session")
			err := client.Consume(csm.stopCtx, topics, handler) // blocking
			if err != nil {
				handler.ssnEndedErr = fmt.Errorf("session ended: %v", err)
				close(handler.ssnEndedChan) // sarama Consume stop (or stopCxl)
			} else {
				err = errors.New("probably Kafka cluster is rebalancing")
				handler.ssnEndedErr = err
				close(handler.ssnEndedChan) // brokers rebalance
			}
			log.Printf("returned consuming session: %v", err)
		}()
		return handler
	}

	// the first runSession to the cluster, will not retry on error
	{ // limit var handler scope
		handler := runSession()
		select {
		case <-handler.ssnEndedChan:
			return nil, handler.ssnEndedErr
		case <-handler.readyChan:
			csm.mutex.Lock()
			csm.handler = handler
			csm.mutex.Unlock()
		}
	}
	// from now, consumer will auto reconnect if needed
	go func() {
		for csm.stopCtx.Err() == nil { // while consumer have not stopped
			if csm.handler != nil {
				<-csm.handler.ssnEndedChan
			}
			time.Sleep(1 * time.Second) // retry back-off
			newHandler := runSession()
			select {
			case <-newHandler.ssnEndedChan:
				continue
			case <-newHandler.readyChan:
				csm.mutex.Lock()
				csm.handler = newHandler
				csm.mutex.Unlock()
			}
		}
	}()
	log.Printf("connected to kafka cluster %v", conf.BootstrapServers)
	return csm, nil
}

// ReadMessage block until it receives at least one message or an error occurred
func (c Consumer) ReadMessage(ctx context.Context) ([]Message, error) {
	if c.stopCtx.Err() != nil {
		return nil, ErrConsumerStopped
	}
	c.mutex.Lock()
	currentHandler := c.handler
	c.mutex.Unlock()
	if currentHandler == nil {
		return nil, ErrNilHandler
	}

	// send the request to all partitions reader
	partitionChans := make([](chan *partRequest), 0)
	currentHandler.mu.RLock()
	for _, v := range currentHandler.readMsgChans {
		partitionChans = append(partitionChans, v)
	}
	currentHandler.mu.RUnlock()
	if len(partitionChans) == 0 { // should be unreachable
		return nil, errors.New("no ConsumeClaim is running")
	}

	ret := make([]Message, 0)
	mu := &sync.Mutex{} // to protect ret
	wg := &sync.WaitGroup{}
	// create a shared context that will be passed to all partitions, calling
	// firstMsgCxl means a partition received a message so others stop waiting
	firstMsgCtx, firstMsgCxl := context.WithCancel(ctx)
	for _, partitionChan := range partitionChans {
		r := &partRequest{ctx: firstMsgCtx, responseChan: make(chan *Message)}
		partitionChan := partitionChan // local var for goroutines
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			select {
			case <-ctx.Done(): // caller of ReadMessage cancels
			case <-currentHandler.ssnEndedChan: // receiver ConsumeClaim stopped
			case partitionChan <- r: // sent partRequest successfully
				msg := <-r.responseChan // partition's ConsumeClaim have to reply
				if msg != nil {
					firstMsgCxl()
					mu.Lock()
					ret = append(ret, *msg)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	firstMsgCxl()
	if len(ret) == 0 { // should be unreachable
		return nil, ErrReturnEmptyMsgs
	}
	return ret, nil
}

// Stop needs to release all resources of the consumer
func (c *Consumer) Stop() { c.stopCxl() }

type handlerImpl struct {
	// will be closed at returning of func consumerGroupHandler_Setup
	readyChan chan bool
	// will be closed when corresponding session goroutine returned
	ssnEndedChan chan bool
	ssnEndedErr  error // paired with ssnEndedChan
	// each entry in this map correspond to an assigned partition
	readMsgChans map[string](chan *partRequest)

	consumer *Consumer            // just for get log config
	client   sarama.ConsumerGroup // just for calling client_Close
	mu       *sync.RWMutex
}

func (h *handlerImpl) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("joined consumer group, assigned partitions %#v", s.Claims())
	h.mu.Lock()
	h.readMsgChans = make(map[string](chan *partRequest))
	for topic, parts := range s.Claims() {
		for _, part := range parts {
			h.readMsgChans[fmt.Sprintf("%v:%v", topic, part)] =
				make(chan *partRequest)
		}
	}
	h.mu.Unlock()
	close(h.readyChan)
	return nil
}

func (h *handlerImpl) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// each assigned partition will run this func in a goroutine
func (h *handlerImpl) ConsumeClaim(
	session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	partition := fmt.Sprintf("%v:%v", claim.Topic(), claim.Partition())
	log.Printf("started ConsumeClaim partition %v", partition)
	defer log.Printf("returned partition ConsumeClaim %v", partition)
	h.mu.RLock()
	readMsgChan, ok := h.readMsgChans[partition]
	h.mu.RUnlock()
	if !ok { // should be unreachable, key has to be inited in func Setup
		return nil
	}
	for {
		select {
		case partReq := <-readMsgChan:
			// have to reply to responseChan even if partReq's context cancelled
			select {
			case samMsg, opening := <-claim.Messages():
				if !opening { // cluster needs to be rebalanced
					partReq.responseChan <- nil
					return nil
				}
				if samMsg == nil { // unreachable
					partReq.responseChan <- nil
					continue
				}
				session.MarkMessage(samMsg, "") // commit offset
				msg := &Message{Value: string(samMsg.Value), Offset: samMsg.Offset,
					Topic: samMsg.Topic, Partition: samMsg.Partition,
					Key: string(samMsg.Key), Timestamp: samMsg.Timestamp}
				log.Condf(h.consumer.IsLog,
					"received a message from topic %v:%v:%v: %v",
					msg.Topic, msg.Partition, msg.Offset, msg.Value)
				partReq.responseChan <- msg // take care of blocking
			case <-partReq.ctx.Done(): //
				partReq.responseChan <- nil
			}
		case <-h.ssnEndedChan: // included consumer stopped
			return nil
		}
	}
}

// partRequest will be created by ReadMessage, one partRequest for one partition
type partRequest struct {
	ctx          context.Context
	responseChan chan *Message
}

// Offset configs the consumer
type Offset int64

// Initial offset if consumer group does not have a valid committed offset
const (
	OffsetEarliest = Offset(sarama.OffsetOldest)
	OffsetLatest   = Offset(sarama.OffsetNewest)
)

var (
	ErrConsumerStopped = errors.New("consumer stopped")
	ErrNilHandler      = errors.New("session is starting, try again")
	ErrReturnEmptyMsgs = errors.New("something went wrong, empty return")
)
