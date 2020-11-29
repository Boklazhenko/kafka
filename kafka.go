package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

const chanBuffSize = 1000

func NewConfig() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_5_0_0

	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.MaxMessageBytes = 1000000
	conf.Producer.Compression = sarama.CompressionGZIP
	conf.Producer.Retry.Max = 10

	conf.Consumer.Offsets.Initial = sarama.OffsetNewest
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	conf.Consumer.Return.Errors = true
	return conf
}

type Producer struct {
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
	addrs     []string
	cfg       *sarama.Config
}

func NewProducer(addrs []string, conf *sarama.Config) *Producer {
	p := &Producer{
		input:     make(chan *sarama.ProducerMessage, chanBuffSize),
		successes: make(chan *sarama.ProducerMessage, chanBuffSize),
		errors:    make(chan *sarama.ProducerError, chanBuffSize),
		addrs:     addrs,
		cfg:       conf,
	}

	return p
}

func (p *Producer) Run(ctx context.Context) {
	defer close(p.successes)
	defer close(p.errors)

	ready := make(chan struct{})
	var whyNotReady error
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	var producer sarama.AsyncProducer

	producer, whyNotReady = sarama.NewAsyncProducer(p.addrs, p.cfg)

	if whyNotReady == nil {
		close(ready)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for ; whyNotReady != nil; {
			mu.Lock()
			producer, whyNotReady = sarama.NewAsyncProducer(p.addrs, p.cfg)
			mu.Unlock()
			if ctx.Err() != nil {
				return
			}
			time.Sleep(time.Second)
		}

		select {
		case <-ready:
		default:
			close(ready)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range producer.Successes() {
				p.successes <- s
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range producer.Errors() {
				p.errors <- e
			}
		}()

		select {
		case <-ctx.Done():
			producer.AsyncClose()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-p.input:
				select {
				case <-ready:
					producer.Input() <- msg
				default:
					mu.Lock()
					err := &sarama.ProducerError{
						Msg: msg,
						Err: whyNotReady,
					}
					mu.Unlock()
					p.errors <- err
				}
			case <-ctx.Done():
				for msg := range p.input {
					p.errors <- &sarama.ProducerError{
						Msg: msg,
						Err: fmt.Errorf("producer stopped"),
					}
				}
				return
			}
		}
	}()

	wg.Wait()
}

func (p *Producer) Input() chan<- *sarama.ProducerMessage {
	return p.input
}

func (p *Producer) Successes() <-chan *sarama.ProducerMessage {
	return p.successes
}

func (p *Producer) Errors() <-chan *sarama.ProducerError {
	return p.errors
}

type Consumer struct {
	messages chan *sarama.ConsumerMessage
	errors   chan error
	addrs    []string
	cfg      *sarama.Config
	groupId  string
	topics   []string
}

func NewConsumer(addrs []string, conf *sarama.Config, groupId string, topics []string) *Consumer {
	return &Consumer{
		messages: make(chan *sarama.ConsumerMessage),
		errors:   make(chan error, chanBuffSize),
		addrs:    addrs,
		cfg:      conf,
		groupId:  groupId,
		topics:   topics,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	defer close(c.errors)
	defer close(c.messages)

	var consumerGroup sarama.ConsumerGroup
	var err error
	for consumerGroup, err = sarama.NewConsumerGroup(c.addrs, c.groupId, c.cfg);
		err != nil;
	consumerGroup, err = sarama.NewConsumerGroup(c.addrs, c.groupId, c.cfg) {
		c.errors <- err
		if ctx.Err() != nil {
			return
		}
		time.Sleep(time.Second)
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range c.Errors() {
			c.errors <- e
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			if err := consumerGroup.Close(); err != nil {
				c.errors <- err
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, c.topics, &consumerHandler{messages: c.messages}); err != nil {
				c.errors <- err
				if ctx.Err() != nil {
					return
				}
			}
		}
	}()

	wg.Wait()
}

func (c *Consumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

func (c *Consumer) Errors() <-chan error {
	return c.errors
}

type consumerHandler struct {
	messages chan<- *sarama.ConsumerMessage
}

func (h *consumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		select {
		case h.messages <- msg:
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
	return nil
}
