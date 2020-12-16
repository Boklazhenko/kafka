package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
	"time"
)

const chanBuffSize = 1000

type Producer struct {
	input  chan *kafka.Message
	logs   chan kafka.LogEvent
	events chan kafka.Event
	config *kafka.ConfigMap
}

func NewProducer(config *kafka.ConfigMap) *Producer {
	p := &Producer{
		input:  make(chan *kafka.Message, chanBuffSize),
		logs:   make(chan kafka.LogEvent, chanBuffSize),
		events: make(chan kafka.Event, chanBuffSize),
		config: config,
	}

	return p
}

func (p *Producer) Run(ctx context.Context) {
	defer close(p.logs)
	defer close(p.events)

	ready := make(chan struct{})
	var whyNotReady error
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	var producer *kafka.Producer

	producer, whyNotReady = kafka.NewProducer(p.config)

	if whyNotReady == nil {
		close(ready)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for ; whyNotReady != nil; {
			mu.Lock()
			producer, whyNotReady = kafka.NewProducer(p.config)
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
			for l := range producer.Logs() {
				p.logs <- l
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for evt := range producer.Events() {
				p.events <- evt
			}
		}()

		select {
		case <-ctx.Done():
			producer.Close()
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
					producer.ProduceChannel() <- msg
				default:
					mu.Lock()
					msg.TopicPartition.Error = whyNotReady
					mu.Unlock()
					p.events <- msg
				}
			case <-ctx.Done():
				close(p.input)
				for msg := range p.input {
					msg.TopicPartition.Error = fmt.Errorf("producer stopped")
					p.events <- msg
				}
				return
			}
		}
	}()

	wg.Wait()
}

func (p *Producer) Input() chan<- *kafka.Message {
	return p.input
}

func (p *Producer) Logs() <-chan kafka.LogEvent {
	return p.logs
}

func (p *Producer) Errors() <-chan kafka.Event {
	return p.events
}

type Consumer struct {
	messages chan *kafka.Message
	errors   chan error
	config   *kafka.ConfigMap
	topics   []string
}

func NewConsumer(config *kafka.ConfigMap, topics []string) *Consumer {
	return &Consumer{
		messages: make(chan *kafka.Message, chanBuffSize),
		errors:   make(chan error, chanBuffSize),
		config:   config,
		topics:   topics,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	defer close(c.errors)
	defer close(c.messages)

	var consumer *kafka.Consumer
	var err error
	for consumer, err = kafka.NewConsumer(c.config); err != nil; consumer, err = kafka.NewConsumer(c.config) {
		c.errors <- err
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}

	for err = consumer.SubscribeTopics(c.topics, nil); err != nil; err = consumer.SubscribeTopics(c.topics, nil) {
		c.errors <- err
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			if err = consumer.Close(); err != nil {
				c.errors <- err
			}
		default:
			evt := consumer.Poll(100)

			if evt == nil {
				continue
			}

			switch e := evt.(type) {
			case *kafka.Message:
				c.messages <- e
			case kafka.Error:
				c.errors <- e
			}
		}
	}
}

func (c *Consumer) Messages() <-chan *kafka.Message {
	return c.messages
}

func (c *Consumer) Errors() <-chan error {
	return c.errors
}