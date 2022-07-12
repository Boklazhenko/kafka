package eof

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type ConsumerGroupHandler struct {
	handler   sarama.ConsumerGroupHandler
	eofCh     chan struct{}
	eofClaims map[string]bool
	mu        sync.Mutex
}

func NewConsumerGroupHandler(handler sarama.ConsumerGroupHandler) *ConsumerGroupHandler {
	return &ConsumerGroupHandler{
		handler: handler,
		eofCh:   make(chan struct{}, 1),
	}
}

func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.eofClaims = make(map[string]bool)
	for topic, partitions := range session.Claims() {
		for _, partition := range partitions {
			h.eofClaims[fmt.Sprintf("%s%d", topic, partition)] = false
		}
	}
	return h.handler.Setup(session)
}

func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.eofCh = nil
	return h.handler.Cleanup(session)
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	<-time.After(5 * time.Second)

	c := newConsumerGroupClaim(claim, func() {
		h.mu.Lock()
		defer h.mu.Unlock()

		h.eofClaims[fmt.Sprintf("%s%d", claim.Topic(), claim.Partition())] = true

		for _, eofClaim := range h.eofClaims {
			if !eofClaim {
				return
			}
		}

		select {
		case h.eofCh <- struct{}{}:
		default:
		}

		for tp := range h.eofClaims {
			h.eofClaims[tp] = false
		}
	})
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()

		c.Run(ctx)
	}()

	err := h.handler.ConsumeClaim(session, c)

	cancel()

	wg.Wait()

	return err
}

func (h *ConsumerGroupHandler) EOF() <-chan struct{} {
	return h.eofCh
}

type consumerGroupClaim struct {
	claim    sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
	eofCb    func()
}

func newConsumerGroupClaim(claim sarama.ConsumerGroupClaim, eofCb func()) *consumerGroupClaim {
	return &consumerGroupClaim{
		claim:    claim,
		messages: make(chan *sarama.ConsumerMessage, 100),
		eofCb:    eofCb,
	}
}

func (c *consumerGroupClaim) Topic() string {
	return c.claim.Topic()
}

func (c *consumerGroupClaim) Partition() int32 {
	return c.claim.Partition()
}

func (c *consumerGroupClaim) InitialOffset() int64 {
	return c.claim.InitialOffset()
}

func (c *consumerGroupClaim) HighWaterMarkOffset() int64 {
	return c.claim.HighWaterMarkOffset()
}

func (c *consumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

func (c *consumerGroupClaim) Run(ctx context.Context) {
	defer close(c.messages)

	lctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	switch c.claim.InitialOffset() {
	case sarama.OffsetNewest:
		c.eofCb()
	case sarama.OffsetOldest:
		c.waitHighWaterMarkOffset(lctx)
		if c.claim.HighWaterMarkOffset() == 0 {
			c.eofCb()
		}
	default:
		c.waitHighWaterMarkOffset(lctx)
		if c.claim.InitialOffset() >= c.claim.HighWaterMarkOffset()-1 {
			c.eofCb()
		}
	}

	for {
		select {
		case msg, ok := <-c.claim.Messages():
			if !ok {
				return
			}

			c.messages <- msg

			if msg.Offset == c.claim.HighWaterMarkOffset()-1 {
				c.eofCb()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *consumerGroupClaim) waitHighWaterMarkOffset(ctx context.Context) {
	for {
		if c.HighWaterMarkOffset() != 0 {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}
