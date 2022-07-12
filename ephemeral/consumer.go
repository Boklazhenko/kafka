package ephemeral

import (
	"github.com/Shopify/sarama"
)

type Consumer struct {
	config   *sarama.Config
	c        sarama.Consumer
	errorsCh chan *sarama.ConsumerError
	closeCh  chan struct{}
}

func NewConsumer(addrs []string, config *sarama.Config) (*Consumer, error) {
	c, err := sarama.NewConsumer(addrs, config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		config:   config,
		c:        c,
		errorsCh: make(chan *sarama.ConsumerError, 1000),
		closeCh:  make(chan struct{}),
	}, nil
}

func (c *Consumer) Consume(topics []string, handler sarama.ConsumerGroupHandler) error {
	defer close(c.errorsCh)

	claims := make(map[string][]int32)
	for _, t := range topics {
		ps, err := c.c.Partitions(t)
		if err != nil {
			return err
		}

		claims[t] = ps
	}

	sess, err := newSession(claims, c.config.Consumer.Offsets.Initial, c.errorsCh)
	if err != nil {
		return err
	}

	err = handler.Setup(sess)
	if err != nil {
		return err
	}

	err = sess.consume(c.closeCh, c.c, handler.ConsumeClaim)
	if err != nil {
		return err
	}

	err = handler.Cleanup(sess)
	if err != nil {
		return err
	}

	return c.c.Close()
}

func (c *Consumer) Errors() <-chan *sarama.ConsumerError {
	return c.errorsCh
}

func (c *Consumer) Close() {
	select {
	case <-c.closeCh:
	default:
		close(c.closeCh)
	}
}
