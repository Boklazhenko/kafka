package ephemeral

import (
	"github.com/Shopify/sarama"
)

type claim struct {
	topic         string
	partition     int32
	initialOffset int64
	pc            sarama.PartitionConsumer
}

func newClaim(pc sarama.PartitionConsumer, topic string, partition int32, initialOffset int64) *claim {
	return &claim{
		topic:         topic,
		partition:     partition,
		initialOffset: initialOffset,
		pc:            pc,
	}
}

func (c *claim) Topic() string {
	return c.topic
}

func (c *claim) Partition() int32 {
	return c.partition
}

func (c *claim) InitialOffset() int64 {
	return c.initialOffset
}

func (c *claim) HighWaterMarkOffset() int64 {
	return c.pc.HighWaterMarkOffset()
}

func (c *claim) Messages() <-chan *sarama.ConsumerMessage {
	return c.pc.Messages()
}
