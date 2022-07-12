package ephemeral

import (
	"context"
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

type session struct {
	claims              map[string][]int32
	initialOffset       int64
	claimInitialOffsets map[string]map[int32]int64
	errorCh             chan<- *sarama.ConsumerError
}

func newSession(rawClaims map[string][]int32, initialOffset int64, errorsCh chan<- *sarama.ConsumerError) (*session, error) {
	claimInitialOffsets := make(map[string]map[int32]int64)

	for t, ps := range rawClaims {
		claimInitialOffsets[t] = make(map[int32]int64)
		for _, p := range ps {
			claimInitialOffsets[t][p] = initialOffset
		}
	}

	return &session{
		claims:              rawClaims,
		initialOffset:       initialOffset,
		claimInitialOffsets: claimInitialOffsets,
		errorCh:             errorsCh,
	}, nil
}

func (s *session) Claims() map[string][]int32 {
	return s.claims
}

func (s *session) MemberID() string {
	return uuid.NewString()
}

func (s *session) GenerationID() int32 {
	return 0
}

func (s *session) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	s.resetOffset(topic, partition, offset)
}

func (s *session) Commit() {
	// do nothing
}

func (s *session) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	s.resetOffset(topic, partition, offset)
}

func (s *session) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	// do nothing
}

func (s *session) Context() context.Context {
	return context.Background()
}

func (s *session) resetOffset(topic string, partition int32, offset int64) {
	partitionsOffsets, ok := s.claimInitialOffsets[topic]
	if !ok {
		return
	}

	_, ok = partitionsOffsets[partition]
	if !ok {
		return
	}

	partitionsOffsets[partition] = offset
}

func (s *session) consume(closeCh chan struct{}, consumer sarama.Consumer, handler func(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error) error {
	claims := make([]*claim, 0)
	pcs := make([]sarama.PartitionConsumer, 0)

	for t, ps := range s.claims {
		for _, p := range ps {
			initialOffset := s.claimInitialOffsets[t][p]
			pc, err := consumer.ConsumePartition(t, p, initialOffset)
			if errors.Is(err, sarama.ErrOffsetOutOfRange) {
				initialOffset = s.initialOffset
				pc, err = consumer.ConsumePartition(t, p, initialOffset)
			}

			if err != nil {
				return err
			}

			claims = append(claims, newClaim(pc, t, p, initialOffset))
			pcs = append(pcs, pc)
		}
	}

	wg := sync.WaitGroup{}

	for _, claim := range claims {
		wg.Add(1)
		go func(c sarama.ConsumerGroupClaim) {
			defer wg.Done()

			_ = handler(s, c)
		}(claim)
	}

	for _, pc := range pcs {
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()

			for err := range pc.Errors() {
				s.errorCh <- err
			}
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()

			select {
			case <-closeCh:
				_ = pc.Close()
			}
		}(pc)
	}

	wg.Wait()

	return nil
}
