package balancestrategy

import (
	"github.com/IBM/sarama"
)

type TopicToMemberBalanceStrategy struct {
}

func (s *TopicToMemberBalanceStrategy) Name() string {
	return "topic_to_member"
}

func (s *TopicToMemberBalanceStrategy) Plan(members map[string]sarama.ConsumerGroupMemberMetadata, topics map[string][]int32) (sarama.BalanceStrategyPlan, error) {
	plan := sarama.BalanceStrategyPlan{}

	membersIDs := make([]string, 0)
	for memberID, _ := range members {
		membersIDs = append(membersIDs, memberID)
	}

	if len(membersIDs) == 0 {
		return plan, nil
	}

	i := 0

	for topic, partitions := range topics {
		memberID := membersIDs[i]
		i = (i + 1) % len(membersIDs)
		for _, partition := range partitions {
			plan.Add(memberID, topic, partition)
		}
	}

	return plan, nil
}

func (s *TopicToMemberBalanceStrategy) AssignmentData(memberID string, topics map[string][]int32, generationID int32) ([]byte, error) {
	return nil, nil
}
