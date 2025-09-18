package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type AutoCommitConsumerGroupHandler struct{}

func (AutoCommitConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (AutoCommitConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (AutoCommitConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Partition=%d Offset=%d | Key=%s | Value=%s\n",
			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	brokers := []string{"localhost:9094"}
	topic := "test-topic-keys"
	groupID := "group-auto-commit"

	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Не удалось создать consumer group: %v", err)
	}
	defer func() {
		if err := consumerGroup.Close(); err != nil {
			log.Printf("Ошибка при закрытии consumer group: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := AutoCommitConsumerGroupHandler{}

	for {
		if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
			log.Fatalf("Ошибка во время Consume: %v", err)
		}
	}
}
