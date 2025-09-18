package main

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type ConsumerGroupHandler struct{}

func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Group: %s | Партиция=%d Offset=%d | Сообщение=%s\n",
			session.MemberID(), msg.Partition, msg.Offset, string(msg.Value))

		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	brokers := []string{"localhost:9094"}
	topic := "test-topic"
	groupID := "my-group"

	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Не удалось создать consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := ConsumerGroupHandler{}

	for {
		if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
			log.Fatalf("Ошибка при чтении: %v", err)
		}
	}
}
