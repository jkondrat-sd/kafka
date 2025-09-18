package main

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type AckConsumerGroupHandler struct{}

func (AckConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (AckConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (AckConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
	groupID := "group-with-ack"

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

	handler := AckConsumerGroupHandler{}

	for {
		if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
			log.Fatalf("Ошибка во время Consume: %v", err)
		}
	}
}
