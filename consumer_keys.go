package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9094"}
	topic := "test-topic-keys"

	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		log.Fatalf("не удалось создать consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("не удалось получить партиции топика %q: %v", topic, err)
	}

	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(topic, p, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("не удалось подписаться на партицию %d: %v", p, err)
		}

		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				var key string
				if msg.Key != nil {
					key = string(msg.Key)
				} else {
					key = "<nil>"
				}
				fmt.Printf("Partition=%d Offset=%d | Key=%s | Value=%s\n",
					msg.Partition, msg.Offset, key, string(msg.Value))
			}
		}(pc)
	}

	select {}
}
