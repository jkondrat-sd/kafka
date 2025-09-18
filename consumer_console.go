package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9094"}
	topic := "test-topic-3"

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Не удалось создать consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("Не удалось получить список партиций: %v", err)
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("Не удалось подписаться на партицию %d: %v", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Partition=%d Offset=%d | %s\n",
					msg.Partition, msg.Offset, string(msg.Value))
			}
		}(pc)
	}
	select {}
}
