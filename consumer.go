package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := []string{"localhost:9094"}

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Не удалось создать consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions("test-topic")
	if err != nil {
		log.Fatalf("Не удалось получить список партиций: %v", err)
	}

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition("test-topic", partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("Не удалось подписаться на партицию %d: %v", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				fmt.Printf("Получено сообщение: Партиция=%d Offset=%d, Значение=%s\n",
					msg.Partition, msg.Offset, string(msg.Value))
			}
		}(pc)
	}

	select {}
}
