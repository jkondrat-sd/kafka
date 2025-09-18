package main

import (
	"fmt"
	"log"
	"runtime"
	"sync"

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

	messageChan := make(chan *sarama.ConsumerMessage, 100)

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition("test-topic", partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("Не удалось подписаться на партицию %d: %v", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				messageChan <- msg
			}
		}(pc)
	}

	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 1; w <= numWorkers; w++ {
		go func(id int) {
			defer wg.Done()
			for msg := range messageChan {
				fmt.Printf("[Worker %d] Партиция=%d Offset=%d, Значение=%s\n",
					id, msg.Partition, msg.Offset, string(msg.Value))
			}
		}(w)
	}

	wg.Wait()
}
