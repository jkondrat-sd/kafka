package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9094"}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Ошибка создания producer: %v", err)
	}
	defer producer.Close()

	topic := "test-topic-3"

	for partition := 0; partition < 3; partition++ {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: int32(partition),
			Value:     sarama.StringEncoder(fmt.Sprintf("Сообщение в партицию %d", partition)),
		}

		partitionID, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Ошибка при отправке в партицию %d: %v", partition, err)
		} else {
			log.Printf("Отправлено в партицию %d, offset=%d", partitionID, offset)
		}
	}
}
