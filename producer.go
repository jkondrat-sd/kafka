package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	brokers := []string{"localhost:9094"}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Не удалось создать producer: %v", err)
	}
	defer producer.Close()

	for i := 1; i <= 10; i++ {
		msg := &sarama.ProducerMessage{
			Topic: "test-topic",
			Value: sarama.StringEncoder(fmt.Sprintf("Сообщение #%d", i)),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Ошибка при отправке сообщения: %v", err)
		} else {
			log.Printf("Сообщение #%d отправлено в партицию %d с offset %d", i, partition, offset)
		}
	}
}
