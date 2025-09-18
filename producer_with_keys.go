package main

import (
	"log"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9094"}
	topic := "test-topic-keys"

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Ошибка создания producer: %v", err)
	}
	defer producer.Close()

	messages := []struct {
		key   string
		value string
	}{
		{"user1", "Сообщение от user1 - 1"},
		{"user2", "Сообщение от user2 - 1"},
		{"user1", "Сообщение от user1 - 2"},
		{"user3", "Сообщение от user3 - 1"},
		{"user2", "Сообщение от user2 - 2"},
	}

	for _, m := range messages {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(m.key),
			Value: sarama.StringEncoder(m.value),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Ошибка при отправке сообщения с ключом %s: %v", m.key, err)
		} else {
			log.Printf("Отправлено сообщение с ключом=%s | партиция=%d offset=%d",
				m.key, partition, offset)
		}
	}
}
