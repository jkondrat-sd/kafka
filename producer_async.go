package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	brokers := []string{"localhost:9094"}

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Не удалось создать async producer: %v", err)
	}
	defer producer.Close()

	go func() {
		for {
			select {
			case success := <-producer.Successes():
				log.Printf("Отправлено: партиция=%d, offset=%d", success.Partition, success.Offset)
			case err := <-producer.Errors():
				log.Printf("Ошибка при отправке: %v", err)
			}
		}
	}()

	for i := 1; i <= 10; i++ {
		msg := &sarama.ProducerMessage{
			Topic: "test-topic",
			Value: sarama.StringEncoder(fmt.Sprintf("Асинхронное сообщение #%d", i)),
		}
		producer.Input() <- msg
		log.Printf("Запланировано сообщение #%d", i)
	}

	select {}
}
