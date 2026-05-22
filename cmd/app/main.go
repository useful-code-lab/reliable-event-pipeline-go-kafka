package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mykafka "kafka-pipeline-go/internal/kafka"

	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := mykafka.Config{
		Brokers: []string{"kafka-service:9092"},
		Topic:   "local-events",
		GroupID: "local-processor-group",
		UseTLS:  false,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	producer := mykafka.NewProducer(cfg)
	consumer := mykafka.NewConsumer(cfg)

	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Ошибка закрытия продюсера: %v", err)
		}
		if err := consumer.Close(); err != nil {
			log.Printf("Ошибка закрытия консьюмера: %v", err)
		}
		log.Println("Соединения с Kafka успешно закрыты.")
	}()

	eventHandler := func(ctx context.Context, msg kafka.Message) error {
		log.Printf("Получено сообщение: Ключ=%s, Значение=%s", string(msg.Key), string(msg.Value))
		return nil
	}

	log.Println("Запуск пула консьюмеров...")
	go consumer.Start(ctx, 3, eventHandler)

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := producer.SendMessage(ctx, []byte("user_1"), []byte("click_event"))
				if err != nil {
					log.Printf("Не удалось отправить сообщение: %v", err)
				} else {
					log.Println("Сообщение успешно отправлено в топик.")
				}
			}
		}
	}()

	<-ctx.Done()
	log.Println("Завершение работы приложения...")
	time.Sleep(1 * time.Second)
}
