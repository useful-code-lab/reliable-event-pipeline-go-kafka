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
	// Конфигурация для локального запуска с Docker
	cfg := mykafka.Config{
		// Используем внутреннее DNS-имя сервиса Kubernetes
		Brokers: []string{"kafka-service:9092"},
		Topic:   "local-events",
		GroupID: "local-processor-group",
		UseTLS:  false,
	}

	// Отслеживаем сигналы закрытия программы (Ctrl+C или остановка контейнера)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	producer := mykafka.NewProducer(cfg)
	consumer := mykafka.NewConsumer(cfg)

	// Гарантируем закрытие соединений перед полным выходом из программы
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Ошибка закрытия продюсера: %v", err)
		}
		if err := consumer.Close(); err != nil {
			log.Printf("Ошибка закрытия консьюмера: %v", err)
		}
		log.Println("Соединения с Kafka успешно закрыты.")
	}()

	// Функция-обработчик входящих сообщений
	eventHandler := func(ctx context.Context, msg kafka.Message) error {
		log.Printf("Получено сообщение: Ключ=%s, Значение=%s", string(msg.Key), string(msg.Value))
		return nil
	}

	// Запускаем консьюмер в фоне (3 параллельных воркера)
	log.Println("Запуск пула консьюмеров...")
	go consumer.Start(ctx, 3, eventHandler)

	// Тестовая отправка сообщения каждые 3 секунды
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

	// Блокируем поток, пока приложение не закроют
	<-ctx.Done()
	log.Println("Завершение работы приложения...")
	time.Sleep(1 * time.Second) // Время воркерам завершить текущие задачи
}
