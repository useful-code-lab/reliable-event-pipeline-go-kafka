package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mykafka "kafka-pipeline-go/internal/kafka"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Основная конфигурация для чтения и отправки обычных событий
	cfg := mykafka.Config{
		Brokers: []string{"kafka-service:9092"},
		Topic:   "local-events",
		GroupID: "local-processor-group",
		UseTLS:  false,
	}

	// Отдельная конфигурация для топика-отстойника (DLQ)
	dlqCfg := mykafka.Config{
		Brokers: cfg.Brokers,
		Topic:   "local-events-dlq", // Имя топика для проблемных сообщений
		UseTLS:  cfg.UseTLS,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Инициализируем компоненты
	producer := mykafka.NewProducer(cfg)
	dlqProducer := mykafka.NewProducer(dlqCfg) // Продюсер для отправки ошибок
	consumer := mykafka.NewConsumer(cfg)

	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Ошибка закрытия продюсера: %v", err)
		}
		if err := dlqProducer.Close(); err != nil {
			log.Printf("Ошибка закрытия DLQ-продюсера: %v", err)
		}
		if err := consumer.Close(); err != nil {
			log.Printf("Ошибка закрытия консьюмера: %v", err)
		}
		log.Println("Соединения с Kafka успешно закрыты.")
	}()

	// Счетчик для имитации периодической ошибки
	var messageCounter int

	// Наша чистая бизнес-логика
	eventHandler := func(ctx context.Context, msg kafka.Message) error {
		messageCounter++

		// Имитируем бизнес-ошибку: каждое 3-е сообщение будет признано "бракованным"
		if messageCounter%3 == 0 {
			return errors.New("ошибка валидации данных: поврежденный формат JSON")
		}

		log.Printf("Успешно обработано воркером: Ключ=%s, Значение=%s", string(msg.Key), string(msg.Value))
		return nil
	}

	// Запускаем консьюмер, применяя Middleware обертку WithDLQ к нашему eventHandler
	log.Println("Запуск пула консьюмеров с поддержкой DLQ...")
	go consumer.Start(ctx, 3, WithDLQ(dlqProducer, eventHandler))

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

	<-ctx.Done()
	log.Println("Завершение работы приложения...")
	time.Sleep(1 * time.Second)
}

// WithDLQ — функция-декоратор (Middleware). Она перехватывает ошибки вашей бизнес-логики.
func WithDLQ(dlqProducer *mykafka.Producer, next mykafka.MessageHandler) mykafka.MessageHandler {
	return func(ctx context.Context, msg kafka.Message) error {
		// Передаем сообщение в реальную бизнес-логику
		err := next(ctx, msg)
		if err == nil {
			return nil // Если всё обработалось без ошибок, просто выходим
		}

		// Если бизнес-логика вернула ошибку, изолируем сообщение
		log.Printf("[DLQ Middleware] Перехват ошибки: %v. Эвакуация сообщения в отстойник...", err)

		dlqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Пересылаем исходное сообщение в топик ошибок
		dlqErr := dlqProducer.SendMessage(dlqCtx, msg.Key, msg.Value)
		if dlqErr != nil {
			// Если упала сама Кафка и отправить в DLQ не удалось, возвращаем ошибку наружу.
			// Консьюмер НЕ закомитит это сообщение, чтобы мы не потеряли данные.
			log.Printf("[DLQ Middleware] КРИТИЧЕСКАЯ ОШИБКА: не удалось записать в DLQ топик: %v", dlqErr)
			return err
		}

		log.Println("[DLQ Middleware] Сообщение успешно изолировано. Конвейер продолжает работу.")
		return nil // Возвращаем nil, чтобы консьюмер сделал Commit и перешел к следующему сообщению
	}
}
