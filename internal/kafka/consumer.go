package kafka

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageHandler — функция-обработчик, которую мы будем передавать из бизнес-логики
type MessageHandler func(ctx context.Context, msg kafka.Message) error

type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer создает новый объект для чтения сообщений
func NewConsumer(cfg Config) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.Brokers,
			GroupID:        cfg.GroupID,
			Topic:          cfg.Topic,
			Dialer:         NewDialer(cfg),
			MinBytes:       10e3, // 10KB
			MaxBytes:       10e6, // 10MB
			CommitInterval: 0,    // Отключаем авто-коммит для ручного подтверждения
			StartOffset:    kafka.LastOffset,
		}),
	}
}

// Start запускает указанное количество воркеров для параллельной обработки
func (c *Consumer) Start(ctx context.Context, workersCount int, handler MessageHandler) {
	var wg sync.WaitGroup
	msgChan := make(chan kafka.Message, workersCount)

	// 1. Запуск пула воркеров
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for msg := range msgChan {
				// Ограничиваем время обработки одного сообщения
				processCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				err := handler(processCtx, msg)
				cancel()

				if err != nil {
					log.Printf("[Воркер %d] Ошибка обработки сообщения: %v", workerID, err)
					continue
				}

				// Коммитим (подтверждаем) сообщение только при успешной обработке
				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("[Воркер %d] Ошибка коммита: %v", workerID, err)
				}
			}
		}(i)
	}

	// 2. Поток чтения из Kafka и передача сообщений в канал воркеров
	go func() {
		defer close(msgChan)
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("Ошибка получения сообщения: %v", err)
				time.Sleep(time.Second) // Защита от спама при сетевых сбоях
				continue
			}
			msgChan <- msg
		}
	}()

	wg.Wait()
}

// Close закрывает соединение консьюмера
func (c *Consumer) Close() error {
	return c.reader.Close()
}
