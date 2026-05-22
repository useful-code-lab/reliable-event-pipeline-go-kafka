package kafka

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type MessageHandler func(ctx context.Context, msg kafka.Message) error

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(cfg Config) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.Brokers,
			GroupID:        cfg.GroupID,
			Topic:          cfg.Topic,
			Dialer:         NewDialer(cfg),
			MinBytes:       1,                 // ВАЖНО: 1 байт, чтобы читать сообщения мгновенно
			MaxBytes:       10e6,              // 10MB лимит на пакет
			CommitInterval: 0,                 // Ручной коммит
			StartOffset:    kafka.FirstOffset, // Читаем топик с самого начала
		}),
	}
}


func (c *Consumer) Start(ctx context.Context, workersCount int, handler MessageHandler) {
	var wg sync.WaitGroup
	msgChan := make(chan kafka.Message, workersCount)

	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for msg := range msgChan {
				processCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				err := handler(processCtx, msg)
				cancel()

				if err != nil {
					log.Printf("[Воркер %d] Ошибка обработки сообщения: %v", workerID, err)
					continue
				}

				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("[Воркер %d] Ошибка коммита: %v", workerID, err)
				}
			}
		}(i)
	}

	go func() {
		defer close(msgChan)
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("Ошибка получения сообщения: %v", err)
				time.Sleep(time.Second)
				continue
			}
			msgChan <- msg
		}
	}()

	wg.Wait()
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
