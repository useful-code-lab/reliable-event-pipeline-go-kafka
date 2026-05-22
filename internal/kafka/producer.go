package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

// NewProducer создает новый объект для отправки сообщений
func NewProducer(cfg Config) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			WriteTimeout: 10 * time.Second,
			MaxAttempts:  5,
		},
	}
}

// SendMessage отправляет ключ и значение в топик
func (p *Producer) SendMessage(ctx context.Context, key, value []byte) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	})
	if err != nil {
		return fmt.Errorf("ошибка отправки в kafka: %w", err)
	}
	return nil
}

// Close закрывает соединение продюсера
func (p *Producer) Close() error {
	return p.writer.Close()
}
