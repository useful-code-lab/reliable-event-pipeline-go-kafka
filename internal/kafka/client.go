package kafka

import (
	"crypto/tls"
	"time"

	"github.com/segmentio/kafka-go"
)

// Config содержит базовые параметры для подключения
type Config struct {
	Brokers []string
	Topic   string
	GroupID string
	UseTLS  bool
}

// NewDialer создает объект для настройки сетевого соединения
func NewDialer(cfg Config) *kafka.Dialer {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	if cfg.UseTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true, // Для локальной разработки без сертификатов
		}
	}

	return dialer
}
