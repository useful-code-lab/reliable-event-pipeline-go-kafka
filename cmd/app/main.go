package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mykafka "kafka-pipeline-go/internal/kafka"

	// Регистрация драйвера pgx для database/sql
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func main() {
	// 1. Инициализация конфигурации
	kafkaBrokers := getEnv("KAFKA_BROKERS", "kafka-service:9092")
	cfg := mykafka.Config{
		Brokers: []string{kafkaBrokers},
		Topic:   getEnv("KAFKA_TOPIC", "local-events"),
		GroupID: getEnv("KAFKA_GROUP_ID", "local-processor-group"),
		UseTLS:  false,
	}
	dlqCfg := mykafka.Config{
		Brokers: cfg.Brokers,
		Topic:   getEnv("KAFKA_DLQ_TOPIC", "local-events-dlq"),
		UseTLS:  cfg.UseTLS,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Отказоустойчивое подключение к PostgreSQL через pgx драйвер (до 5 попыток)
	var dbPool *sql.DB
	var err error
	for i := 1; i <= 5; i++ {
		log.Printf("Попытка подключения к PostgreSQL (%d/5)...", i)
		dbPool, err = sql.Open("pgx", getEnv("DATABASE_URL", "postgres://user:password@localhost:5432/app_db?sslmode=disable"))
		if err == nil {
			// Проверяем, готова ли база принимать запросы
			if err = dbPool.PingContext(ctx); err == nil {
				log.Println("Успешно подключено к PostgreSQL.")
				break
			}
		}
		if i == 5 {
			log.Fatalf("Не удалось подключиться к PostgreSQL после 5 попыток: %v", err)
		}
		time.Sleep(3 * time.Second) // Даем время СУБД на инициализацию
	}
	defer dbPool.Close()

	// Инициализация таблицы в БД
	if err := initDatabaseSchema(dbPool); err != nil {
		log.Fatalf("Ошибка инициализации БД: %v", err)
	}

	// 3. Отказоустойчивое подключение к Redis (до 5 попыток)
	var rdb *redis.Client
	for i := 1; i <= 5; i++ {
		log.Printf("Попытка подключения к Redis (%d/5)...", i)
		rdb = redis.NewClient(&redis.Options{
			Addr: getEnv("REDIS_URL", "localhost:6379"),
		})
		if err = rdb.Ping(ctx).Err(); err == nil {
			log.Println("Успешно подключено к Redis.")
			break
		}
		if i == 5 {
			log.Fatalf("Не удалось подключиться к Redis после 5 попыток: %v", err)
		}
		time.Sleep(2 * time.Second)
	}
	defer rdb.Close()

	// 4. Инициализация компонентов Kafka
	producer := mykafka.NewProducer(cfg)
	dlqProducer := mykafka.NewProducer(dlqCfg)
	consumer := mykafka.NewConsumer(cfg)

	defer func() {
		_ = producer.Close()
		_ = dlqProducer.Close()
		_ = consumer.Close()
		log.Println("Соединения с Kafka успешно закрыты.")
	}()

	var messageCounter int
	var counterMu sync.Mutex

	// 5. Описание бизнес-логики (Handler) с интеграцией Postgres и Redis
	eventHandler := func(ctx context.Context, msg kafka.Message) error {
		messageID := string(msg.Key)

		// --- ШАГ А: Дедупликация через Redis (Идемпотентность) ---
		isNew, err := rdb.SetNX(ctx, "processed_msg:"+messageID, true, 24*time.Hour).Result()
		if err != nil {
			log.Printf("[Redis] Ошибка проверки кэша: %v. Пропускаем дедупликацию ради надежности.", err)
		} else if !isNew {
			log.Printf("[Идемпотентность/Redis] Сообщение %s уже обрабатывалось ранее. Пропускаем дубликат.", messageID)
			return nil
		}

		// Имитация случайных ошибок бизнес-логики для демонстрации DLQ
		counterMu.Lock()
		messageCounter++
		isErrorIteration := messageCounter%3 == 0
		counterMu.Unlock()

		if isErrorIteration {
			_ = rdb.Del(ctx, "processed_msg:"+messageID).Err()
			return errors.New("ошибка валидации: поврежденный JSON")
		}

		// --- ШАГ Б: Запись бизнес-данных в PostgreSQL в транзакции ---
		tx, err := dbPool.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		_, err = tx.ExecContext(ctx, "INSERT INTO events (msg_key, msg_value, processed_at) VALUES ($1, $2, $3)", messageID, string(msg.Value), time.Now())
		if err != nil {
			_ = rdb.Del(ctx, "processed_msg:"+messageID).Err()
			return err
		}

		if err := tx.Commit(); err != nil {
			_ = rdb.Del(ctx, "processed_msg:"+messageID).Err()
			return err
		}

		log.Printf("[Бизнес-Логика] Данные успешно сохранены в Postgres. Ключ=%s", messageID)
		return nil
	}

	// Запуск пула консьюмеров с Middleware DLQ
	log.Println("Запуск пайплайна [Kafka + Postgres + Redis]...")
	go consumer.Start(ctx, 3, WithDLQ(dlqProducer, eventHandler))

	// Имитация генератора событий (Отправка уникальных ключей на базе таймстемпа)
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				uniqueKey := time.Now().Format("150405") // Генерация уникального ID (ЧЧММСС)
				err := producer.SendMessage(ctx, []byte(uniqueKey), []byte("user_click_action"))
				if err != nil {
					log.Printf("Ошибка отправки: %v", err)
				}
			}
		}
	}()

	<-ctx.Done()
	log.Println("Завершение работы приложения...")
	time.Sleep(1 * time.Second)
}

func initDatabaseSchema(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS events (
		id SERIAL PRIMARY KEY,
		msg_key VARCHAR(50) NOT NULL,
		msg_value TEXT NOT NULL,
		processed_at TIMESTAMP NOT NULL
	);`
	_, err := db.Exec(query)
	return err
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func WithDLQ(dlqProducer *mykafka.Producer, next mykafka.MessageHandler) mykafka.MessageHandler {
	return func(ctx context.Context, msg kafka.Message) error {
		err := next(ctx, msg)
		if err == nil {
			return nil
		}

		log.Printf("[DLQ Middleware] Эвакуация сообщения %s в отстойник из-за ошибки: %v", string(msg.Key), err)

		dlqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if dlqErr := dlqProducer.SendMessage(dlqCtx, msg.Key, msg.Value); dlqErr != nil {
			log.Printf("[DLQ] КРИТИЧЕСКАЯ ОШИБКА записи в DLQ: %v", dlqErr)
			return err
		}

		log.Println("[DLQ] Сообщение успешно изолировано.")
		return nil
	}
}
