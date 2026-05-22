package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	// Регистрация драйвера pgx для database/sql
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

var (
	ErrFatalValidation  = errors.New("unrecoverable validation error (e.g. bad JSON)")
	ErrTransientNetwork = errors.New("transient infrastructure error (e.g. DB timeout)")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := []string{getEnv("KAFKA_BROKERS", "kafka-service:9092")}

	mainTopic := getEnv("KAFKA_TOPIC", "local-events")
	retryTopic := mainTopic + "-retry"
	dlqTopic := mainTopic + "-dlq"
	groupID := getEnv("KAFKA_GROUP_ID", "local-processor-group")

	// --- ПАТТЕРН 1: Управление топологией ---
	log.Println("[Kafka] Инициализация топиков...")
	// Передаем brokers[0] (тип string) вместо brokers (тип []string)
	if err := ensureKafkaTopics(brokers[0], mainTopic, retryTopic, dlqTopic); err != nil {
		log.Fatalf("Ошибка создания топиков Kafka: %v", err)
	}

	// Инициализация PostgreSQL
	var dbPool *sql.DB
	var dbErr error
	for i := 1; i <= 5; i++ {
		dbPool, dbErr = sql.Open("pgx", getEnv("DATABASE_URL", "postgres://user:password@postgres-service:5432/app_db?sslmode=disable"))
		if dbErr == nil && dbPool.PingContext(ctx) == nil {
			log.Println("Успешно подключено к PostgreSQL.")
			break
		}
		if dbPool != nil {
			dbPool.Close()
		}
		time.Sleep(2 * time.Second)
	}
	if dbErr != nil {
		log.Fatalf("Postgres недоступен: %v", dbErr)
	}
	defer dbPool.Close()
	_ = initDatabaseSchema(dbPool)

	// Инициализация Redis
	rdb := redis.NewClient(&redis.Options{Addr: getEnv("REDIS_URL", "redis-service:6379")})
	defer rdb.Close()

	// --- Исправленная конфигурация Продюсеров с автоматическими ретраями ---
	retryProducer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  5, // Количество автоматических попыток при ошибках метаданных (Unknown Topic)
	}

	dlqProducer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  5,
	}

	// Метод Close() у них вызывается точно так же, код defer менять не нужно!
	defer retryProducer.Close()
	defer dlqProducer.Close()

	// --- НАСТРОЙКА КОНСЬЮМЕРОВ ---
	// Главный читатель (Основной поток)
	mainReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, GroupID: groupID, Topic: mainTopic, MinBytes: 10e3, MaxBytes: 10e6,
	})
	defer mainReader.Close()

	// Читатель ретраев (Фоновый поток повторных попыток)
	retryReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, GroupID: groupID + "-retry", Topic: retryTopic, MinBytes: 10e3, MaxBytes: 10e6,
	})
	defer retryReader.Close()

	var messageCounter int
	var counterMu sync.Mutex

	// Запуск фонового процесса Outbox Worker (Паттерн 6)
	// Передаем контекст, пул БД и продюсер для отправки в Kafka
	go startOutboxWorker(ctx, dbPool, retryProducer)

	// Функция бизнес-обработки
	executeBusinessLogic := func(ctx context.Context, msg kafka.Message) error {
		messageID := string(msg.Key)
		redisKey := "processed_msg:" + messageID

		// Идемпотентность через Redis
		isNew, err := rdb.SetNX(ctx, redisKey, true, 24*time.Hour).Result()
		if err != nil {
			log.Printf("[Redis] Ошибка сети: %v. Полагаемся на UNIQUE в БД.", err)
		} else if !isNew {
			log.Printf("[Идемпотентность] Сообщение %s уже обрабатывалось. Пропуск.", messageID)
			return nil
		}

		// Имитация случайных ошибок для тестов роутинга
		counterMu.Lock()
		messageCounter++
		currentCount := messageCounter
		counterMu.Unlock()

		if currentCount%5 == 0 {
			_ = rdb.Del(ctx, redisKey).Err()
			return fmt.Errorf("%w: bad JSON structure", ErrFatalValidation)
		}
		if currentCount%7 == 0 {
			_ = rdb.Del(ctx, redisKey).Err()
			return fmt.Errorf("%w: database connection timeout", ErrTransientNetwork)
		}

		// Запись в базу данных
		query := `INSERT INTO events (msg_key, msg_value, processed_at) VALUES ($1, $2, $3) ON CONFLICT (msg_key) DO NOTHING RETURNING id;`
		var insertedID int64
		err = dbPool.QueryRowContext(ctx, query, messageID, string(msg.Value), time.Now()).Scan(&insertedID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			_ = rdb.Del(ctx, redisKey).Err()
			return fmt.Errorf("%w: db fail: %v", ErrTransientNetwork, err)
		}

		log.Printf("[Бизнес-Логика] Успешно обработано сообщение Key=%s", messageID)
		return nil
	}

	// Универсальный обработчик сообщений с роутером ошибок (Паттерны 3 и 4)
	processMessageRouter := func(workerTag string, reader *kafka.Reader, msg kafka.Message) {
		procErr := executeBusinessLogic(ctx, msg)

		if procErr != nil {
			log.Printf("[%s] Ошибка сообщения %s: %v", workerTag, string(msg.Key), procErr)

			targetTopic := retryTopic
			attempts := getRetryAttempts(msg) + 1

			if errors.Is(procErr, ErrFatalValidation) {
				targetTopic = dlqTopic
				log.Printf("[Outbox] Фатальная ошибка. Регистрация %s в Outbox -> DLQ", string(msg.Key))
			} else if attempts > 3 {
				targetTopic = dlqTopic
				log.Printf("[Outbox] Исчерпан лимит ретраев. Регистрация %s в Outbox -> DLQ", string(msg.Key))
			} else {
				log.Printf("[Outbox] Временная ошибка. Регистрация %s в Outbox -> Retry (Попытка %d)", string(msg.Key), attempts)
				msg.Headers = append(msg.Headers, kafka.Header{Key: "x-retry-attempts", Value: []byte(strconv.Itoa(attempts))})
			}

			// ПАТТЕРН OUTBOX: Вместо прямой отправки в сеть, пишем задачу в БД
			// Это гарантирует, что если приложение упадет прямо сейчас, данные не потеряются
			err := writeToOutbox(dbPool, targetTopic, msg)
			if err != nil {
				log.Printf("[Критический сбой Outbox] Не удалось записать ошибку в БД: %v", err)
			}
		}

		// Паттерн 4: Ручной коммит офсета только после успешного сохранения ИЛИ успешного ретрая/DLQ
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[Критическая ошибка] Не удалось закоммитить офсет: %v", err)
		}
	}

	// --- ЗАПУСК ПОТОКОВ ОБРАБОТКИ ---

	// Воркеры для Основного Топика
	for w := 1; w <= 2; w++ {
		go func(id int) {
			tag := fmt.Sprintf("Основной-Воркер-%d", id)
			for {
				msg, err := mainReader.FetchMessage(ctx)
				if err != nil {
					return
				}
				processMessageRouter(tag, mainReader, msg)
			}
		}(w)
	}

	// --- ИСПРАВЛЕННЫЙ ПАТТЕРН 2: Воркеры для Топика Повторов (Retry) ---
	// Они читают сообщения из local-events-retry и пытаются обработать их снова [1]
	for w := 1; w <= 2; w++ {
		go func(id int) {
			tag := fmt.Sprintf("Retry-Воркер-%d", id)
			for {
				msg, err := retryReader.FetchMessage(ctx)
				if err != nil {
					return
				}
				processMessageRouter(tag, retryReader, msg)
			}
		}(w)
	}

	log.Println("[Пайплайн] Все консьюмеры (Основной + Retry) успешно запущены.")

		// --- СИМУЛЯЦИЯ ЖИВОГО ТРАФИКА (Генератор кликов) ---
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				uniqueKey := time.Now().Format("150405") // ID в формате ЧЧММСС
				
				// Отправляем тестовое событие клика в основной топик
				err := dlqProducer.WriteMessages(ctx, kafka.Message{
					Topic: mainTopic,
					Key:   []byte(uniqueKey),
					Value: []byte("user_click_action"),
				})
				if err != nil {
					log.Printf("[Симулятор] Ошибка отправки события: %v", err)
				}
			}
		}
	}()

	// Эта строка у вас уже есть, она должна быть самой последней в main():
	<-ctx.Done()


	<-ctx.Done()
	log.Println("Завершение работы приложения...")
}

// Вспомогательные функции остаются без изменений
func ensureKafkaTopics(brokerAddr string, topics ...string) error {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer cConn.Close()
	var tConfigs []kafka.TopicConfig
	for _, t := range topics {
		p := 3
		if t != topics[0] {
			p = 1
		}
		tConfigs = append(tConfigs, kafka.TopicConfig{Topic: t, NumPartitions: p, ReplicationFactor: 1})
	}
	_ = cConn.CreateTopics(tConfigs...)
	return nil
}

func sendToKafkaTopic(ctx context.Context, writer *kafka.Writer, topic string, msg kafka.Message) error {
	return writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: msg.Key, Value: msg.Value, Headers: msg.Headers})
}

func getRetryAttempts(msg kafka.Message) int {
	for _, h := range msg.Headers {
		if h.Key == "x-retry-attempts" {
			v, _ := strconv.Atoi(string(h.Value))
			return v
		}
	}
	return 0
}

func initDatabaseSchema(db *sql.DB) error {
	// 1. Создание основной таблицы бизнес-событий
	_, _ = db.Exec(`
	CREATE TABLE IF NOT EXISTS events (
		id BIGSERIAL PRIMARY KEY,
		msg_key VARCHAR(50) NOT NULL UNIQUE,
		msg_value TEXT NOT NULL,
		processed_at TIMESTAMP NOT NULL
	);`)

	// 2. Создание сервисной таблицы для паттерна Transactional Outbox
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS kafka_outbox (
		id BIGSERIAL PRIMARY KEY,
		target_topic VARCHAR(100) NOT NULL,
		msg_key TEXT NOT NULL,
		msg_value TEXT NOT NULL
	);`)
	return err
}

// Функция записи сообщения в таблицу Outbox
func writeToOutbox(db *sql.DB, topic string, msg kafka.Message) error {
	_, err := db.Exec(`
		INSERT INTO kafka_outbox (target_topic, msg_key, msg_value) 
		VALUES ($1, $2, $3)`,
		topic, string(msg.Key), string(msg.Value),
	)
	return err
}

// Фоновый воркер, который раз в секунду переносит сообщения из БД в Kafka
func startOutboxWorker(ctx context.Context, db *sql.DB, producer *kafka.Writer) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Читаем пачку из 10 старых неотправленных сообщений
			rows, err := db.QueryContext(ctx, "SELECT id, target_topic, msg_key, msg_value FROM kafka_outbox ORDER BY id ASC LIMIT 10")
			if err != nil {
				continue
			}

			type outboxItem struct {
				id              int64
				topic, key, val string
			}
			var items []outboxItem

			for rows.Next() {
				var item outboxItem
				if err := rows.Scan(&item.id, &item.topic, &item.key, &item.val); err == nil {
					items = append(items, item)
				}
			}
			rows.Close()

			// Отправляем сообщения в Kafka и удаляем их из БД только при успешной отправке
			for _, item := range items {
				err := producer.WriteMessages(ctx, kafka.Message{
					Topic: item.topic,
					Key:   []byte(item.key),
					Value: []byte(item.val),
				})
				if err == nil {
					_, _ = db.ExecContext(ctx, "DELETE FROM kafka_outbox WHERE id = $1", item.id)
					log.Printf("[Outbox-Worker] Успешно перенесено в Kafka: Ключ=%s -> Топик=%s", item.key, item.topic)
				} else {
					log.Printf("[Outbox-Worker] Ошибка отправки в Kafka для ID %d: %v", item.id, err)
				}
			}
		}
	}
}

func getEnv(key, fallback string) string {
	if v, exists := os.LookupEnv(key); exists {
		return v
	}
	return fallback
}
