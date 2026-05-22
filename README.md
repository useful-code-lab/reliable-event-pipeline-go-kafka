# kafka-pipeline-go

Отказоустойчивый пайплайн обработки событий на **Go + Apache Kafka + PostgreSQL + Redis**, предназначенный для работы в **Kubernetes (Minikube)**.

Проект демонстрирует реализацию паттернов распределённой обработки данных: **Exactly-Once Processing**, **Transactional Outbox**, **Retry Queue**, **Dead Letter Queue (DLQ)** и **Graceful Shutdown**.

---

## Возможности

* Обработка событий через Apache Kafka
* Идемпотентная запись в PostgreSQL
* Кэширование обработанных сообщений через Redis
* Повторная обработка временных ошибок (Retry Topic)
* Изоляция невалидных сообщений (DLQ)
* Автоматическое восстановление после отказов
* Работа внутри Kubernetes
* Безопасный контейнер (Distroless + non-root)

---

## Архитектура

```text
Producer
   │
   ▼
┌───────────────────┐
│ local-events      │
│ (Kafka Topic)     │
└────────┬──────────┘
         │
         ▼
┌─────────────────────────┐
│ Go Consumers (Workers)  │
└────────┬────────────────┘
         │
         │
 ┌───────┴─────────────┐
 │                     │
 ▼                     ▼
Успех              Ошибка
 │                     │
 ▼                     ▼
PostgreSQL       Transactional
(events)            Outbox
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
   Retry Topic              DLQ Topic
(local-events-retry)   (local-events-dlq)
```

---

## Используемые паттерны

### 1. Exactly-Once Processing

Реализован через комбинацию:

* **Redis SETNX** — быстрая проверка дубликатов;
* **UNIQUE(msg_key)** — окончательная гарантия уникальности в PostgreSQL.

Пример:

```sql
INSERT INTO events (...)
ON CONFLICT (msg_key)
DO NOTHING;
```

---

### 2. Transactional Outbox

Ошибочные сообщения сначала сохраняются в таблицу:

```text
kafka_outbox
```

После успешной отправки в Kafka запись удаляется.

Это устраняет потерю данных при аварийном завершении приложения.

---

### 3. Retry Queue

Для временных ошибок:

```text
local-events
    ↓
local-events-retry
```

Поддерживается до **3 попыток** повторной обработки.

---

### 4. Dead Letter Queue (DLQ)

Для неустранимых ошибок:

```text
local-events-dlq
```

Пример причины:

```text
bad JSON structure
```

---

### 5. Graceful Shutdown

При завершении:

* останавливается чтение Kafka;
* завершаются транзакции;
* коммитятся офсеты;
* контейнер завершается корректно.

---

# Структура проекта

```text
.
├── main.go
├── Dockerfile
├── k8s-kafka.yaml
├── README.md
└── docs/
```

---

# Требования

```text
Go 1.25+
Docker
Minikube
kubectl
```

---

# Запуск в Minikube

## 1. Запуск кластера

```bash
minikube start --cpus=3 --memory=4096
```

## 2. Подключение Docker

```bash
eval $(minikube docker-env)
```

## 3. Сборка образа

```bash
docker build -t kafka-pipeline-go:local .
```

## 4. Развёртывание инфраструктуры

```bash
kubectl apply -f k8s-kafka.yaml
```

---

## 5. Создание Kafka топиков

```bash
kubectl exec -it deployment/kafka-deployment \
-- kafka-topics.sh \
--create \
--topic local-events \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

DLQ:

```bash
kubectl exec -it deployment/kafka-deployment \
-- kafka-topics.sh \
--create \
--topic local-events-dlq \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1
```

---

## 6. Перезапуск приложения

```bash
kubectl rollout restart deployment/go-kafka-app
```

---

# Мониторинг

Статус подов:

```bash
kubectl get pods -w
```

Логи:

```bash
kubectl logs -f deployment/go-kafka-app --container=app
```

---

# Пример работы

```text
[Бизнес-Логика] Успешно обработано сообщение Key=093217

[Outbox]
Временная ошибка → Retry

[Outbox-Worker]
Перенесено → local-events-retry

[Retry-Воркер]
Повторная обработка успешна

[Outbox]
Фатальная ошибка → DLQ
```

---

# Очистка

Удаление инфраструктуры:

```bash
kubectl delete -f k8s-kafka.yaml
```

---

## Технические особенности реализации

| Компонент  | Назначение               |
| ---------- | ------------------------ |
| Kafka      | транспорт сообщений      |
| PostgreSQL | идемпотентное хранилище  |
| Redis      | дедупликация             |
| Outbox     | гарантированная доставка |
| Retry      | повторные попытки        |
| DLQ        | изоляция ошибок          |
| Kubernetes | оркестрация              |

