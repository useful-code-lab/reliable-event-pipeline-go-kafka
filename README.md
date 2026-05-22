# kafka-pipeline-go

Отказоустойчивый пайплайн интеграции с Apache Kafka, PostgreSQL и Redis на языке Go, оптимизированный для работы в Kubernetes (Minikube).

## Архитектура и паттерны
- **Гарантия доставки (Exactly-Once Semantics через БД)**: Комбинация паттерна *Идемпотентный потребитель* (с использованием уникального индекса `UNIQUE` в PostgreSQL) и распределенного кэша в Redis (паттерн *Проверка и установка* через `SETNX`).
- **Cloud-Native Probes**: Нативные проверки здоровья компонентов (`livenessProbe` и `readinessProbe`), заменяющие нестабильные скрипты инициализации.
- **Плавное завершение (Graceful Shutdown)**: На уровне Go и Kubernetes (`terminationGracePeriodSeconds`). При обновлении деплоя воркеры корректно дописывают транзакции и коммитят офсеты.
- **Изоляция ошибок (Dead Letter Queue)**: Паттерн *DLQ Middleware*. Сообщения, вызвавшие неустранимую ошибку бизнес-логики, автоматически эвакуируются в отдельный топик-отстойник.
- **Безопасный образ**: Сборка по паттерну *Distroless (Multi-Stage)*. Финальный контейнер не содержит командной оболочки (`sh`/`bash`) и запускается от имени не-привилегированного пользователя (`non-root`).

## Требования
- Go 1.25+
- Minikube
- kubectl

## Инструкция по запуску в Minikube

### 1. Запуск и настройка кластера
Выделите кластеру достаточно ресурсов для работы брокера Kafka и СУБД:
```bash
minikube start --cpus=3 --memory=4096
```

### 2. Переключение контекста Docker на Minikube
Обязательно выполните эту команду в терминале перед сборкой, чтобы образ сохранился внутри локального реестра Minikube:
```bash
eval $(minikube docker-env)
```

### 3. Сборка безопасного Docker-образа приложения
```bash
docker build -t kafka-pipeline-go:local .
```

### 4. Развертывание инфраструктуры и дисков
Примените манифест. Kubernetes автоматически создаст постоянные диски (`PersistentVolumeClaim`) для персистентного хранения данных Postgres и Kafka:
```bash
kubectl apply -f k8s-kafka.yaml
```

### 5. Инициализация топиков в Kafka (Критически важно!)
Поскольку в промышленной конфигурации автосоздание топиков отключено, их необходимо создать вручную до начала активной работы Go-приложения. 

Подождите 10-15 секунд, пока под `kafka-deployment` перейдет в статус `Running`, и выполните:
```bash
# Создание основного бизнес-топика с 3 партициями для параллельной обработки
kubectl exec -it deployment/kafka-deployment -- kafka-topics.sh --create --topic local-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Создание топика для аварийных сообщений (DLQ)
kubectl exec -it deployment/kafka-deployment -- kafka-topics.sh --create --topic local-events-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 6. Перезапуск Go-приложения
Если Go-приложение запустилось раньше, чем вы создали топики брокера, принудительно перезапустите его поды, чтобы они переподключились к чистой структуре:
```bash
kubectl rollout restart deployment/go-kafka-app
```

### 7. Мониторинг пайплайна
Проверьте, что все поды успешно перешли в статус `Running`:
```bash
kubectl get pods -w
```

Смотрите логи работы распределенного пула воркеров:
```bash
kubectl logs -f deployment/go-kafka-app --all-containers=true
```

## Очистка ресурсов
Чтобы полностью удалить все развернутые поды, сервисы и освободить постоянные диски:
```bash
kubectl delete -f k8s-kafka.yaml
```



## Пример выводимого сообщения 
```bash
2026/05/22 09:32:14 [Kafka] Инициализация топиков...
2026/05/22 09:32:14 Успешно подключено к PostgreSQL.
2026/05/22 09:32:14 [Пайплайн] Все консьюмеры (Основной + Retry) успешно запущены.
2026/05/22 09:32:31 [Бизнес-Логика] Успешно обработано сообщение Key=093217
2026/05/22 09:32:31 [Бизнес-Логика] Успешно обработано сообщение Key=093223
2026/05/22 09:32:31 [Бизнес-Логика] Успешно обработано сообщение Key=093226
2026/05/22 09:32:31 [Бизнес-Логика] Успешно обработано сообщение Key=093220
2026/05/22 09:32:31 [Основной-Воркер-2] Ошибка сообщения 093229: unrecoverable validation error (e.g. bad JSON): bad JSON structure
2026/05/22 09:32:31 [Outbox] Фатальная ошибка. Регистрация 093229 в Outbox -> DLQ
2026/05/22 09:32:33 [Outbox-Worker] Успешно перенесено в Kafka: Ключ=093229 -> Топик=local-events-dlq
2026/05/22 09:32:40 [Основной-Воркер-1] Ошибка сообщения 093232: transient infrastructure error (e.g. DB timeout): database connection timeout
2026/05/22 09:32:40 [Outbox] Временная ошибка. Регистрация 093232 в Outbox -> Retry (Попытка 1)
2026/05/22 09:32:40 [Бизнес-Логика] Успешно обработано сообщение Key=093235
2026/05/22 09:32:40 [Бизнес-Логика] Успешно обработано сообщение Key=093238
2026/05/22 09:32:42 [Outbox-Worker] Успешно перенесено в Kafka: Ключ=093232 -> Топик=local-events-retry
2026/05/22 09:32:45 [Бизнес-Логика] Успешно обработано сообщение Key=093232
2026/05/22 09:32:49 [Основной-Воркер-1] Ошибка сообщения 093241: unrecoverable validation error (e.g. bad JSON): bad JSON structure
2026/05/22 09:32:49 [Outbox] Фатальная ошибка. Регистрация 093241 в Outbox -> DLQ
2026/05/22 09:32:49 [Бизнес-Логика] Успешно обработано сообщение Key=093244
2026/05/22 09:32:49 [Бизнес-Логика] Успешно обработано сообщение Key=093247
2026/05/22 09:32:51 [Outbox-Worker] Успешно перенесено в Kafka: Ключ=093241 -> Топик=local-events-dlq
```
## Быстрый запуск
```bash
# 1. Переключаем терминал на Docker внутри Minikube
eval $(minikube docker-env)

# 2. Собираем свежий Docker-образ приложения
docker build -t kafka-pipeline-go:local .

# 3. Применяем манифест (поднимутся PVC, СУБД, Kafka, ConfigMap и Go-приложение)
kubectl apply -f k8s-kafka.yaml
```

## Логи 
```bash
kubectl logs -f deployment/go-kafka-app --container=app
```

## Перезапуск
```bash
eval $(minikube docker-env) && \
docker build -t kafka-pipeline-go:local . && \
kubectl rollout restart deployment/go-kafka-app && \
echo "Ожидаем применение обновления..." && \
sleep 15 && \
kubectl logs -f deployment/go-kafka-app --container=app
```
