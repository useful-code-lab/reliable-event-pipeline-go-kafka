# kafka-pipeline-go

Отказоустойчивый паттерн интеграции с Apache Kafka на языке Go, оптимизированный для работы в Kubernetes (Minikube).

## Архитектура
- **Конкурентная обработка**: Пул воркеров (Worker Pool) на горутинах для параллельного разбора сообщений.
- **Гарантия доставки**: Использование `At-Least-Once` семантики за счет ручной фиксации смещений (`Manual Commit`) только после успешной обработки сообщения.
- **Graceful Shutdown**: Безопасная остановка приложения с доработкой текущих сообщений при получении сигналов `SIGTERM`/`SIGINT`.

## Требования
- Go 1.25+
- Minikube
- kubectl

## Быстрый запуск в Minikube

1. **Запуск и настройка кластера**
   ```bash
   minikube start --cpus=3 --memory=4096
   ```

2. **Переключение контекста Docker на Minikube**
   (Обязательно для сборки образа локально внутри кластера)
   ```bash
   eval $(minikube docker-env)
   ```

3. **Сборка Docker-образа приложения**
   ```bash
   docker build -t kafka-pipeline-go:local .
   ```

4. **Развертывание инфраструктуры (Kafka + App)**
   ```bash
   kubectl apply -f k8s-kafka.yaml
   ```

5. **Проверка статуса подов**
   ```bash
   kubectl get pods -w
   ```

6. **Просмотр логов работы пайплайна**
   ```bash
   kubectl logs -f deployment/go-kafka-app
   ```

## Очистка ресурсов
Чтобы полностью удалить все развернутые поды и сервисы:
```bash
kubectl delete -f k8s-kafka.yaml
```
