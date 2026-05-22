# Слой сборки
FROM golang:1.25-alpine AS builder
WORKDIR /app
# Кэшируем зависимости
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Собираем статический бинарник без CGO
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /kafka-app cmd/app/main.go

# Финальный слой (Паттерн: Distroless / Secure Base)
FROM gcr.io/distroless/static-debian12:latest
WORKDIR /
COPY --from=builder /kafka-app /kafka-app
# Запуск от имени не-root пользователя (ID 65532 - nobody)
USER 65532:65532
ENTRYPOINT ["/kafka-app"]
