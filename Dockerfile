# Слой сборки
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /kafka-app cmd/app/main.go

# Финальный легковесный слой
FROM alpine:latest
WORKDIR /
COPY --from=builder /kafka-app /kafka-app
CMD ["/kafka-app"]
