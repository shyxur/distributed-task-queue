FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /producer ./cmd/producer

FROM alpine:3.20
RUN adduser -D -u 1000 appuser
COPY --from=builder /producer /producer
USER appuser
EXPOSE 8080
ENTRYPOINT ["/producer"]