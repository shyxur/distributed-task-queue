FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /worker ./cmd/worker

FROM alpine:3.20
RUN adduser -D -u 1000 appuser
COPY --from=builder /worker /worker
USER appuser
ENTRYPOINT ["/worker"]