# WAL Writer (Go) container image

FROM golang:1.25 AS builder

WORKDIR /app

COPY wal_writer_go/go.mod wal_writer_go/go.sum ./
RUN go mod download

COPY wal_writer_go/main.go ./main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/wal-writer-go ./main.go

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /out/wal-writer-go /app/wal-writer-go

RUN mkdir -p /var/log/wal-writer

EXPOSE 9090

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

ENTRYPOINT ["/app/wal-writer-go"]
