# Data Pump Go - UPI Transaction Generator

High-throughput UPI transaction data generator for PostgreSQL, written in Go. Port of the Python data_pump project.

## Features

- Generates realistic UPI transaction data using faker
- Inserts into PostgreSQL using COPY for high throughput
- Configurable target transactions per second (default: 10,000)
- Concurrent batch inserts with worker pool
- Graceful shutdown on SIGINT

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `postgres` | PostgreSQL user |
| `DB_PASSWORD` | `postgres` | PostgreSQL password |
| `DB_NAME` | `postgres` | Database name |

## Build

```bash
go build -o data_pump_go .
```

## Run

```bash
./data_pump_go
```

Or with custom database connection:

```bash
DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=postgres ./data_pump_go
```

## Stop

Press `Ctrl+C` for graceful shutdown. The program waits for pending inserts to complete.

## Dependencies

- [pgx v5](https://github.com/jackc/pgx) - PostgreSQL driver
- [gofakeit v7](https://github.com/brianvoe/gofakeit) - Fake data generator
