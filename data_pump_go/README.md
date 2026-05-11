# Data Pump Go - UPI Transaction Generator

High-throughput UPI transaction data generator for PostgreSQL, written in Go. Port of the Python data_pump project.

## Features

- Generates realistic UPI transaction data using faker
- Inserts into PostgreSQL using COPY for high throughput
- Configurable target transactions per second via `DATA_PUMP_TARGET_PER_SEC`
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
| `DATA_PUMP_TARGET_PER_SEC` | `20000` | Target UPI transactions per second |
| `DATA_PUMP_BATCH_SIZE` | `1000` | Number of records per UPI insert batch |
| `DATA_PUMP_USERS_BATCH_SIZE` | `100` | Number of user records per batch |
| `DATA_PUMP_NUM_WORKERS` | `20` | Number of concurrent batch insert workers |

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

# Run at 5k inserts/sec
DATA_PUMP_TARGET_PER_SEC=5000 DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=postgres ./data_pump_go
```

## Stop

Press `Ctrl+C` for graceful shutdown. The program waits for pending inserts to complete.

## Dependencies

- [pgx v5](https://github.com/jackc/pgx) - PostgreSQL driver
- [gofakeit v7](https://github.com/brianvoe/gofakeit) - Fake data generator
