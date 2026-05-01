# WAL Writer - PostgreSQL CDC to Kafka

High-throughput CDC capture from PostgreSQL WAL with sub-second latency, designed for PhonePe scale.

## Project Structure

```
rust-wal-cake-writer/
├── AGENTS.md                 # This file
├── .gitignore
├── k8s/                      # Kubernetes manifests
│   ├── configmap.yaml
│   ├── deployment.yaml
│   ├── kafka.yaml
│   ├── kustomization.yaml
│   ├── postgres.yaml
│   └── rbac.yaml
├── data_pump/                # Python data pump utility
│   ├── main.py
│   ├── pyproject.toml
│   ├── README.md
│   └── .python-version
└── wal_writer/               # Main Rust project
    ├── Cargo.toml
    ├── Cargo.lock
    ├── src/
    │   ├── main.rs           # Entry point
    │   ├── lib.rs            # Library root
    │   ├── config.rs         # Configuration from env vars
    │   ├── decoder.rs        # Kafka message encoding
    │   ├── kafka.rs          # Kafka producer
    │   ├── metrics.rs        # Prometheus metrics
    │   ├── pg_replication.rs # PostgreSQL replication
    │   ├── state.rs          # State persistence
    │   └── wal_parser.rs     # WAL message parser
    └── tests/
        ├── wal_parser_tests.rs
        └── wal_parser_fixtures.rs
```

## Build

```bash
cargo build --release
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WAL_WRITER_PG_HOST` | `localhost` | PostgreSQL host |
| `WAL_WRITER_PG_PORT` | `5432` | PostgreSQL port |
| `WAL_WRITER_PG_USER` | `postgres` | PostgreSQL user |
| `WAL_WRITER_PG_PASSWORD` | `` | PostgreSQL password |
| `WAL_WRITER_PG_DATABASE` | `postgres` | Database name |
| `WAL_WRITER_PG_SLOT_NAME` | `wal_writer_slot` | Replication slot name |
| `WAL_WRITER_KAFKA_BROKERS` | `localhost:9092` | Kafka brokers |
| `WAL_WRITER_KAFKA_TOPIC_PREFIX` | `cdc` | Topic prefix |
| `WAL_WRITER_KAFKA_ACKS` | `all` | Kafka acks |
| `WAL_WRITER_KAFKA_LINGER_MS` | `5` | Linger ms |
| `WAL_WRITER_KAFKA_BATCH_SIZE` | `16384` | Batch size |
| `WAL_WRITER_REPLICATION_BATCH_SIZE` | `100` | WAL batch size |
| `WAL_WRITER_REPLICATION_POLL_INTERVAL_MS` | `10` | Poll interval ms |
| `WAL_WRITER_LOGGING_DIRECTORY` | `/var/log/wal-writer` | Log directory |
| `WAL_WRITER_LOGGING_LEVEL` | `info` | Log level |

## PostgreSQL Setup

```sql
-- Create publication
CREATE PUBLICATION wal_writer_publication FOR ALL TABLES;

-- Slot is created automatically
```

## Kafka Topics

Each table maps to `{topic_prefix}.{schema}.{table}` (e.g., `cdc.public.users`).

## Run

```bash
./target/release/rust-wal-cake-writer
```

## Kubernetes

Deploy using Kustomize:

```bash
kubectl apply -k k8s/
```

Or directly:

```bash
kubectl apply -f k8s/
```

### Manifests

- `k8s/configmap.yaml` - Configuration
- `k8s/deployment.yaml` - Deployment + Service + PDB
- `k8s/rbac.yaml` - ServiceAccount + RBAC
- `k8s/postgres.yaml` - PostgreSQL with logical replication
- `k8s/kafka.yaml` - Kafka broker
- `k8s/kustomization.yaml` - Kustomize overlay

### Resource Limits

- Requests: 500m CPU, 512Mi memory
- Limits: 2000m CPU, 2Gi memory

### High Availability

- 2 replicas with pod anti-affinity
- PodDisruptionBudget: minAvailable: 1

## Local Testing

```bash
# Deploy infra (PostgreSQL + Kafka)
kubectl apply -k k8s/ -t infra

# Or individually
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/kafka.yaml
```

## Notes

- Uses PostgreSQL logical replication via `tokio-postgres::copy_out()`
- Custom WAL parser for decoding replication messages
- Kafka producer with configurable batch settings
- Sub-second latency via 10ms poll interval
- Uses PostgreSQL logical replication via `pgwire-replication` (streaming) and `pg_walstream` for pgoutput parsing
- WAL parser implemented with pg_walstream: decodes Relation/Begin/Commit/Insert/Update/Delete/Truncate messages, caches relation metadata, maps tuple columns to named columns and attaches transaction metadata when available
- Unit test added: tests/wal_parser_tests.rs (basic empty payload test). More fixtures can be added using pg_walstream helpers
- Note: building pg_walstream requires libpq headers on some systems. On macOS install libpq via Homebrew: `brew install libpq && brew link --force libpq`

## Next Steps

1. Fully consume multi-message WAL payloads in the parser
   - Use pg_walstream's BufferReader-based API to iterate messages from a single Bytes payload and return all messages as WalRecord entries. This avoids relying on the replication stream to provide one message per payload.
2. Wire parser into replication loop end-to-end
   - Call WalParser::parse() from the replication XLogData handler, batch results and pass them to WalDecoder::send_batch, ensuring LSN persistence on commit and graceful shutdown flows flush pending Kafka messages.
3. Add comprehensive unit & integration tests
   - Create fixtures for Insert/Update/Delete/Truncate using pg_walstream test helpers and assert decoded WalRecord contents (column names, types, values, tx metadata).
4. Clean up and observability
   - Make Metrics shared (Arc) so counters are updated across tasks, remove unused imports, and silence remaining warnings. Add more unit tests for metrics and decoder behaviour.

If you want I can implement these steps in order. Stopping now as requested.

## Recent Work

- Implemented low-level BufferReader-based WAL parser using pg_walstream buffer primitives. The parser now fully consumes multiple logical messages from a single Bytes payload and decodes Relation/Begin/Commit/Insert/Update/Delete/Truncate into WalRecord entries with named columns and transaction metadata when available.
- Added fixtures-based unit tests: tests/wal_parser_fixtures.rs builds a Relation + Insert + Update + Delete payload and asserts the parser decodes the expected records. The test suite was run locally and the fixture test passed.
- Notes: temporary debug output remains in the parser (eprintln!) to aid verification and there is a minor unused-import warning in decoder.rs. These are planned cleanup tasks.
