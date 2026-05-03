# WAL Writer - PostgreSQL CDC to Kafka

![Architecture Diagram](docs/images/screenshot-architect.png)

High-throughput CDC capture from PostgreSQL WAL with sub-second latency, designed for PhonePe scale.

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 — Data Ingestion | Go Data Pump → PostgreSQL | ✅ Complete |
| Phase 2 — CDC Capture & Streaming | WAL Cake Agent → Kafka | ✅ Complete |
| Phase 3 — Data Lake Write | Table Writer → Iceberg → S3 | ❌ Not Started |

### Phase 2 Completed Items
- [x] Wire `WalParser::parse()` into the replication loop end-to-end
- [x] LSN persistence on commit
- [x] Graceful shutdown flushing of pending Kafka messages
- [x] Comprehensive Insert/Update/Delete/Truncate fixture tests
- [x] Remove `eprintln!` debug output; fix unused import warnings in `decoder.rs`
- [x] Make `Metrics` shared (`Arc`) so counters update across tasks
- [x] Advance replication slot `confirmed_flush_lsn` via `update_applied_lsn()`
- [x] Fix publication wiring using `WAL_WRITER_PUBLICATION`
- [x] Verify end-to-end CDC delivery from PostgreSQL WAL to Kafka topics
- [x] Add PostgreSQL and Kafka Grafana dashboards for replication and broker visibility
- [x] Cap PostgreSQL WAL retention to 5GB in local Docker Compose

## Architecture

The system is split into three phases of execution:

### Phase 1 — Data Ingestion ✅ Complete
The **Go Data Pump** generates synthetic UPI transactions and writes inserts/updates into **PostgreSQL**. This simulates high-throughput transactional workloads against the source database.

Deployed services: `postgres` (PostgreSQL 16 with `wal_level=logical`), `data-pump-go` (UPI transaction generator ~10k txns/sec), `db-init` (one-shot job that creates the replication slot `wal_writer_slot` and publication `wal_writer_publication`).

```
Go Data Pump ──(insert & update)──► PgSQL
```

### Phase 2 — CDC Capture & Streaming ✅ Complete
The **WAL Cake Agent** (this Rust project) connects to PostgreSQL via logical replication, consumes the Write-Ahead Log, decodes change events (Insert/Update/Delete/Truncate), and pushes them as structured CDC messages to **Kafka Topics**. Metrics are exported to **Grafana** for observability.

Deployed services: `wal-writer` (Rust CDC agent, metrics on `:9090`), `kafka` (Confluent CP-Kafka 7.5, auto topic creation enabled on port `9092`).

Verified: the replication loop now parses pgoutput messages end-to-end, acknowledges consumed LSNs back to PostgreSQL, persists commit progress, and publishes CDC events to Kafka topics. Observability is in place through Prometheus, PostgreSQL Exporter, Kafka Exporter, and Grafana dashboards.

```
PgSQL ──(consume WAL)──► WAL Cake Agent ──(push to topic)──► Kafka Topics
                                 │
                                 └──(metrics :9090)──► Prometheus ──► Grafana
```

### Phase 3 — Data Lake Write ❌ Not Started
The **Table Writer** will consume CDC messages from Kafka, register table schemas with the **Iceberg Rest Catalog**, and write data files to **S3**. Grafana will also monitor this phase.

No services deployed yet. Table Writer, Iceberg Rest Catalog, and S3 are not present in the current docker-compose setup.

```
Kafka Topics ──(consume CDC msgs)──► Table Writer ──► Iceberg Rest Catalog
                                            │                    │
                                            └────────────────────►  S3
                                            │
                                            └──(metrics)──► Grafana
```

## Project Structure

```
rust-wal-cake-writer/
├── AGENTS.md                 # This file
├── .gitignore
├── docker-compose.yml        # Local development setup
├── k8s/                      # Kubernetes manifests
│   ├── configmap.yaml
│   ├── data-pump-go.yaml   # Go data pump deployment
│   ├── deployment.yaml
│   ├── kafka.yaml
│   ├── kustomization.yaml
│   ├── namespace.yaml
│   ├── postgres.yaml
│   └── rbac.yaml
├── data_pump_go/             # Go data pump utility (UPI transaction generator)
│   ├── main.go
│   ├── go.mod
│   ├── go.sum
│   ├── Dockerfile
│   └── README.md
└── wal_writer/               # Main Rust project
    ├── Cargo.toml
    ├── Cargo.lock
    ├── Dockerfile
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
| `WAL_WRITER_PUBLICATION` | `wal_writer_publication` | Logical replication publication |
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

## Local Development

### Docker Compose Services

All Phase 1 and Phase 2 services plus the full monitoring stack are available via Docker Compose:

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `postgres` | postgres:16-alpine | 5432 | Source database with logical replication |
| `kafka` | confluentinc/cp-kafka:7.5.0 | 9092 | CDC event broker |
| `data-pump-go` | data-pump-go:latest | — | UPI transaction load generator |
| `db-init` | postgres:16-alpine | — | One-shot: creates replication slot + publication |
| `wal-writer` | wal-writer:latest | 9090 | CDC agent (metrics on `/health`, `/ready`) |
| `postgres-exporter` | prometheuscommunity/postgres-exporter | 9187 | PostgreSQL metrics exporter |
| `kafka-exporter` | danielqsj/kafka-exporter:latest | 9308 | Kafka broker and topic metrics exporter |
| `prometheus` | prom/prometheus | 9091 | Metrics collection |
| `grafana` | grafana/grafana | 3000 | Metrics dashboards (admin/admin) |

```bash
# Start all services
docker compose up -d

# Start infra only (postgres + kafka + db-init)
docker compose up -d postgres kafka db-init

# Start monitoring stack
docker compose up -d prometheus grafana postgres-exporter
```

### Kubernetes Testing

```bash
# Deploy infra (PostgreSQL + Kafka)
kubectl apply -k k8s/ -t infra

# Or individually
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/kafka.yaml
```

## Monitoring

The observability stack is fully deployed alongside the application services:

- **PostgreSQL Exporter** (`:9187`) — scrapes PostgreSQL internal metrics (replication lag, WAL activity, connection counts) and exposes them in Prometheus format.
- **Kafka Exporter** (`:9308`) — exposes Kafka broker, topic, partition, and consumer lag metrics for Grafana dashboards.
- **Prometheus** (`:9091`) — collects metrics from the PostgreSQL exporter and from the WAL Writer's own Prometheus endpoint (`:9090`). Configuration in `prometheus.yml`.
- **Grafana** (`:3000`) — visualises all metrics. Pre-provisioned with:
  - Datasource: Prometheus (configured in `grafana/datasources/datasource.yml`)
  - Dashboard provider: `grafana/dashboards/provider.yml`
  - Dashboard JSON: `grafana/dashboard-json/postgres-monitoring.json`
   - Dashboard JSON: `grafana/dashboard-json/kafka-exporter.json`
  - Default credentials: `admin / admin`

Metric flow:

```
PgSQL ──► postgres-exporter (:9187) ──┐
                                      ├──► Prometheus (:9091) ──► Grafana (:3000)
wal-writer (:9090/metrics) ───────────┘
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

1. Start Phase 3 table writer implementation
   - Consume CDC topics from Kafka, resolve table schemas, and write into Iceberg-backed storage.
2. Add broader integration coverage
   - Exercise PostgreSQL to Kafka flow under Docker Compose with replication slot recovery and Kafka topic assertions.
3. Harden operational workflows
   - Add runbooks for slot lag recovery, slot invalidation after WAL retention limits, and redeploy verification steps.
4. Expand observability
   - Add wal-writer specific dashboards and alerts for parse failures, Kafka send errors, replication lag, and message throughput.

If you want I can implement these steps in order. Stopping now as requested.

## Recent Work

- Wired the parser into the replication loop end-to-end and verified CDC delivery into Kafka topics.
- Fixed replication slot advancement by calling `update_applied_lsn()`, allowing PostgreSQL to advance `confirmed_flush_lsn` and recycle WAL.
- Added `WAL_WRITER_PUBLICATION` configuration, correcting publication selection for logical replication.
- Persisted commit LSN progress and verified slot lag converges after replication catches up.
- Added fixture-based WAL parser tests covering Relation plus Insert/Update/Delete decoding.
- Cleaned up logging so info-level output is summary-oriented instead of per-change verbose.
- Added PostgreSQL and Kafka Grafana dashboard support, including datasource fixes and stable template queries for Kafka exporter metrics.
- Updated the PostgreSQL dashboard to show WAL size, database size, and slot lag in GB.
- Capped PostgreSQL WAL retention to 5GB in Docker Compose using `max_wal_size` and `max_slot_wal_keep_size`.
