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
The **WAL Cake Agent** (Go implementation) connects to PostgreSQL via logical replication, consumes the Write-Ahead Log, decodes change events (Insert/Update/Delete/Truncate), and pushes them as structured CDC messages to **Kafka Topics**. Metrics are exported to **Grafana** for observability.

Deployed services: `wal-writer` (Go CDC agent, metrics on `:9090`), `kafka` (Confluent CP-Kafka 7.5, auto topic creation enabled on port `9092`).

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
├── wal_writer_go/            # Main Go WAL writer project
│   ├── Dockerfile
│   ├── go.mod
│   └── main.go
└── wal_writer/               # Deprecated Rust WAL writer (legacy fallback)
   ├── Cargo.toml
   ├── Cargo.lock
   ├── Dockerfile            # Deprecated image, retained for rollback only
   ├── src/
   │   ├── main.rs
   │   ├── lib.rs
   │   ├── config.rs
   │   ├── decoder.rs
   │   ├── kafka.rs
   │   ├── metrics.rs
   │   ├── pg_replication.rs
   │   ├── state.rs
   │   └── wal_parser.rs
   └── tests/
      ├── wal_parser_tests.rs
      └── wal_parser_fixtures.rs
```

## Build

```bash
docker compose build wal-writer
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
docker compose up -d wal-writer
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
| `wal-writer` | wal-writer:latest | 9090 | Go CDC agent (metrics on `/health`, `/ready`) |
| `wal-writer-rust` | wal-writer-rust:latest | 9095 | Deprecated Rust fallback (disabled by default) |
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

- WAL Writer default runtime is now Go (`wal_writer_go/`)
- Rust WAL Writer (`wal_writer/`) is deprecated and kept only for rollback safety
- Kafka topic naming remains `{topic_prefix}.{schema}.{table}`
- Metrics endpoint remains on `:9090` with `/health` and `/ready`
- Use Docker Compose profile `deprecated-rust` only when validating legacy fallback

## Known Issues

The following issues were identified in the full codebase review (May 2026). Ordered by severity.

| # | File | Severity | Issue |
|---|------|----------|-------|
| 1 | `wal_writer_go/main.go` | **Critical** | LSN ACK sent before Kafka delivery confirmed — data loss on crash (at-most-once semantics) |
| 2 | `wal_writer_go/main.go` | **Critical** | Synchronous blocking Kafka writes in the WAL replication hot path — replication slot drops under Kafka backpressure |
| 3 | `wal_writer_go/main.go` | **High** | ✅ Fixed: SQL injection surface in `resetReplicationSlot` — now uses parameterised execution (`ExecParams`) for slot name |
| 4 | `wal_writer_go/main.go` | **High** | ✅ Fixed: DELETE rows are incomplete when table `REPLICA IDENTITY` is not `FULL` — now emits `partial_old_tuple` based on relation replica identity |
| 5 | `wal_writer_go/main.go` | **Medium** | Database password leaks into log output through pgconn connection error messages |
| 6 | `wal_writer_go/main.go` | **Medium** | Dead assignment `rem = rem` in `parseUpdate` on unrecognised tuple tag — buffer position not advanced, causes misparsing of subsequent columns |
| 7 | `wal_writer_go/main.go` | **Medium** | Publish retry loop does not check `ctx.Err()` before sleeping — delays graceful shutdown by up to 30 s |
| 8 | `wal_writer_go/main.go` | **Medium** | `runReplication` calls itself recursively on slot-loss — unbounded stack growth under repeated failures |
| 9 | `wal_writer_go/main.go` | **Low** | WAL message type labels in Prometheus metrics are raw bytes (`B`, `C`, …) instead of human-readable names |
| 10 | `wal_writer_go/main.go` | **Low** | `BatchSize` not set on Kafka writer — defaults to 100, negating `lingerMs` batching at high throughput |
| 11 | `wal_consumer/src/main.rs` | **High** | ✅ Fixed: `fetch_metadata` no longer blocks Tokio runtime — moved into `tokio::task::spawn_blocking` |
| 12 | `wal_consumer/src/main.rs` | **High** | ✅ Fixed: replaced `std::thread::sleep` in async flow with `tokio::time::sleep(...).await` |
| 13 | `wal_consumer/src/main.rs` | **Medium** | Offset commit counters reset on failure — delays retry commit and can widen uncommitted offset window |
| 14 | `wal_consumer/src/main.rs` | **Medium** | No graceful shutdown (SIGTERM / Ctrl-C) — uncommitted offsets lost when container is stopped |
| 15 | `wal_consumer/src/main.rs` | **Low** | Full UTF-8 decode (`payload_view::<str>()`) on every message only to log `payload.len()` — use `payload().map_or(0, \|p\| p.len())` |
| 16 | `data_pump_go/main.go` | **Medium** | ✅ Fixed: removed `ORDER BY random()` sampling in update paths (switched to `TABLESAMPLE`) |
| 17 | `data_pump_go/main.go` | **Medium** | ✅ Fixed: replaced row-by-row UPI updates with batched `UPDATE ... WHERE transaction_id = ANY($1)` |
| 18 | `data_pump_go/main.go` | **Medium** | ✅ Fixed: configured pool max connections to 30 via `pgxpool.Config` |
| 19 | `data_pump_go/main.go` | **Low** | ✅ Fixed: wired `updateRandomRecords` into periodic UPI update ticker |

## Code Review Details

Full findings from the May 2026 codebase review, ordered by file and severity.

### wal_writer_go/main.go

**Issue 1 — LSN ACK advances before Kafka confirms delivery (Critical)**

After processing WAL data, `nextStatus = time.Time{}` forces an immediate standby heartbeat with `lastLSN` set to `xld.ServerWALEnd` — meaning PostgreSQL is told to recycle WAL **before** the Kafka publish finishes. If the process crashes mid-publish, those changes are permanently lost.

Fix: Only advance `lastLSN` and send the standby update after `publishRecord` succeeds. Track a `confirmedLSN` that advances per-record on successful publish, separate from `receivedLSN`.

---

**Issue 2 — Blocking Kafka I/O on replication loop (Critical)**

`processWALData` is synchronous. Every `publishRecord` call blocks the replication loop for up to 5 s × 6 retries = **30 seconds worst case**. During that time no keepalives are sent, the replication connection times out, and PostgreSQL drops the slot.

Fix: Decouple WAL parsing from Kafka publishing. Parse WAL messages into a buffered channel; a separate goroutine pool drains and publishes. Use per-partition or per-table ordering queues to preserve event order within a table.

---

**Issue 3 — SQL injection in `resetReplicationSlot` (High)**

Status: ✅ Fixed in `wal_writer_go/main.go` using `pgconn.ExecParams` with `$1` parameter binding.

The slot name is interpolated via naive single-quote escaping:
```go
slotName := strings.ReplaceAll(rt.cfg.pgSlotName, "'", "''")
dropSQL := "SELECT pg_drop_replication_slot('" + slotName + "') ..."
```
This is an SQL injection surface. Use parameterised queries via `pgconn.Exec` with `$1` placeholders instead.

---

**Issue 4 — Incomplete DELETE rows when REPLICA IDENTITY is not FULL (High)**

Status: ✅ Fixed in `wal_writer_go/main.go` by persisting relation replica identity and emitting `partial_old_tuple` in DELETE events when identity is not `f`.

When a table has `REPLICA IDENTITY DEFAULT` (primary key only), pgoutput sends an `O` tuple tag for the old row containing only PK columns. `parseTuple` will silently produce incomplete column data (nulls for non-PK columns) without signalling this to consumers. Downstream consumers receive partial rows for deletes.

Fix: Record the replica identity flag from the `R` (Relation) message and attach it to `relationMeta`. In `parseDelete`, set a field like `PartialOldTuple: true` when replica identity is not `f` (full).

---

**Issue 5 — Password leaks into log output (Medium)**

The connection string is built and passed to `pgconn.Connect`. If pgconn logs errors, the password appears in log output. Log connection parameters individually without the password:
```go
rt.logger.Printf("connecting to pg=%s:%s db=%s", rt.cfg.pgHost, rt.cfg.pgPort, rt.cfg.pgDatabase)
```

---

**Issue 6 — Dead `rem = rem` in `parseUpdate` (Medium)**

```go
} else {
    rem = rem  // dead assignment — wrong semantics
}
```
If the tuple tag is neither `K`, `O`, nor `N`, the buffer position is not advanced, causing `parseTuple` to misparse subsequent columns. This branch should return an error.

---

**Issue 7 — Publish retry ignores context cancellation (Medium)**

```go
for i := 0; i < 6; i++ {
    writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    lastErr = rt.writer.WriteMessages(writeCtx, ...)
    cancel()
    if lastErr == nil { return nil }
    time.Sleep(backoff)  // blocks even if ctx is Done
```
When the process is shutting down (SIGTERM), this loop continues sleeping for up to 3 seconds between retries. Add a check before sleeping:
```go
if ctx.Err() != nil { return ctx.Err() }
```

---

**Issue 8 — Recursive `runReplication` on slot-loss (Medium)**

```go
return rt.runReplication(ctx)  // recursive call
```
Each slot-loss event grows the Go call stack by one frame. Under repeated failures this leaks goroutine stack memory. Replace with a `for` loop and `continue`.

---

**Issue 9 — Raw byte WAL message type labels in Prometheus metrics (Low)**

```go
rt.m.walMessages.WithLabelValues(string(msgType)).Inc()
```
`msgType` is a raw byte (`'B'`, `'C'`, `'R'`, etc.). Labels will be single-character strings that are ambiguous in dashboards. Use explicit named labels: `"begin"`, `"commit"`, `"relation"`, `"insert"`, `"update"`, `"delete"`, `"truncate"`.

---

**Issue 10 — `BatchSize` not configured on Kafka writer (Low)**

```go
writer := &kafka.Writer{
    BatchTimeout: time.Duration(cfg.lingerMs) * time.Millisecond,
    // BatchSize not set — defaults to 100 messages
}
```
At 10 k msg/sec, batches fill and flush every 100 messages regardless of linger time, negating the batching effect. Set `BatchSize` from the existing `WAL_WRITER_KAFKA_BATCH_SIZE` config variable.

---

### wal_consumer/src/main.rs

**Issue 11 — `fetch_metadata` blocks Tokio executor (High)**

Status: ✅ Fixed in `wal_consumer/src/main.rs` by moving metadata fetch into `tokio::task::spawn_blocking`.

```rust
let metadata = consumer.fetch_metadata(None, Duration::from_secs(5));  // synchronous
```
This is a synchronous blocking call inside a `#[tokio::main]` async runtime. Under load it blocks the Tokio executor for up to 5 seconds, stalling all async tasks. Move to `spawn_blocking` or use the async metadata API.

---

**Issue 12 — `std::thread::sleep` in async context (High)**

Status: ✅ Fixed in `wal_consumer/src/main.rs` by replacing with `tokio::time::sleep(...).await`.

```rust
std::thread::sleep(Duration::from_secs(5));  // in #[tokio::main]
```
Blocks the Tokio runtime thread during initial topic discovery. Replace with `tokio::time::sleep(Duration::from_secs(5)).await`.

---

**Issue 13 — Commit counters reset on failure (Medium)**

```rust
Err(err) => {
    warn!("Kafka offset commit failed (will retry): {}", err);
    pending_commit_messages = 0;        // resets position
    last_commit_at = tokio::time::Instant::now();
}
```
Resetting `pending_commit_messages` and `last_commit_at` on failure means the next commit won't trigger until another 500 messages or 1 second passes. The uncommitted messages are not tracked, so the retry commit window widens under high lag. Do not reset on failure — let counters continue growing so a retry fires sooner.

---

**Issue 14 — No graceful shutdown (Medium)**

The consumer loop runs until `stream.next()` returns `None` (broker disconnect). There is no SIGTERM/SIGINT handler. On container stop, the process is killed mid-batch without a final commit. Add a `tokio::signal::ctrl_c` listener that triggers a final synchronous `CommitMode::Sync` commit before exit.

---

**Issue 15 — Full UTF-8 decode just to log payload length (Low)**

```rust
let payload = message.payload_view::<str>()  // full UTF-8 decode every message
    ...
info!("... payload_bytes={}", payload.len());  // only size logged
```
The full UTF-8 decode allocates and validates on every message even though only the byte count is used. Use `message.payload().map_or(0, |p| p.len())` for zero-copy length.

---

### data_pump_go/main.go

**Issue 16 — `ORDER BY random()` is O(N) full table scan (Medium)**

Status: ✅ Fixed in `data_pump_go/main.go` by replacing random-order sampling with `TABLESAMPLE` in update selectors.

```go
SELECT transaction_id FROM upi_transactions ORDER BY random() LIMIT $1
```
At millions of rows, this performs a full sequential scan + sort-by-random every 5 minutes per table. At scale this generates massive I/O and WAL amplification on the source database. Use `TABLESAMPLE SYSTEM(n)` or a keyset-based random sample instead.

---

**Issue 17 — Row-by-row UPDATE loop (Medium)**

Status: ✅ Fixed in `data_pump_go/main.go` by using a batched update with `WHERE transaction_id = ANY($1)`.

```go
for _, id := range txnIDs {
    pool.Exec(ctx, `UPDATE upi_transactions SET ... WHERE transaction_id = $7`, ..., id)
```
Each update is a separate round trip. With `numUpdates` potentially in the thousands, this saturates the connection pool and generates thousands of individual WAL records, amplifying downstream CDC load. Replace with a single `UPDATE ... WHERE transaction_id = ANY($1)` using an array parameter.

---

**Issue 18 — Default pool size too small for concurrent workers (Medium)**

Status: ✅ Fixed in `data_pump_go/main.go` by setting `pgxpool` max connections to 30 via parsed pool config.

`pgxpool.New` uses the default max pool size of 4 connections. With 10 UPI workers + 10 user workers + 3 subscription workers + 2 offer workers + 2 update ticker goroutines = 27 concurrent database workers competing for 4 connections, connection starvation occurs under load. Set `pool_max_conns` in the connection string or via `pgxpool.Config` to at least 30.

---

**Issue 19 — `updateRandomRecords` is dead code (Low)**

Status: ✅ Fixed in `data_pump_go/main.go` by wiring UPI update ticker to call `updateRandomRecords` periodically.

The function `updateRandomRecords` at the bottom of `data_pump_go/main.go` is defined but never called — all update paths use `updateRandomUsers` and `updateRandomSubscriptions`. Either remove it or wire it to the UPI transaction update path (which currently has no update workers despite being the primary table).

---

### docker-compose.yml

**Issue 20 — `KAFKA_ADVERTISED_LISTENERS` hostname collision (Low)**

```yaml
KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9092"
```
Both listeners advertise on port `9092` but with different hostnames (`kafka` vs `localhost`). External clients connecting to `localhost:9092` will be redirected to `kafka:9092`, which is not resolvable outside Docker. Consider mapping `PLAINTEXT_HOST` to `localhost:19092` to match the existing `"9092:19092"` port binding.

---

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
- Converted WAL Writer runtime container from Rust to Go for active development.
- Deprecated Rust WAL Writer component and moved it behind an opt-in Docker Compose profile.
- Fixed high-severity SQL injection in replication slot reset by parameterising slot-name SQL in `wal_writer_go/main.go`.
- Fixed high-severity DELETE partial-row ambiguity by tracking relation replica identity and emitting `partial_old_tuple` in CDC delete events.
- Fixed high-severity Tokio blocking behavior in `wal_consumer/src/main.rs` by using `spawn_blocking` for metadata fetch and async sleep in discovery loop.
- Fixed data pump scale issues by removing `ORDER BY random()` sampling, batching UPI updates, increasing pool max conns, and wiring UPI update ticker.
