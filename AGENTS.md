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
- Debug session: isolate `wal-writer-rust` and `data-pump-go`, stop Kafka and monitoring services, and use `WAL_WRITER_DEBUG_NO_KAFKA=true` + `WAL_WRITER_DEBUG_PRINT_WAL=true` to print parsed WAL records without sending to Kafka.
- Use Docker Compose profile `deprecated-rust` only when validating legacy fallback

## Known Issues

The following issues were identified in the full codebase review (May 2026). Ordered by severity.

| # | File | Severity | Issue |
|---|------|----------|-------|
| 1 | `wal_writer_go/main.go` | **Critical** | ✅ Fixed: LSN ACK now uses `confirmedLSN` — only advances after successful Kafka publish via `publisher()` goroutine |
| 2 | `wal_writer_go/main.go` | **Critical** | ✅ Fixed: WAL parsing decoupled from Kafka I/O via `recordQueue` channel + dedicated `publisher()` goroutine |
| 3 | `wal_writer_go/main.go` | **High** | ✅ Fixed: SQL injection surface in `resetReplicationSlot` — now uses parameterised execution (`ExecParams`) for slot name |
| 4 | `wal_writer_go/main.go` | **High** | ✅ Fixed: DELETE rows are incomplete when table `REPLICA IDENTITY` is not `FULL` — now emits `partial_old_tuple` based on relation replica identity |
| 5 | `wal_writer_go/main.go` | **Medium** | ✅ Fixed: Database password redacted from pgconn connection error messages via `redactPassword()` helper |
| 6 | `wal_writer_go/main.go` | **Medium** | ✅ Fixed: `parseUpdate` now returns an error on unrecognised tuple tag — no more silent buffer misparsing |
| 7 | `wal_writer_go/main.go` | **Medium** | ✅ Fixed: Publish retry loop checks `ctx.Err()` before sleeping — graceful shutdown no longer delayed by up to 30 s |
| 8 | `wal_writer_go/main.go` | **Medium** | ✅ Fixed: `runReplication` now uses `outer: for {}` loop instead of recursive call — no unbounded stack growth |
| 9 | `wal_writer_go/main.go` | **Low** | ✅ Fixed: WAL message type labels use human-readable names via `walMessageType()` helper |
| 10 | `wal_writer_go/main.go` | **Low** | ✅ Fixed: `BatchSize` configured from `WAL_WRITER_KAFKA_BATCH_SIZE` env var (default 16384) |
| 11 | `wal_consumer/src/main.rs` | **High** | ✅ Fixed: `fetch_metadata` no longer blocks Tokio runtime — moved into `tokio::task::spawn_blocking` |
| 12 | `wal_consumer/src/main.rs` | **High** | ✅ Fixed: replaced `std::thread::sleep` in async flow with `tokio::time::sleep(...).await` |
| 13 | `wal_consumer/src/main.rs` | **Medium** | ✅ Fixed: Offset commit counters no longer reset on failure — counters accumulate so retry fires sooner |
| 14 | `wal_consumer/src/main.rs` | **Medium** | ✅ Fixed: Graceful shutdown via `tokio::signal::ctrl_c()` — final `CommitMode::Sync` commit before exit |
| 15 | `wal_consumer/src/main.rs` | **Low** | ✅ Fixed: Replaced `payload_view::<str>()` with `message.payload().map_or(0, \|p\| p.len())` — zero-copy length |
| 16 | `data_pump_go/main.go` | **Medium** | ✅ Fixed: removed `ORDER BY random()` sampling in update paths (switched to `TABLESAMPLE`) |
| 17 | `data_pump_go/main.go` | **Medium** | ✅ Fixed: replaced row-by-row UPI updates with batched `UPDATE ... WHERE transaction_id = ANY($1)` |
| 18 | `data_pump_go/main.go` | **Medium** | ✅ Fixed: configured pool max connections to 30 via `pgxpool.Config` |
| 19 | `data_pump_go/main.go` | **Low** | ✅ Fixed: wired `updateRandomRecords` into periodic UPI update ticker |
| 20 | `docker-compose.yml` | **High** | ✅ Fixed: `db-init` command never exited (looping `until` replaced with `while` + `exit 0`) — caused `docker compose up wal-writer` to hang indefinitely waiting for `service_completed_successfully` |
| 21 | `wal_writer_go/main.go` | **Critical** | ✅ Fixed: `confirmed_flush_lsn` stayed NULL — `nextStatus = time.Time{}` after each XLogData message sent `WALFlushPosition=0` before publisher confirmed anything; removed forced flush, reduced standby heartbeat to 2 s |
| 22 | `wal_writer_go/main.go` | **Critical** | ✅ Fixed: publisher throughput capped at ~44 msg/s — single `WriteMessages` call per record was bounded by `BatchTimeout=5ms`; replaced with batched drain loop (up to 500 records per `WriteMessages` call); verified **~9,700 msg/s** matching data-pump input rate |
| 23 | `wal_common/src/lib.rs` | **Critical** | ✅ Fixed: runtime env parsing gap — `WAL_WRITER_REPLICATION_BATCH_SIZE`, `WAL_WRITER_REPLICATION_POLL_INTERVAL_MS`, `WAL_WRITER_LOGGING_LEVEL`, and `WAL_WRITER_LOGGING_DIRECTORY` are now parsed from env and applied at startup |
| 24 | `wal_writer/src/pg_replication.rs` | **Critical** | ✅ Fixed: persisted slot LSN was loaded but ignored (`start_lsn` hardcoded to `0/0`) — replication now resumes from persisted LSN when present |
| 25 | `wal_writer/src/pg_replication.rs` | **Critical** | ✅ Fixed: pending batch queue overflow previously logged and continued (silent drop risk) — now fail-stop on enqueue overflow to prevent data loss with slot advancement |

## Code Review Details

Full findings from the May 2026 codebase review, ordered by file and severity.

### wal_writer_go/main.go

**Issue 1 — LSN ACK advances before Kafka confirms delivery (Critical)**

Status: ✅ Fixed in `wal_writer_go/main.go` by introducing `confirmedLSN` atomic tracking and a dedicated `publisher()` goroutine. `StandbyStatusUpdate` now sends `confirmedLSN` for both `WALFlushPosition` and `WALApplyPosition`, which only advances after successful Kafka publish.

After processing WAL data, `nextStatus = time.Time{}` forces an immediate standby heartbeat with `lastLSN` set to `xld.ServerWALEnd` — meaning PostgreSQL is told to recycle WAL **before** the Kafka publish finishes. If the process crashes mid-publish, those changes are permanently lost.

Fix: Only advance `lastLSN` and send the standby update after `publishRecord` succeeds. Track a `confirmedLSN` that advances per-record on successful publish, separate from `receivedLSN`.

---

**Issue 2 — Blocking Kafka I/O on replication loop (Critical)**

Status: ✅ Fixed in `wal_writer_go/main.go` by introducing `recordQueue` buffered channel and a dedicated `publisher()` goroutine. `publishRecord()` now enqueues to the channel; the replication loop is never blocked by Kafka I/O.

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

Status: ✅ Fixed in `wal_writer_go/main.go` via `redactPassword()` helper that replaces the literal password in any pgconn error string with `[REDACTED]` before logging.

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

Status: ✅ Fixed in `wal_consumer/src/main.rs` — the `Err` branch no longer resets `pending_commit_messages` or `last_commit_at`, so pressure accumulates and a retry fires on the very next message or tick.

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

Status: ✅ Fixed in `wal_consumer/src/main.rs` — `tokio::signal::ctrl_c()` is pinned and selected in the main loop. On Ctrl-C or stream end, a final `CommitMode::Sync` commit is issued before the process exits.

The consumer loop runs until `stream.next()` returns `None` (broker disconnect). There is no SIGTERM/SIGINT handler. On container stop, the process is killed mid-batch without a final commit. Add a `tokio::signal::ctrl_c` listener that triggers a final synchronous `CommitMode::Sync` commit before exit.

---

**Issue 15 — Full UTF-8 decode just to log payload length (Low)**

Status: ✅ Fixed in `wal_consumer/src/main.rs` — replaced `payload_view::<str>()` with `message.payload().map_or(0, |p| p.len())`.

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
- Fixed critical LSN at-most-once delivery by introducing `confirmedLSN` atomic tracking — PostgreSQL WAL retention position now only advances after successful Kafka publish.
- Fixed critical blocking Kafka I/O in replication loop by decoupling WAL parsing from publishing via `recordQueue` channel and dedicated `publisher()` goroutine.
- Fixed `parseUpdate` dead `rem = rem` branch — now returns an error on unrecognised tuple tag to prevent buffer misparsing.
- Fixed publish retry loop ignoring context cancellation — now checks `ctx.Err()` before each backoff sleep.
- Fixed recursive `runReplication` on slot-loss — replaced with `outer: for {}` loop and `continue outer`.
- Fixed raw WAL byte metric labels — `walMessageType()` helper maps bytes to human-readable names.
- Fixed Kafka `BatchSize` not wired — now reads from `WAL_WRITER_KAFKA_BATCH_SIZE` env var.
- Fixed password credential leak in log output — `redactPassword()` helper sanitizes pgconn connection error messages before logging.
- Fixed wal_consumer offset commit counters resetting on failure — counters now accumulate across failures so retry fires sooner.
- Fixed critical runtime config gap in `wal_common/src/lib.rs` so replication and logging env vars are no longer ignored at runtime.
- Fixed critical resume bug in `wal_writer/src/pg_replication.rs` by starting replication from persisted slot LSN instead of always from `0/0`.
- Fixed critical data-loss mode in `wal_writer/src/pg_replication.rs` by failing fast on pending batch queue overflow instead of logging and continuing.

## Kafka Batching (Current Rust Implementation)

The active runtime path in this repository is the Rust writer (`wal_writer/`) running as `wal-writer-rust` in Docker Compose.

Current batching flow:

1. **Stage 1: Parse + table grouping**
    - `WalParser` decodes WAL bytes into `WalRecord` values.
    - The replication loop groups records by `(schema, table)` and maps each group to topic `{topic_prefix}.{schema}.{table}`.

2. **Stage 1.5: Split into pending batches**
    - Each per-table record slice is split by `max_records_per_batch` into `PendingBatch` entries.
    - Batches are pushed to a bounded `BatchQueue` (max 1000 pending batches).

3. **Stage 2: Publisher micro-batching + Kafka publish**
    - A dedicated `batch_publisher_loop` task dequeues pending batches.
    - It coalesces contiguous same-topic batches up to configured record cap or flush interval.
    - Publish path calls `publish_batch()` and sends one Kafka message per `WalRecord` with retry on `QueueFull`.

4. **Stage 3: ACK only after publish success**
    - On publish success, `max_lsn` from the published batch is sent over ack channel.
    - Replication loop applies LSN via `update_applied_lsn()` and updates `last_acked_lsn` metric.
    - LSN is also persisted to local state for restart resume.

Current behavior guarantees:

- ACK position only advances after Kafka publish success.
- Restart resumes from persisted slot LSN when available.
- Queue overflow now fails fast to avoid silent message loss while advancing slot position.
- Fixed wal_consumer graceful shutdown — `tokio::signal::ctrl_c()` handler now performs a final synchronous commit before process exit.
- Fixed wal_consumer unnecessary UTF-8 payload decode — replaced with zero-copy `message.payload().map_or(0, |p| p.len())`.
- Fixed `db-init` command never terminating — replaced `until` loop (bash-only) with POSIX `while` loop and added explicit `exit 0`; `docker compose up wal-writer` now correctly waits for `service_completed_successfully`.
- Fixed `confirmed_flush_lsn` always NULL in replication slot — removed `nextStatus = time.Time{}` after XLogData which was sending `WALFlushPosition=0` before the async publisher confirmed any LSN; reduced standby heartbeat interval to 2 s so PostgreSQL advances slot position promptly after first batch delivery. Verified: `confirmed_flush_lsn` now advances continuously.
- Fixed publisher throughput bottleneck (44 msg/s → ~9,700 msg/s) — rewrote `publisher()` goroutine to batch-drain `recordQueue` (up to 500 records) and call `WriteMessages` once per batch instead of once per record, eliminating the `BatchTimeout` per-message serialization penalty.

## Proceed Immediately

### Phase 3 — Iceberg Consumer (wal_consumer Rust project)

Now that WAL Writer is stable at ~9,700 msg/s with `confirmed_flush_lsn` advancing, implement the Iceberg writer in the `wal_consumer` Rust project:

- Consume CDC messages from Kafka topics (`cdc.public.*`)
- Perform merge/upsert into Iceberg tables backed by S3 (or local MinIO for dev)
- Connect to catalogs: AWS Glue REST catalog or Iceberg REST catalog
- Test all operation scenarios:
  - Insert → append new rows to Iceberg table
  - Upsert / Merge (with configurable merge key, e.g. primary key)
  - Delete → mark rows as deleted or physically remove
  - Truncate → drop + recreate Iceberg table partition

### Operational Runbooks (add)
- Slot lag recovery: steps when `confirmed_flush_lsn` falls behind
- Slot invalidation: recreate slot + restart wal-writer sequence
- Redeploy verification: confirm `confirmed_flush_lsn` is non-NULL within 10 s of start