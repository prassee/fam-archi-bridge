# WAL Writer - PostgreSQL CDC to Kafka

High-throughput CDC capture from PostgreSQL WAL with sub-second latency, designed for PhonePe scale.

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