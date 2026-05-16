# Iceberg Integration Guide (Phase 3)
## CDC to Data Lake: wal_consumer → Iceberg Tables on S3

**Status:** Ready for implementation  
**Target:** 50,000+ records/sec into Iceberg tables  
**Timeline:** 2-3 weeks (4-5 full-time engineers)

---

## ARCHITECTURE

```
┌─ PostgreSQL ──→ wal_writer → Kafka Topics (cdc.public.*)
                                      ↓
                              ┌─────────────────┐
                              │  wal_consumer   │
                              │  (Rust + Tokio) │
                              └────────┬────────┘
                                       ↓
                    ┌──────────────────┼──────────────────┐
                    ↓                  ↓                  ↓
            ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
            │ Schema Cache │  │ Row Batching │  │ Upsert Logic │
            └──────────────┘  └──────────────┘  └──────────────┘
                    ↓                  ↓                  ↓
            ┌──────────────────────────────────────────────────┐
            │       Iceberg Catalog (Glue / REST / File)       │
            └────────────────┬────────────────────────────────┘
                             ↓
                ┌────────────────────────┐
                │  S3 / MinIO (Backend)  │
                └────────────────────────┘
                             ↓
            ┌──────────────────────────────────┐
            │ Analytics (Athena / Spark / SQL) │
            └──────────────────────────────────┘
```

---

## PHASE 3 IMPLEMENTATION ROADMAP

### Stage 1: Foundation (Week 1)
**Goal:** Basic consume, schema resolution, Iceberg catalog connectivity

#### Tasks
- [ ] **Iceberg Client Setup**
  - [ ] Add dependencies: `iceberg` crate, `arrow`, `parquet`
  - [ ] Implement `IcebergCatalog` wrapper (Glue REST or Iceberg REST)
  - [ ] Test catalog connectivity with test table creation

- [ ] **Schema Resolution**
  - [ ] Extract schema from first CDC record (all columns)
  - [ ] Map PostgreSQL types → Iceberg types:
    - `int2/int4/int8` → `INTEGER/LONG`
    - `float4/float8` → `FLOAT/DOUBLE`
    - `text/varchar` → `STRING`
    - `bool` → `BOOLEAN`
    - `timestamp` → `TIMESTAMP`
    - `jsonb` → `STRING` (serialize)
    - `bytea` → `BINARY`
  - [ ] Cache schema per (schema, table) to avoid repeated lookups

- [ ] **Table Creation on First Record**
  - [ ] If table doesn't exist in catalog, create it:
    - [ ] Name: `{namespace}.{pg_schema}_{pg_table}` (e.g., `cdc.public_transactions`)
    - [ ] Partition key: configurable per table (default: none)
    - [ ] Storage: S3 location = `s3://bucket/cdc/{pg_schema}/{pg_table}/`

- [ ] **DLQ & Error Handling**
  - [ ] Implement error → Kafka DLQ topic: `cdc.errors.{source_table}`
  - [ ] Log: original record, error message, timestamp, retry count

---

### Stage 2: Write Operations (Week 1-2)
**Goal:** Handle Insert/Update/Delete/Truncate operations

#### Tasks

- [ ] **INSERT Operation**
  - [ ] Append new rows to Iceberg table
  - [ ] Schema: extract all columns from `new_tuple`
  - [ ] Call `table.append_batch(arrow_records)`

- [ ] **UPDATE Operation**
  - [ ] Merge-on-primary-key logic:
    - [ ] Load config: `WAL_CONSUMER_PRIMARY_KEYS="public.transactions:id,public.users:user_id"`
    - [ ] Match old row by primary key
    - [ ] Replace with new values
  - [ ] Use Iceberg's `table.update()` or implement via `delete + insert`

- [ ] **DELETE Operation**
  - [ ] Two modes (configurable):
    - [ ] **Physical delete:** Remove row from table
    - [ ] **Logical delete:** Add `_deleted_at` timestamp column, mark with timestamp
  - [ ] Default: Logical delete (preserves audit trail)

- [ ] **TRUNCATE Operation**
  - [ ] Drop table from catalog
  - [ ] Delete S3 objects for table

---

### Stage 3: Batching & Commit Strategy (Week 2)
**Goal:** Efficient writes with transactional guarantees

#### Tasks

- [ ] **Record Batching**
  - [ ] Buffer records by table + operation type
  - [ ] Batch size: 5,000 records (configurable)
  - [ ] Flush interval: 30 seconds (configurable)
  - [ ] Max batch memory: 100MB

- [ ] **Commit Strategy**
  - [ ] **Idempotent commits:** Track LSN per table
    - [ ] Before write, check if LSN already committed
    - [ ] Use LSN as transaction ID (deterministic)
  - [ ] **At-least-once delivery:** OK since updates are idempotent
  - [ ] Offset commit to Kafka only after Iceberg write succeeds

- [ ] **Partition Pruning**
  - [ ] If partitioned by date, route records to correct partition
  - [ ] Extract date from `tx_commit_time` or `_processed_at`

---

### Stage 4: Performance & Scaling (Week 2-3)
**Goal:** Achieve 50k+ records/sec

#### Tasks

- [ ] **Parallel Table Writers**
  - [ ] One tokio task per table (not per partition)
  - [ ] Distribute records by hash(schema, table_name)
  - [ ] Bounded channel per table (capacity 1000 batches)

- [ ] **Iceberg Client Pooling**
  - [ ] Create connection pool to Iceberg catalog
  - [ ] Reuse table handles across writes

- [ ] **Arrow Batch Optimization**
  - [ ] Pre-allocate Arrow arrays with capacity
  - [ ] Minimize allocations in hot path
  - [ ] Use `RecordBatch` builder API

- [ ] **Metrics & Observability**
  - [ ] Per-table write throughput
  - [ ] Commit latency histogram
  - [ ] Schema cache hit rate
  - [ ] Kafka lag (pending records)

---

### Stage 5: Integration & Testing (Week 3)
**Goal:** End-to-end validation

#### Tasks

- [ ] **Integration Tests**
  - [ ] Spin up local Iceberg + S3 (MinIO)
  - [ ] Generate CDC records for Insert/Update/Delete/Truncate
  - [ ] Verify Iceberg table state after each operation
  - [ ] Validate idempotency (re-commit same batch, verify no duplicates)

- [ ] **Performance Tests**
  - [ ] Target: 50k records/sec sustained
  - [ ] Measure: Latency, CPU, memory, Kafka lag

- [ ] **Failover Tests**
  - [ ] Restart wal_consumer mid-write
  - [ ] Verify resume from correct offset
  - [ ] Verify no data loss / duplication

---

## CODE STRUCTURE

### New Files

```rust
// wal_consumer/src/iceberg.rs
pub struct IcebergWriter {
    catalog: Arc<dyn Catalog>,
    namespace: String,
    table_cache: Arc<RwLock<HashMap<String, Table>>>,
    schema_cache: Arc<RwLock<HashMap<String, Schema>>>,
}

impl IcebergWriter {
    pub async fn write_record(&self, record: &WalRecord) -> Result<()>
    pub async fn flush_pending(&self) -> Result<()>
}

// wal_consumer/src/schema_resolver.rs
pub struct SchemaResolver {
    catalog: Arc<dyn Catalog>,
    cache: Arc<RwLock<HashMap<(String, String), Schema>>>,
}

impl SchemaResolver {
    pub async fn resolve(&self, record: &WalRecord) -> Result<Schema>
    pub fn record_to_arrow(&self, record: &WalRecord, schema: &Schema) -> Result<RecordBatch>
}

// wal_consumer/src/write_operations.rs
pub struct WriteOperationHandler {
    iceberg: Arc<IcebergWriter>,
}

impl WriteOperationHandler {
    pub async fn handle(&self, record: &WalRecord) -> Result<()>
    async fn handle_insert(&self, table: &Table, record: &WalRecord) -> Result<()>
    async fn handle_update(&self, table: &Table, record: &WalRecord) -> Result<()>
    async fn handle_delete(&self, table: &Table, record: &WalRecord) -> Result<()>
    async fn handle_truncate(&self, table: &Table) -> Result<()>
}

// wal_consumer/src/primary_key_resolver.rs
pub struct PrimaryKeyResolver {
    config: HashMap<(String, String), Vec<String>>,
}

impl PrimaryKeyResolver {
    pub fn get_keys(&self, schema: &str, table: &str) -> Option<&[String]>
    pub fn extract_key_values(&self, record: &WalRecord) -> Result<HashMap<String, Value>>
}
```

### Updated Files

```rust
// wal_consumer/src/main.rs
// Add iceberg module
mod iceberg;
mod schema_resolver;
mod write_operations;
mod primary_key_resolver;

// Update process_message to route to IcebergWriter
async fn process_message(record: &WalRecord) -> Result<()> {
    iceberg_writer.write_record(record).await
}

// wal_consumer/Cargo.toml
// Add dependencies:
iceberg = "0.1"
arrow = "50.0"
parquet = "50.0"
aws-config = "1.0"
aws-sdk-s3 = "1.0"
aws-smithy-runtime = "1.0"
```

---

## CONFIGURATION

### Environment Variables

```bash
# Catalog Type: "glue", "rest", or "file" (for local testing)
WAL_CONSUMER_ICEBERG_CATALOG_TYPE=glue

# For Glue Catalog (AWS)
WAL_CONSUMER_ICEBERG_AWS_REGION=us-east-1
WAL_CONSUMER_ICEBERG_GLUE_CATALOG_ID=123456789

# For REST Catalog (Open Source Iceberg)
WAL_CONSUMER_ICEBERG_REST_URL=http://iceberg-rest:8181

# For File Catalog (Local Testing)
WAL_CONSUMER_ICEBERG_WAREHOUSE=/tmp/iceberg-warehouse

# S3 Backend
WAL_CONSUMER_ICEBERG_S3_BUCKET=my-data-lake
WAL_CONSUMER_ICEBERG_S3_PREFIX=cdc/
WAL_CONSUMER_ICEBERG_S3_REGION=us-east-1

# Namespace (Iceberg database)
WAL_CONSUMER_ICEBERG_NAMESPACE=cdc

# Primary Keys (per table)
WAL_CONSUMER_PRIMARY_KEYS=public.transactions:id,public.users:user_id,public.subscriptions:subscription_id

# Batching
WAL_CONSUMER_BATCH_SIZE=5000
WAL_CONSUMER_BATCH_FLUSH_INTERVAL_SECS=30
WAL_CONSUMER_MAX_BATCH_MEMORY_MB=100

# Delete Mode: "physical" or "logical"
WAL_CONSUMER_DELETE_MODE=logical

# Parallel Workers
WAL_CONSUMER_WORKERS=4

# Offset Commit
WAL_CONSUMER_COMMIT_EVERY=5000  # Commit offset every N records

# DLQ
WAL_CONSUMER_DLQ_ENABLED=true
```

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: wal-consumer-config
  namespace: cdc
data:
  WAL_CONSUMER_ICEBERG_CATALOG_TYPE: glue
  WAL_CONSUMER_ICEBERG_AWS_REGION: us-east-1
  WAL_CONSUMER_ICEBERG_GLUE_CATALOG_ID: "123456789"
  WAL_CONSUMER_ICEBERG_S3_BUCKET: my-data-lake
  WAL_CONSUMER_ICEBERG_S3_PREFIX: cdc/
  WAL_CONSUMER_ICEBERG_NAMESPACE: cdc
  WAL_CONSUMER_PRIMARY_KEYS: |
    public.transactions:id
    public.users:user_id
    public.subscriptions:subscription_id
    public.offers:offer_id
  WAL_CONSUMER_BATCH_SIZE: "5000"
  WAL_CONSUMER_BATCH_FLUSH_INTERVAL_SECS: "30"
  WAL_CONSUMER_DELETE_MODE: logical
  WAL_CONSUMER_WORKERS: "4"
  WAL_CONSUMER_COMMIT_EVERY: "5000"
  WAL_CONSUMER_DLQ_ENABLED: "true"
```

---

## TESTING STRATEGY

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_resolution() {
        let record = WalRecord {
            table_schema: "public".to_string(),
            table_name: "transactions".to_string(),
            operation: Operation::Insert,
            new_tuple: Some(RowData {
                columns: vec![
                    Column { name: "id".into(), type_oid: 23, value: "TXN123".into(), is_null: false },
                    Column { name: "amount".into(), type_oid: 701, value: "1000.50".into(), is_null: false },
                ],
            }),
            ..Default::default()
        };

        let schema = SchemaResolver::resolve(&record).unwrap();
        assert_eq!(schema.fields.len(), 2);
    }

    #[test]
    fn test_primary_key_extraction() {
        let pk_resolver = PrimaryKeyResolver::new(
            "public.transactions:id"
        );
        let keys = pk_resolver.get_keys("public", "transactions").unwrap();
        assert_eq!(keys, vec!["id"]);
    }

    #[tokio::test]
    async fn test_insert_idempotency() {
        // Write batch 1
        writer.write_batch(&batch1).await.unwrap();
        
        // Re-write batch 1 (same LSN)
        writer.write_batch(&batch1).await.unwrap();
        
        // Verify row count unchanged (idempotent)
        let count = table.scan().count().await.unwrap();
        assert_eq!(count, batch1.len());
    }
}
```

### Integration Tests (Docker Compose)

```bash
# Start LocalStack + MinIO + Iceberg REST
docker-compose -f docker-compose.iceberg.yml up -d

# Run E2E test
cargo test --test iceberg_integration -- --nocapture

# Verify data in S3
aws s3 ls s3://my-data-lake/cdc/ --recursive
```

### Performance Benchmarks

```bash
# Generate 5 minutes of CDC records at 50k/sec
# Measure: throughput, latency, CPU, memory
cargo bench --bench iceberg_write -- --sample-size 1000
```

---

## CLOUD DEPLOYMENT (AWS)

### Prerequisites

1. **S3 Bucket** for Iceberg tables
   ```bash
   aws s3 mb s3://my-data-lake-cdc
   aws s3api put-bucket-versioning --bucket my-data-lake-cdc --versioning-configuration Status=Enabled
   ```

2. **Glue Catalog** (AWS Iceberg native)
   ```bash
   aws glue create-catalog --catalog-name iceberg-cdc
   ```

3. **IAM Role** for wal_consumer pod (EKS IRSA)
   ```bash
   eksctl create iamserviceaccount \
     --cluster my-cluster \
     --namespace cdc \
     --name wal-consumer \
     --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
     --attach-policy-arn arn:aws:iam::aws:policy/AWSGlueFullAccess
   ```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: wal-consumer
  namespace: cdc
spec:
  replicas: 2
  selector:
    matchLabels:
      app: wal-consumer
  template:
    metadata:
      labels:
        app: wal-consumer
    spec:
      serviceAccountName: wal-consumer
      containers:
      - name: wal-consumer
        image: wal-consumer:latest
        envFrom:
        - configMapRef:
            name: wal-consumer-config
        - secretRef:
            name: wal-consumer-secrets
        resources:
          requests:
            cpu: 1000m
            memory: 1Gi
          limits:
            cpu: 4000m
            memory: 4Gi
        livenessProbe:
          httpGet:
            path: /health
            port: 9091
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 9091
          periodSeconds: 10
```

---

## PERFORMANCE TARGETS & OPTIMIZATION

| Metric | Baseline | Target | Optimizations |
|--------|----------|--------|---|
| Throughput | 10k rec/s | 50k rec/s | Parallel tables, batch coalescing, Arrow optimization |
| Latency (p99) | 5s | 1s | Reduce flush interval, pre-allocated buffers |
| CPU per pod | 500m | 1000m | Acceptable at 50k/s with 4 workers |
| Memory | 512Mi | 2Gi | Batch buffering, schema cache |
| S3 Requests | TBD | <1000/s | Batch writes, partition pruning |

### Optimization Techniques

1. **Column Pruning:** Only write columns present in original table
2. **Batch Coalescing:** Group same-table operations before write
3. **Arrow Reuse:** Pre-allocate buffers, reuse across batches
4. **Schema Caching:** Local cache with TTL, reduce catalog calls
5. **Partition Pruning:** Route to correct partition by date
6. **Bloom Filters:** Iceberg native support for fast lookups

---

## MONITORING & OBSERVABILITY

### Prometheus Metrics

```
wal_consumer_records_total{table="public_transactions", operation="insert"}
wal_consumer_records_total{table="public_transactions", operation="update"}
wal_consumer_records_total{table="public_transactions", operation="delete"}

wal_consumer_iceberg_write_duration_seconds{table="public_transactions"}
wal_consumer_schema_cache_hits_total
wal_consumer_schema_cache_misses_total

wal_consumer_kafka_lag_records{topic="cdc.public.transactions"}
wal_consumer_dlq_records_total{source_table="public.transactions"}
```

### Grafana Dashboard

- [ ] Records/sec by table (stacked area chart)
- [ ] Write latency percentiles (graph + heatmap)
- [ ] Schema cache hit rate (gauge)
- [ ] Kafka consumer lag (graph + alerts)
- [ ] DLQ records by error type (pie chart)
- [ ] S3 request rate + errors (combined graph)

### Alerts

```yaml
- alert: IcebergWriteLagHigh
  expr: wal_consumer_kafka_lag_records > 100000
  for: 5m
  annotations:
    summary: "Iceberg writer falling behind (>100k lag)"

- alert: DLQRecordsPiling
  expr: rate(wal_consumer_dlq_records_total[5m]) > 100
  for: 2m
  annotations:
    summary: "DLQ receiving >100 records/sec, possible schema issues"

- alert: SchemaResolutionErrors
  expr: rate(wal_consumer_schema_resolution_errors_total[5m]) > 10
  for: 1m
  annotations:
    summary: "Schema resolution failing >10 times/sec"
```

---

## ROLLOUT PLAN

### Phase 3a: Single Table (Week 1)
- [ ] Deploy wal_consumer with Iceberg writer
- [ ] Monitor `cdc.public.transactions` table
- [ ] Verify data integrity (row counts, checksums)
- [ ] Run for 7 days without issues

### Phase 3b: Multi-Table (Week 2)
- [ ] Enable all tables
- [ ] Test cross-table referential integrity
- [ ] Verify foreign key constraints in queries
- [ ] Run for 7 days

### Phase 3c: Production Cutover (Week 3)
- [ ] Enable analytics queries (Athena, Spark)
- [ ] Validate reports match PostgreSQL source
- [ ] Decommission legacy ETL pipelines
- [ ] Full production launch

---

## SUCCESS CRITERIA

✅ **Functional Requirements**
- [ ] All CDC operations (I/U/D/T) correctly reflected in Iceberg
- [ ] No data loss (row count verified)
- [ ] Idempotent writes (no duplicate rows on replay)
- [ ] Schema evolution supported (new columns don't break writes)

✅ **Performance Requirements**
- [ ] Throughput: ≥50k records/sec sustained
- [ ] Latency: p99 <1 second
- [ ] CPU: <1000m per pod @ 50k/s
- [ ] Memory: <2Gi per pod

✅ **Reliability Requirements**
- [ ] 99.9% uptime (SLA)
- [ ] RPO: <1 minute (Kafka retention)
- [ ] RTO: <5 minutes (pod restart → resume)
- [ ] No silent data loss (DLQ captures all errors)

---

## CONCLUSION

Phase 3 is a **straightforward engineering effort** with clear milestones and well-defined interfaces. The foundation (Phases 1 & 2) is solid; now we build the final step of the pipeline.

**Next:** Begin Stage 1 implementation with Iceberg client setup and schema resolution. Estimated effort: 2-3 weeks for full production deployment.
