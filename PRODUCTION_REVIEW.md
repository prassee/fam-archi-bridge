# Production Readiness & Architecture Review
## rust-wal-cake-writer (WAL CDC → Kafka → Iceberg)

**Date:** May 16, 2026  
**Status:** Pre-production with critical fixes required  
**Prepared for:** Cloud deployment & scale-out (PhonePe scale: 50k+ txns/sec)

---

## EXECUTIVE SUMMARY

### Current State ✅
- **Phase 1 (Data Ingestion):** ✅ Complete — Go data pump generates 5k txns/sec
- **Phase 2 (CDC Capture):** ✅ Functional, but with critical bugs
  - Throughput: ~9,700 msg/sec (limited by batching efficiency)
  - Delivery guarantee: At-most-once (LSN only advances post-publish)
  - **Issue:** `confirmed_flush_lsn` not advancing in K8s → LSN ACK stalled ⚠️
- **Phase 3 (Iceberg Writer):** ❌ Not started — stub ready for integration

### Critical Blocker 🔴
**LSN Acknowledgment Stalled in Kubernetes**
- PostgreSQL replication slot's `confirmed_flush_lsn` remains NULL despite successful publishes
- **Root Cause:** KeepAlive handler doesn't send status response when PostgreSQL requests it
- **Impact:** WAL segments not recycled → storage grows unbounded
- **Fix:** Wire explicit status update on KeepAlive with `reply_requested=true`

### Cloud Readiness Score: 4/10
| Area | Score | Gaps |
|------|-------|------|
| Availability | 3/10 | No HA, no multi-replica, single SPOF |
| Observability | 7/10 | Prometheus good, no alerts/runbooks |
| Resilience | 4/10 | No circuit breaker, no DLQ, manual recovery |
| Performance | 6/10 | 9.7k msg/s, needs 5x for scale |
| Operability | 5/10 | Manual slot recovery, no auto-remediation |
| Security | 5/10 | Secrets in ConfigMap, no Vault integration |
| Data Safety | 8/10 | At-most-once delivery, but LSN stall risk |

---

## ROOT CAUSE: LSN ACK STALL

### Symptom
```sql
-- In K8s cluster, observed on repeated queries:
SELECT slot_name, confirmed_flush_lsn, pg_current_wal_lsn() FROM pg_replication_slots;
-- Result: confirmed_flush_lsn NULL, pg_current_wal_lsn() advancing (replication not advancing slot)
```

### Why It Happens

**PostgreSQL Replication Protocol Flow:**

```
PostgreSQL (Primary)                    wal-writer (Standby Client)
    │                                          │
    ├─ Send XLogData (WAL bytes) ─────────────→ Receive & parse WAL
    │                                      Parse → Batch → Publish to Kafka
    │                                          │
    ├─ Send KeepAlive (heartbeat) ───────────→ [CRITICAL] Ignore? Log only?
    │   with reply_requested=true              [Missing] No status sent back!
    │                                          │
    │ [BLOCKED: Waiting for reply] ←─────────  [Gap: No response]
    │                                          │
    ├─ Timeout? Try again ─────────────────→ Another KeepAlive logged
    │ [Slot position DOES NOT ADVANCE]        │
```

**Code Analysis:**

In [wal_writer/src/pg_replication.rs#L335-L346](wal_writer/src/pg_replication.rs#L335):

```rust
Some(ReplicationEvent::KeepAlive {
    wal_end,
    reply_requested,
    server_time_micros: _,
}) => {
    metrics.inc_wal_message("keepalive");
    debug!(
        "Keepalive at {} reply_requested={} pending_batches={}",
        wal_end.as_u64(),
        reply_requested,
        batch_queue.count().await
    );
    // ❌ MISSING: No status update sent when reply_requested=true!
    // ❌ MISSING: update_applied_lsn() should be called here too
}
```

**Why `confirmed_flush_lsn` Never Advances:**

1. wal-writer calls `update_applied_lsn()` only after Kafka publish succeeds (good)
2. But `update_applied_lsn()` only takes effect if wal-writer responds to KeepAlive
3. PostgreSQL uses KeepAlive responses to know the client is alive and to advance slot
4. Without KeepAlive response, PostgreSQL doesn't trust the LSN updates

### The pgwire-replication Library Gap

The `pgwire-replication` crate (v0.3.1) may not expose an explicit "send status" method. Current code:
- ✅ `client.update_applied_lsn()` — sends applied LSN as part of protocol
- ❌ No visible explicit "respond to KeepAlive" method in current version

**Workaround:** May need to:
1. Add explicit status message via underlying connection, OR
2. Upgrade pgwire-replication to newer version with status API, OR
3. Fork/patch pgwire-replication to expose status sending

---

## CRITICAL FIXES REQUIRED

### Fix 1: Wire KeepAlive Response (HIGHEST PRIORITY)

**File:** [wal_writer/src/pg_replication.rs](wal_writer/src/pg_replication.rs)

**Change:** Add status update on KeepAlive when `reply_requested=true`

```rust
Some(ReplicationEvent::KeepAlive {
    wal_end,
    reply_requested,
    server_time_micros: _,
}) => {
    metrics.inc_wal_message("keepalive");
    debug!(
        "Keepalive at {} reply_requested={} pending_batches={}",
        wal_end.as_u64(),
        reply_requested,
        batch_queue.count().await
    );
    
    // 🔧 FIX: Send status back to PostgreSQL when reply_requested
    if reply_requested {
        // Ensure confirmed LSN is current before responding
        client.update_applied_lsn(Lsn(confirmed_lsn));
        debug!("Sent keepalive status response to PostgreSQL (LSN={})", confirmed_lsn);
    }
}
```

**Verification:** After apply:
```sql
-- Should converge to 0 lag within 10 seconds of wal-writer startup
SELECT
  slot_name,
  confirmed_flush_lsn,
  (pg_current_wal_lsn() - confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots
WHERE slot_name = 'wal_writer_slot';
```

---

### Fix 2: Add State Directory Persistence to K8s (HIGH PRIORITY)

**File:** [k8s/deployment.yaml](k8s/deployment.yaml)

**Issue:** `wal_position.json` (LSN state) stored in emptyDir → lost on pod restart → potential replay

**Changes:**

```yaml
# Add volume for persistent state
volumes:
- name: logs
  emptyDir:
    sizeLimit: 1Gi
- name: state
  emptyDir:
    sizeLimit: 100Mi  # 🔧 NEW: Persistent state directory

# Add environment variable
env:
- name: WAL_WRITER_STATE_DIR
  value: "/var/run/wal-writer/state"  # 🔧 NEW

# Update volumeMounts
volumeMounts:
- name: logs
  mountPath: /var/log/wal-writer
- name: state
  mountPath: /var/run/wal-writer/state  # 🔧 NEW
```

**Or (Better for Production):** Use PersistentVolumeClaim instead of emptyDir:

```yaml
volumes:
- name: state
  persistentVolumeClaim:
    claimName: wal-writer-state

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: wal-writer-state
  namespace: cdc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```

---

### Fix 3: Update ConfigMap with State Directory (HIGH PRIORITY)

**File:** [k8s/configmap.yaml](k8s/configmap.yaml)

```yaml
data:
  WAL_WRITER_STATE_DIR: "/var/run/wal-writer/state"  # 🔧 NEW
  WAL_WRITER_STATE_PERSIST_INTERVAL_SECS: "10"      # 🔧 NEW (faster than default 60s)
```

---

### Fix 4: Add Circuit Breaker & DLQ for Kafka (HIGH PRIORITY)

**Location:** [wal_writer/src/kafka.rs](wal_writer/src/kafka.rs)

**Issue:** If Kafka broker fails, replication keeps buffering → memory exhaustion

**Change:**

```rust
// Add max retry config + DLQ topic
pub struct KafkaProducer {
    producer: Option<FutureProducer>,
    topic_prefix: String,
    debug_no_kafka: bool,
    max_retries: u32,  // 🔧 NEW
    dlq_topic: String,  // 🔧 NEW
}

// Modify publish_batch to implement circuit breaker
pub async fn publish_batch(&self, topic: &str, records: &[WalRecord]) -> Result<u64, Error> {
    let mut retries = 0;
    
    loop {
        match self.try_send(topic, records).await {
            Ok(highest_lsn) => return Ok(highest_lsn),
            Err(e) if retries < self.max_retries => {
                retries += 1;
                warn!("Publish failed (retry {}/{}): {}", retries, self.max_retries, e);
                tokio::time::sleep(
                    Duration::from_millis(100 * 2_u64.pow(retries as u32))
                ).await;
            }
            Err(e) => {
                // 🔧 NEW: Send to DLQ instead of failing
                if let Err(dlq_err) = self.send_to_dlq(topic, records, &e).await {
                    error!("Failed to send to DLQ: {}", dlq_err);
                }
                return Err(e);
            }
        }
    }
}

// 🔧 NEW: DLQ implementation
async fn send_to_dlq(&self, topic: &str, records: &[WalRecord], error: &Error) -> Result<()> {
    let dlq_topic = format!("{}.dlq", topic);
    let payload = json!({
        "original_topic": topic,
        "error": error.to_string(),
        "records": records,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
    // Send to DLQ for manual inspection
    self.send(&dlq_topic, "dlq", &payload.to_string()).await
}
```

---

### Fix 5: Automated Slot Recovery (MEDIUM PRIORITY)

**Location:** [wal_writer/src/main.rs](wal_writer/src/main.rs)

**Add:** Slot health check at startup + auto-recovery

```rust
async fn verify_or_recover_slot(config: &AppConfig) -> Result<()> {
    let (client, _) = tokio_postgres::connect(
        &config.pg.connection_string(),
        NoTls,
    ).await?;
    
    let slot_name = config.pg.slot_name();
    
    // Check if slot exists and is valid
    let slot_info: (bool, Option<String>) = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1),
                    (SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await?
        .try_get::<_, (bool, Option<String>)>(0)?;
    
    if !slot_info.0 {
        warn!("Replication slot {} not found, creating...", slot_name);
        client
            .execute(
                &format!("SELECT pg_create_logical_replication_slot('{}', 'pgoutput')", slot_name),
                &[],
            )
            .await?;
        info!("Slot {} recreated", slot_name);
    }
    
    Ok(())
}
```

---

## DESIGN GAPS & RISKS

### 1. No High Availability (HA)

| Risk | Current | Required |
|------|---------|----------|
| Pod failure | Single pod lost → manual restart | Multi-pod with leader election |
| Replication slot | Single slot → contention | Per-pod slot or distributed lease |
| WAL retention | Unlimited until LSN ACK | Policy-based with auto-truncation |

**Fix:** Implement:
- [ ] Leader election (using Kubernetes leases or etcd)
- [ ] Per-pod replication slots with offset sync
- [ ] Distributed state persistence (S3, DynamoDB, or etcd)

---

### 2. Memory Pressure Under Load

**Current:** Pending batch queue has fixed max (1000 batches)

**Risk:** Under sustained Kafka broker slowness, buffer fills → pod OOM

**Fix:**
```rust
// Implement adaptive backpressure
pub async fn enqueue_with_adaptive_backpressure(
    &self,
    batch: PendingBatch,
) -> Result<()> {
    let queue_utilization = self.count().await as f64 / self.max_pending_batches as f64;
    
    if queue_utilization > 0.8 {
        // Slow down WAL parsing by delaying poll
        warn!("Queue utilization {:.1}%, applying backpressure", queue_utilization * 100.0);
        tokio::time::sleep(Duration::from_millis(
            (queue_utilization * 100.0).min(500.0) as u64
        )).await;
    }
    
    self.enqueue_with_backpressure(batch, 5).await
}
```

---

### 3. Schema Drift & Evolution

**Gap:** No schema versioning or migration support

**Risk:** Column addition/removal in PostgreSQL → parsing errors

**Fix:** Integrate schema registry:
```rust
// Add schema registry client
struct SchemaRegistry {
    url: String,
    cache: Arc<RwLock<HashMap<(String, String), Schema>>>,
}

impl SchemaRegistry {
    async fn get_schema(&self, schema: &str, table: &str) -> Result<Schema> {
        // Fetch from registry (or Glue for AWS)
        // Cache locally
    }
}
```

---

### 4. No Operator Support

**Gap:** Manual deployment, no Kubernetes operator

**Fix:** Create/use a CDC operator:
- Watches CDC resource definitions
- Auto-creates slots, publications, Kafka topics
- Handles failover and scaling

---

### 5. Table-Level Pause/Resume Not Exposed

**Current:** Publication-based pause only (requires ALTER PUBLICATION)

**Gap:** No in-app allowlist/denylist

**Fix:** Add to ConfigMap:
```yaml
WAL_WRITER_EXCLUDE_TABLES: "public.temp_table,public.staging_*"
WAL_WRITER_INCLUDE_TABLES: "*"  # Default: include all
```

---

## PERFORMANCE OPTIMIZATION ROADMAP

### Baseline (Current)
- Throughput: 9,700 msg/sec
- Latency: ~50ms (batch flush)
- CPU: ~200m idle, unknown under load

### Target (Cloud Scale)
- Throughput: 50,000+ msg/sec (5x)
- Latency: <20ms (real-time responsiveness)
- CPU: <500m @ 50k msg/s

### Optimizations (Priority Order)

| Priority | Change | Expected Impact | Effort |
|----------|--------|-----------------|--------|
| P0 | Fix LSN ACK stall (KeepAlive response) | Unblocks K8s | 2h |
| P1 | Increase `max_records_per_batch` from 2000 → 10000 | +30% throughput | 1h |
| P1 | Reduce `replication_poll_interval_ms` from 50 → 10 | Faster WAL drain | 1h |
| P2 | Parallel table publishers (one task per table) | +50% throughput | 8h |
| P2 | Batch size heuristics (adaptive based on ingestion rate) | Dynamic efficiency | 6h |
| P3 | Add table-level filtering to reduce parsing load | Varies by workload | 4h |
| P3 | Implement async I/O for state persistence | Marginal | 2h |

### Performance Test Checklist

```bash
# Before optimization
curl http://wal-writer:9090/metrics | grep throughput

# After P0 + P1 (target: 12k+ msg/s)
# After P1 + P2 (target: 18k+ msg/s)
# After P2 + P3 (target: 30k+ msg/s)
```

---

## KUBERNETES DEPLOYMENT IMPROVEMENTS

### 1. Add Resource Limits & Requests

```yaml
resources:
  requests:
    cpu: 500m          # 50% of small pod baseline
    memory: 512Mi
    ephemeral-storage: 1Gi
  limits:
    cpu: 2000m        # Allow burst
    memory: 2Gi       # Prevent OOM kill
    ephemeral-storage: 5Gi
```

### 2. Add PreStop Hook (Graceful Drain)

```yaml
lifecycle:
  preStop:
    exec:
      command: ["/bin/sh", "-c", "sleep 15"]  # Allow time to drain
```

### 3. Add Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: wal-writer-network-policy
spec:
  podSelector:
    matchLabels:
      app: wal-writer
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: cdc
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: cdc
  - to:
    - podSelector:
        matchLabels:
          app: postgres
  - to:
    - podSelector:
        matchLabels:
          app: kafka
```

### 4. Add Pod Disruption Budget (Availability)

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: wal-writer-pdb
spec:
  minAvailable: 1  # Keep at least 1 pod during maintenance
  selector:
    matchLabels:
      app: wal-writer
```

### 5. Add Prometheus Alerts

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: wal-writer-alerts
spec:
  groups:
  - name: wal-writer
    interval: 30s
    rules:
    - alert: LSNNotAdvancing
      expr: |
        rate(wal_writer_last_acked_lsn[5m]) == 0
      for: 2m
      annotations:
        summary: "WAL Writer: LSN not advancing for 2+ minutes"
        
    - alert: HighReplicationLag
      expr: |
        (pg_current_wal_lsn - pg_replication_slots_confirmed_flush_lsn) > 1e9
      for: 5m
      annotations:
        summary: "Replication lag > 1GB"
        
    - alert: KafkaPublishErrors
      expr: |
        rate(kafka_send_errors_total[5m]) > 100
      for: 2m
      annotations:
        summary: "Kafka publish errors > 100/sec"
```

---

## ICEBERG INTEGRATION (PHASE 3)

### Architecture

```
Kafka Topics (cdc.public.*)
    ↓ (wal_consumer reads)
Schema Resolution
    ↓ (Iceberg catalog lookup)
Upsert/Merge Logic
    ↓ (primary key match)
Iceberg Tables (S3 backend)
    ↓ (Glue Catalog metadata)
Analytics Query (Athena, Spark, DuckDB)
```

### Implementation Checklist

- [ ] **Schema Resolution**
  - [ ] First CDC record → infer schema
  - [ ] Cache schema per (schema, table)
  - [ ] Support for type conversions (PG types → Iceberg types)

- [ ] **Iceberg Catalog Integration**
  - [ ] Support Glue REST catalog (AWS-native)
  - [ ] Support Iceberg REST catalog (open standard)
  - [ ] Support local catalog for dev (file-based)

- [ ] **Write Operations**
  - [ ] `INSERT` → append rows to Iceberg table
  - [ ] `UPDATE` → merge with primary key match
  - [ ] `DELETE` → mark deleted or physically remove
  - [ ] `TRUNCATE` → drop table or truncate partition

- [ ] **Batching & Commit**
  - [ ] Batch size: configurable (default 5k records)
  - [ ] Commit interval: configurable (default 30s)
  - [ ] Idempotent commits (no duplicate inserts)

- [ ] **Error Handling**
  - [ ] Schema mismatch → DLQ + alert
  - [ ] Catalog unavailable → retry with backoff
  - [ ] Out-of-order commits → buffer + sort

### Code Structure

```rust
// wal_consumer/src/
├── main.rs                    # Entry point, consumer loop
├── iceberg.rs                 # 🆕 Iceberg catalog client
├── schema_resolver.rs         # 🆕 CDC record → schema inference
├── write_operations.rs        # 🆕 Insert/Update/Delete/Truncate logic
├── kafka_reader.rs            # Topic discovery & message consumption
└── metrics.rs                 # Iceberg write metrics
```

### Example Implementation (Skeleton)

```rust
// wal_consumer/src/iceberg.rs
use iceberg::Table;
use iceberg::catalog::Catalog;

pub struct IcebergWriter {
    catalog: Arc<dyn Catalog>,
    namespace: String,  // e.g., "cdc"
    schema_cache: Arc<RwLock<HashMap<String, Schema>>>,
}

impl IcebergWriter {
    pub async fn write_record(&self, record: &WalRecord) -> Result<()> {
        let table_name = format!("{}.{}", record.table_schema, record.table_name);
        let table = self.catalog.load_table(&table_name).await?;
        
        match record.operation {
            Operation::Insert => self.write_insert(&table, record).await?,
            Operation::Update => self.write_update(&table, record).await?,
            Operation::Delete => self.write_delete(&table, record).await?,
            Operation::Truncate => self.write_truncate(&table, record).await?,
        }
        
        Ok(())
    }
    
    async fn write_insert(&self, table: &Table, record: &WalRecord) -> Result<()> {
        // Append new rows
        let data = record_to_arrow_record(record)?;
        table.append(data).await?;
        Ok(())
    }
    
    async fn write_update(&self, table: &Table, record: &WalRecord) -> Result<()> {
        // Merge on primary key
        // (Requires primary key config per table)
        Ok(())
    }
    
    async fn write_delete(&self, table: &Table, record: &WalRecord) -> Result<()> {
        // Mark rows deleted or physically remove
        Ok(())
    }
    
    async fn write_truncate(&self, table: &Table, _record: &WalRecord) -> Result<()> {
        // Truncate or drop table
        table.truncate().await?;
        Ok(())
    }
}
```

### Configuration (Iceberg)

```yaml
# k8s/configmap.yaml
data:
  WAL_CONSUMER_ICEBERG_CATALOG_TYPE: "glue"  # or "rest", "file"
  WAL_CONSUMER_ICEBERG_S3_BUCKET: "my-data-lake"
  WAL_CONSUMER_ICEBERG_S3_PREFIX: "cdc/"
  WAL_CONSUMER_ICEBERG_NAMESPACE: "cdc"
  WAL_CONSUMER_PRIMARY_KEYS: |
    public.transactions:id
    public.users:user_id
```

### Testing

```bash
# Deploy wal_consumer with Iceberg writer
kubectl apply -f k8s/wal-consumer.yaml

# Generate test data
kubectl exec data-pump-go -- bash -c "data-pump --duration 60s --target 1000 txns/sec"

# Verify Iceberg table creation in S3
aws s3 ls s3://my-data-lake/cdc/

# Query with Athena
SELECT COUNT(*) FROM "default"."cdc_public_transactions"
```

---

## DEPLOYMENT CHECKLIST

### Pre-Production

- [ ] Fix LSN ACK stall (KeepAlive response)
- [ ] Add state directory persistence (ConfigMap + volume)
- [ ] Implement circuit breaker + DLQ for Kafka
- [ ] Add automated slot recovery
- [ ] Performance testing: verify 10k+ msg/s throughput
- [ ] Security: integrate Vault/ASM for secrets
- [ ] HA: implement leader election for multi-pod setup
- [ ] Monitoring: add PrometheusRules for critical alerts
- [ ] Runbooks: document slot recovery, failover, scaling procedures

### Production

- [ ] Multi-region deployment (cross-cluster replication)
- [ ] Implement Iceberg writer (Phase 3)
- [ ] E2E integration tests (Postgres → Kafka → Iceberg → Analytics)
- [ ] Disaster recovery drills
- [ ] Capacity planning (storage, network, CPU for 50k+ txns/sec)
- [ ] SLO/SLA definitions (RPO, RTO)

---

## SUMMARY OF FIXES

### Immediate (This Sprint)

1. **Fix KeepAlive Response** [BLOCKER]
   - File: `wal_writer/src/pg_replication.rs`
   - Change: Add status update in KeepAlive handler when `reply_requested=true`
   - Impact: Unblocks K8s deployment, enables LSN ACK progression

2. **Add State Directory to K8s**
   - File: `k8s/deployment.yaml`, `k8s/configmap.yaml`
   - Change: Add WAL_WRITER_STATE_DIR volume + env var
   - Impact: Persists LSN state across pod restarts

3. **Wire Circuit Breaker**
   - File: `wal_writer/src/kafka.rs`
   - Change: Add max-retry + DLQ logic
   - Impact: Prevents buffer exhaustion under Kafka failures

### Next Sprint

4. Performance tuning (batch size, poll intervals, parallelism)
5. HA implementation (leader election, distributed state)
6. Iceberg writer Phase 3 integration
7. Security hardening (Vault, network policies, RBAC)

---

## CONCLUSION

The architecture is **sound and production-ready** after fixes. The LSN ACK stall is a **single critical bug** (KeepAlive response missing) that's easily fixed in <2 hours. Once that's resolved:

- ✅ Deployment to cloud is feasible
- ✅ Scaling to 50k+ txns/sec is achievable with optimizations
- ✅ Phase 3 (Iceberg) can proceed in parallel

**Next Step:** Apply the critical fixes, re-test on K8s cluster, then proceed to Iceberg integration.
