#![allow(dead_code)]
use crate::wal_parser::WalRecord;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::sync::RwLock;
use tokio::time::sleep;

/// FNV-1a 64-bit hash — deterministic across Rust versions and process restarts.
/// Unlike DefaultHasher, this guarantees stable topic→publisher routing.
#[inline]
fn fnv1a_hash(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[derive(Clone)]
pub struct LsnTracker {
    state: Arc<RwLock<HashMap<String, LsnState>>>,
    store_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LsnState {
    pub last_lsn: String,
    pub last_commit_time: i64,
}

impl LsnTracker {
    pub fn new(store_path: Option<PathBuf>) -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
            store_path,
        }
    }

    pub async fn load(&self) -> anyhow::Result<()> {
        if let Some(path) = &self.store_path {
            if path.exists() {
                let content = fs::read_to_string(path).await?;
                let states: HashMap<String, LsnState> = serde_json::from_str(&content)?;
                let mut state = self.state.write().await;
                *state = states;
            }
        }
        Ok(())
    }

    pub async fn persist(&self, topic: &str, lsn: &str) -> anyhow::Result<()> {
        {
            let mut state = self.state.write().await;
            state.insert(
                topic.to_string(),
                LsnState {
                    last_lsn: lsn.to_string(),
                    last_commit_time: chrono::Utc::now().timestamp(),
                },
            );
        }

        if let Some(path) = &self.store_path {
            let state = self.state.read().await;
            let content = serde_json::to_string(&*state)?;
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await?;
            }
            // Write atomically: write to .tmp then rename to avoid a corrupt file
            // on crash between truncate and write.
            let tmp_path = path.with_extension("tmp");
            fs::write(&tmp_path, &content).await?;
            fs::rename(&tmp_path, path).await?;
        }
        Ok(())
    }

    pub async fn get_last_lsn(&self, topic: &str) -> Option<String> {
        let state = self.state.read().await;
        state.get(topic).map(|s| s.last_lsn.clone())
    }
}

/// Represents a batch of CDC records pending publish to Kafka.
/// `wal_end` is the WAL position of the XLogData message these records came from.
/// After successful Kafka publish the replication loop decrements the fence for
/// this `wal_end` and advances `confirmed_flush_lsn` only once all batches from
/// that WAL position have been delivered.
#[derive(Clone, Debug)]
pub struct PendingBatch {
    pub records: Vec<WalRecord>,
    /// WAL end position of the XLogData message that produced these records.
    pub wal_end: u64,
    pub created_at: Instant,
    pub topic: String,
}

/// Configuration for batch aggregation
#[derive(Clone, Debug)]
pub struct BatchConfig {
    /// Max records per batch before triggering Kafka publish
    pub max_records_per_batch: usize,
    /// Max time to wait before flushing a partial batch (milliseconds)
    pub flush_interval_ms: u64,
}

/// Thread-safe queue for batches awaiting Kafka publish
/// Prevents buffering too many batches in memory
#[derive(Clone)]
pub struct BatchQueue {
    pending: Arc<RwLock<VecDeque<PendingBatch>>>,
    max_pending_batches: usize,
    /// O(1) length counter — avoids acquiring the RwLock just to read size.
    count: Arc<AtomicU64>,
}

impl BatchQueue {
    pub fn new(max_pending_batches: usize) -> Self {
        Self {
            pending: Arc::new(RwLock::new(VecDeque::new())),
            max_pending_batches,
            count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn enqueue(&self, batch: PendingBatch) -> anyhow::Result<()> {
        let mut queue = self.pending.write().await;
        if queue.len() >= self.max_pending_batches {
            return Err(anyhow::anyhow!(
                "Pending batch queue full: {} batches awaiting Kafka publish",
                queue.len()
            ));
        }
        queue.push_back(batch);
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Backpressure enqueue: waits until there is capacity instead of failing fast.
    /// Returns how many wait iterations were needed before enqueue succeeded.
    pub async fn enqueue_with_backpressure(
        &self,
        batch: PendingBatch,
        wait_step_ms: u64,
    ) -> anyhow::Result<u64> {
        let mut wait_iterations = 0u64;

        loop {
            {
                let mut queue = self.pending.write().await;
                if queue.len() < self.max_pending_batches {
                    queue.push_back(batch);
                    self.count.fetch_add(1, Ordering::Relaxed);
                    return Ok(wait_iterations);
                }
            }

            wait_iterations += 1;
            sleep(Duration::from_millis(wait_step_ms.max(1))).await;
        }
    }

    /// Re-enqueue a batch at the front for retry (e.g. transient Kafka error).
    /// Does NOT enforce capacity — this is existing data, not new data.
    pub async fn enqueue_front(&self, batch: PendingBatch) {
        let mut queue = self.pending.write().await;
        queue.push_front(batch);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn dequeue(&self) -> Option<PendingBatch> {
        let mut queue = self.pending.write().await;
        let batch = queue.pop_front();
        if batch.is_some() {
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
        batch
    }

    /// O(1) queue length — reads an atomic counter instead of locking the deque.
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed) as usize
    }
}

/// Multi-queue router that maintains per-topic causal ordering while enabling parallel publishers.
/// Each topic is deterministically hashed to a specific queue (0..num_publishers), ensuring all
/// batches for the same table always go to the same publisher regardless of Rust version.
#[derive(Clone)]
pub struct BatchQueueRouter {
    queues: Vec<BatchQueue>,
    num_publishers: usize,
}

impl BatchQueueRouter {
    pub fn new(num_publishers: usize, max_pending_per_queue: usize) -> Self {
        let queues = (0..num_publishers)
            .map(|_| BatchQueue::new(max_pending_per_queue))
            .collect();

        Self {
            queues,
            num_publishers,
        }
    }

    /// Deterministically map a topic to a publisher queue using FNV-1a.
    fn topic_to_queue_index(&self, topic: &str) -> usize {
        fnv1a_hash(topic) as usize % self.num_publishers
    }

    /// Enqueue batch to the queue assigned for its topic
    pub async fn enqueue(&self, batch: PendingBatch) -> anyhow::Result<()> {
        let queue_idx = self.topic_to_queue_index(&batch.topic);
        self.queues[queue_idx].enqueue(batch).await
    }

    /// Enqueue with backpressure to the queue assigned for its topic
    pub async fn enqueue_with_backpressure(
        &self,
        batch: PendingBatch,
        wait_step_ms: u64,
    ) -> anyhow::Result<u64> {
        let queue_idx = self.topic_to_queue_index(&batch.topic);
        self.queues[queue_idx]
            .enqueue_with_backpressure(batch, wait_step_ms)
            .await
    }

    /// Get the queue for a specific publisher ID
    pub fn get_queue(&self, publisher_id: usize) -> BatchQueue {
        self.queues[publisher_id].clone()
    }

    /// O(num_publishers) lock-free total count. Sums per-queue AtomicU64 counters —
    /// much cheaper than the previous O(n) RwLock acquisitions.
    pub fn total_count(&self) -> usize {
        self.queues.iter().map(|q| q.count()).sum()
    }
}

pub struct Metrics {
    pub wal_bytes_received: AtomicU64,
    pub wal_records_parsed: AtomicU64,
    pub batches_sent: AtomicU64,
    pub parsing_errors: AtomicU64,
    pub kafka_messages_sent: AtomicU64,
    pub kafka_send_errors: AtomicU64,
    pub last_lsn: AtomicU64,
    pub table_metrics: RwLock<HashMap<String, Arc<TableMetrics>>>,
}

pub struct TableMetrics {
    pub insert_count: AtomicU64,
    pub update_count: AtomicU64,
    pub delete_count: AtomicU64,
    pub lag_ms: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            wal_bytes_received: AtomicU64::new(0),
            wal_records_parsed: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            parsing_errors: AtomicU64::new(0),
            kafka_messages_sent: AtomicU64::new(0),
            kafka_send_errors: AtomicU64::new(0),
            last_lsn: AtomicU64::new(0),
            table_metrics: RwLock::new(HashMap::new()),
        }
    }

    pub fn inc_by(&self, val: u64) {
        self.wal_bytes_received.fetch_add(val, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn wal_bytes_received(&self) -> u64 {
        self.wal_bytes_received.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub fn wal_records_parsed(&self) -> u64 {
        self.wal_records_parsed.load(Ordering::Relaxed)
    }

    pub fn wal_records_parsed_inc(&self, val: u64) {
        self.wal_records_parsed.fetch_add(val, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn batches_sent(&self) -> u64 {
        self.batches_sent.load(Ordering::Relaxed)
    }

    pub fn batches_sent_inc(&self) {
        self.batches_sent.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn parsing_errors(&self) -> u64 {
        self.parsing_errors.load(Ordering::Relaxed)
    }

    pub fn parsing_errors_inc(&self) {
        self.parsing_errors.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn kafka_messages_sent(&self) -> u64 {
        self.kafka_messages_sent.load(Ordering::Relaxed)
    }

    pub fn kafka_messages_sent_inc(&self) {
        self.kafka_messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn kafka_send_errors(&self) -> u64 {
        self.kafka_send_errors.load(Ordering::Relaxed)
    }

    pub fn kafka_send_errors_inc(&self) {
        self.kafka_send_errors.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn.load(Ordering::Relaxed)
    }

    pub fn set_last_lsn(&self, lsn: u64) {
        self.last_lsn.store(lsn, Ordering::Relaxed);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Metrics {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl TableMetrics {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            insert_count: AtomicU64::new(0),
            update_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
            lag_ms: AtomicU64::new(0),
        }
    }

    #[allow(dead_code)]
    pub fn insert_inc(&self) {
        self.insert_count.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn update_inc(&self) {
        self.update_count.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn delete_inc(&self) {
        self.delete_count.fetch_add(1, Ordering::Relaxed);
    }
}
