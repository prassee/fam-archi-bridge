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
            fs::write(path, content).await?;
        }
        Ok(())
    }

    pub async fn get_last_lsn(&self, topic: &str) -> Option<String> {
        let state = self.state.read().await;
        state.get(topic).map(|s| s.last_lsn.clone())
    }
}

/// Represents a batch of CDC records pending publish to Kafka
/// After successful Kafka publish, all LSNs in this batch are acknowledged together
#[derive(Clone, Debug)]
pub struct PendingBatch {
    pub records: Vec<WalRecord>,
    pub lsns: Vec<u64>,
    pub max_lsn: u64,
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
}

impl BatchQueue {
    pub fn new(max_pending_batches: usize) -> Self {
        Self {
            pending: Arc::new(RwLock::new(VecDeque::new())),
            max_pending_batches,
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
                    return Ok(wait_iterations);
                }
            }

            wait_iterations += 1;
            sleep(Duration::from_millis(wait_step_ms.max(1))).await;
        }
    }

    pub async fn enqueue_front(&self, batch: PendingBatch) -> anyhow::Result<()> {
        let mut queue = self.pending.write().await;
        if queue.len() >= self.max_pending_batches {
            return Err(anyhow::anyhow!(
                "Pending batch queue full: {} batches awaiting Kafka publish",
                queue.len()
            ));
        }
        queue.push_front(batch);
        Ok(())
    }

    pub async fn dequeue(&self) -> Option<PendingBatch> {
        let mut queue = self.pending.write().await;
        queue.pop_front()
    }

    pub async fn count(&self) -> usize {
        self.pending.read().await.len()
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
