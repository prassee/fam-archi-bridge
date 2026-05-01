use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

pub struct Metrics {
    pub wal_bytes_received: AtomicU64,
    pub wal_records_parsed: AtomicU64,
    pub batches_sent: AtomicU64,
    pub parsing_errors: AtomicU64,
    pub kafka_messages_sent: AtomicU64,
    pub kafka_send_errors: AtomicU64,
    pub last_lsn: AtomicU64,
    #[allow(dead_code)]
    table_metrics: RwLock<std::collections::HashMap<String, Arc<TableMetrics>>>,
}

pub struct TableMetrics {
    pub insert_count: AtomicU64,
    pub update_count: AtomicU64,
    pub delete_count: AtomicU64,
    #[allow(dead_code)]
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
            table_metrics: RwLock::new(std::collections::HashMap::new()),
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

    #[allow(dead_code)]
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