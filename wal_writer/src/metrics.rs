use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::wal_parser::Operation;
use parking_lot::RwLock;

/// Histogram buckets for publish latency in seconds.
const PUBLISH_DURATION_BUCKETS: &[f64] =
    &[0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.5, 5.0, 10.0];

/// Inner metrics data, held behind an `Arc` so all clones share counters.
struct MetricsInner {
    ready: AtomicBool,
    replication_connected: AtomicBool,
    wal_bytes_received: AtomicU64,
    wal_records_parsed: AtomicU64,
    batches_sent: AtomicU64,
    parsing_errors: AtomicU64,
    kafka_messages_sent: AtomicU64,
    kafka_send_errors: AtomicU64,
    publish_failures: AtomicU64,
    publish_duration_count: AtomicU64,
    publish_duration_sum_ns: AtomicU64,
    publish_duration_buckets: [AtomicU64; 11],
    published_records_insert: AtomicU64,
    published_records_update: AtomicU64,
    published_records_delete: AtomicU64,
    published_records_truncate: AtomicU64,
    wal_messages_xlogdata: AtomicU64,
    wal_messages_begin: AtomicU64,
    wal_messages_commit: AtomicU64,
    wal_messages_keepalive: AtomicU64,
    wal_messages_message: AtomicU64,
    wal_messages_stopped: AtomicU64,
    last_receive_lsn: AtomicU64,
    last_process_lsn: AtomicU64,
    last_acked_lsn: AtomicU64,
    replication_loop_exits: AtomicU64,
    process_wal_errors: AtomicU64,
    #[allow(dead_code)]
    table_metrics: RwLock<std::collections::HashMap<String, Arc<TableMetrics>>>,
}

/// Cheaply cloneable handle to shared metrics counters.
#[derive(Clone)]
pub struct Metrics(Arc<MetricsInner>);

pub struct TableMetrics {
    pub insert_count: AtomicU64,
    pub update_count: AtomicU64,
    pub delete_count: AtomicU64,
    #[allow(dead_code)]
    pub lag_ms: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self(Arc::new(MetricsInner {
            ready: AtomicBool::new(true),
            replication_connected: AtomicBool::new(false),
            wal_bytes_received: AtomicU64::new(0),
            wal_records_parsed: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            parsing_errors: AtomicU64::new(0),
            kafka_messages_sent: AtomicU64::new(0),
            kafka_send_errors: AtomicU64::new(0),
            publish_failures: AtomicU64::new(0),
            publish_duration_count: AtomicU64::new(0),
            publish_duration_sum_ns: AtomicU64::new(0),
            publish_duration_buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            published_records_insert: AtomicU64::new(0),
            published_records_update: AtomicU64::new(0),
            published_records_delete: AtomicU64::new(0),
            published_records_truncate: AtomicU64::new(0),
            wal_messages_xlogdata: AtomicU64::new(0),
            wal_messages_begin: AtomicU64::new(0),
            wal_messages_commit: AtomicU64::new(0),
            wal_messages_keepalive: AtomicU64::new(0),
            wal_messages_message: AtomicU64::new(0),
            wal_messages_stopped: AtomicU64::new(0),
            last_receive_lsn: AtomicU64::new(0),
            last_process_lsn: AtomicU64::new(0),
            last_acked_lsn: AtomicU64::new(0),
            replication_loop_exits: AtomicU64::new(0),
            process_wal_errors: AtomicU64::new(0),
            table_metrics: RwLock::new(std::collections::HashMap::new()),
        }))
    }

    pub fn inc_by(&self, val: u64) {
        self.0.wal_bytes_received.fetch_add(val, Ordering::Relaxed);
    }

    pub fn set_replication_connected(&self, connected: bool) {
        self.0
            .replication_connected
            .store(connected, Ordering::Relaxed);
    }

    pub fn wal_records_parsed_inc(&self, val: u64) {
        self.0.wal_records_parsed.fetch_add(val, Ordering::Relaxed);
    }

    pub fn batches_sent_inc(&self) {
        self.0.batches_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn parsing_errors_inc(&self) {
        self.0.parsing_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn kafka_messages_sent_inc(&self) {
        self.0.kafka_messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn kafka_send_errors_inc(&self) {
        self.0.kafka_send_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn publish_failure_inc(&self) {
        self.0.publish_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_publish_duration(&self, duration: std::time::Duration) {
        let seconds = duration.as_secs_f64();
        self.0
            .publish_duration_sum_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.0
            .publish_duration_count
            .fetch_add(1, Ordering::Relaxed);

        // Increment only the FIRST (smallest) matching bucket.
        // The gather pass cumulates from small→large, so this produces correct
        // Prometheus cumulative histograms without double-counting.
        if let Some(idx) = PUBLISH_DURATION_BUCKETS.iter().position(|&b| seconds <= b) {
            self.0.publish_duration_buckets[idx].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn published_record_inc(&self, operation: Operation) {
        match operation {
            Operation::Insert => self
                .0
                .published_records_insert
                .fetch_add(1, Ordering::Relaxed),
            Operation::Update => self
                .0
                .published_records_update
                .fetch_add(1, Ordering::Relaxed),
            Operation::Delete => self
                .0
                .published_records_delete
                .fetch_add(1, Ordering::Relaxed),
            Operation::Truncate => self
                .0
                .published_records_truncate
                .fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn inc_wal_message(&self, message_type: &str) {
        match message_type {
            "xlogdata" => {
                self.0.wal_messages_xlogdata.fetch_add(1, Ordering::Relaxed);
            }
            "begin" => {
                self.0.wal_messages_begin.fetch_add(1, Ordering::Relaxed);
            }
            "commit" => {
                self.0.wal_messages_commit.fetch_add(1, Ordering::Relaxed);
            }
            "keepalive" => {
                self.0
                    .wal_messages_keepalive
                    .fetch_add(1, Ordering::Relaxed);
            }
            "message" => {
                self.0.wal_messages_message.fetch_add(1, Ordering::Relaxed);
            }
            "stopped" => {
                self.0.wal_messages_stopped.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub fn set_last_receive_lsn(&self, lsn: u64) {
        self.0.last_receive_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn set_last_process_lsn(&self, lsn: u64) {
        self.0.last_process_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn set_last_acked_lsn(&self, lsn: u64) {
        self.0.last_acked_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn inc_replication_loop_exits(&self) {
        self.0
            .replication_loop_exits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn process_wal_errors_inc(&self) {
        self.0.process_wal_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn gather_prometheus(&self) -> String {
        let mut output = String::with_capacity(2048);

        writeln!(
            output,
            "# HELP wal_writer_ready WAL writer process readiness"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_ready gauge").ok();
        writeln!(
            output,
            "wal_writer_ready {}",
            if self.0.ready.load(Ordering::Relaxed) {
                1
            } else {
                0
            }
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_replication_connected Replication client connectivity status"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_replication_connected gauge").ok();
        writeln!(
            output,
            "wal_writer_replication_connected {}",
            if self.0.replication_connected.load(Ordering::Relaxed) {
                1
            } else {
                0
            }
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_published_records_total Published CDC records by operation"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_published_records_total counter").ok();
        writeln!(
            output,
            "wal_writer_published_records_total{{operation=\"insert\"}} {}",
            self.0.published_records_insert.load(Ordering::Relaxed)
        )
        .ok();
        writeln!(
            output,
            "wal_writer_published_records_total{{operation=\"update\"}} {}",
            self.0.published_records_update.load(Ordering::Relaxed)
        )
        .ok();
        writeln!(
            output,
            "wal_writer_published_records_total{{operation=\"delete\"}} {}",
            self.0.published_records_delete.load(Ordering::Relaxed)
        )
        .ok();
        writeln!(
            output,
            "wal_writer_published_records_total{{operation=\"truncate\"}} {}",
            self.0.published_records_truncate.load(Ordering::Relaxed)
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_publish_failures_total Kafka publish failures"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_publish_failures_total counter").ok();
        writeln!(
            output,
            "wal_writer_publish_failures_total {}",
            self.0.publish_failures.load(Ordering::Relaxed)
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_kafka_messages_sent_total Kafka messages successfully sent"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_kafka_messages_sent_total counter").ok();
        writeln!(
            output,
            "wal_writer_kafka_messages_sent_total {}",
            self.0.kafka_messages_sent.load(Ordering::Relaxed)
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_kafka_send_errors_total Kafka send error count"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_kafka_send_errors_total counter").ok();
        writeln!(
            output,
            "wal_writer_kafka_send_errors_total {}",
            self.0.kafka_send_errors.load(Ordering::Relaxed)
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_publish_duration_seconds Publish duration histogram"
        )
        .ok();
        writeln!(
            output,
            "# TYPE wal_writer_publish_duration_seconds histogram"
        )
        .ok();
        let total_count = self.0.publish_duration_count.load(Ordering::Relaxed);
        let total_sum_ns = self.0.publish_duration_sum_ns.load(Ordering::Relaxed);
        let mut cumulative = 0;

        for (idx, bound) in PUBLISH_DURATION_BUCKETS.iter().enumerate() {
            cumulative += self.0.publish_duration_buckets[idx].load(Ordering::Relaxed) as u64;
            writeln!(
                output,
                "wal_writer_publish_duration_seconds_bucket{{le=\"{}\"}} {}",
                bound, cumulative
            )
            .ok();
        }
        writeln!(
            output,
            "wal_writer_publish_duration_seconds_bucket{{le=\"+Inf\"}} {}",
            total_count
        )
        .ok();
        writeln!(
            output,
            "wal_writer_publish_duration_seconds_sum {}",
            total_sum_ns as f64 / 1_000_000_000.0
        )
        .ok();
        writeln!(
            output,
            "wal_writer_publish_duration_seconds_count {}",
            total_count
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_wal_messages_total Replication event counts by type"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_wal_messages_total counter").ok();
        writeln!(
            output,
            "wal_writer_wal_messages_total{{type=\"xlogdata\"}} {}",
            self.0.wal_messages_xlogdata.load(Ordering::Relaxed)
        )
        .ok();
        writeln!(
            output,
            "wal_writer_wal_messages_total{{type=\"begin\"}} {}",
            self.0.wal_messages_begin.load(Ordering::Relaxed)
        )
        .ok();
        writeln!(
            output,
            "wal_writer_wal_messages_total{{type=\"commit\"}} {}",
            self.0.wal_messages_commit.load(Ordering::Relaxed)
        )
        .ok();
        writeln!(
            output,
            "wal_writer_wal_messages_total{{type=\"keepalive\"}} {}",
            self.0.wal_messages_keepalive.load(Ordering::Relaxed)
        )
        .ok();
        writeln!(
            output,
            "wal_writer_wal_messages_total{{type=\"message\"}} {}",
            self.0.wal_messages_message.load(Ordering::Relaxed)
        )
        .ok();
        writeln!(
            output,
            "wal_writer_wal_messages_total{{type=\"stopped\"}} {}",
            self.0.wal_messages_stopped.load(Ordering::Relaxed)
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_last_receive_lsn Last received WAL end LSN"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_last_receive_lsn gauge").ok();
        writeln!(
            output,
            "wal_writer_last_receive_lsn {}",
            self.0.last_receive_lsn.load(Ordering::Relaxed)
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_last_process_lsn Last successfully processed record LSN"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_last_process_lsn gauge").ok();
        writeln!(
            output,
            "wal_writer_last_process_lsn {}",
            self.0.last_process_lsn.load(Ordering::Relaxed)
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_last_acked_lsn Last WAL position acknowledged to PostgreSQL"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_last_acked_lsn gauge").ok();
        writeln!(
            output,
            "wal_writer_last_acked_lsn {}",
            self.0.last_acked_lsn.load(Ordering::Relaxed)
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_replication_loop_exits_total Replication run loop exits"
        )
        .ok();
        writeln!(
            output,
            "# TYPE wal_writer_replication_loop_exits_total counter"
        )
        .ok();
        writeln!(
            output,
            "wal_writer_replication_loop_exits_total {}",
            self.0.replication_loop_exits.load(Ordering::Relaxed)
        )
        .ok();

        writeln!(
            output,
            "# HELP wal_writer_process_wal_errors_total WAL processing errors"
        )
        .ok();
        writeln!(output, "# TYPE wal_writer_process_wal_errors_total counter").ok();
        writeln!(
            output,
            "wal_writer_process_wal_errors_total {}",
            self.0.process_wal_errors.load(Ordering::Relaxed)
        )
        .ok();

        output
    }
}

impl Default for Metrics {
    fn default() -> Self {
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
