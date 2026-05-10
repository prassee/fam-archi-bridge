use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use tracing::{debug, error, warn};

use crate::kafka::KafkaProducer;
use crate::metrics::Metrics;
use crate::wal_parser::WalRecord;

#[derive(Clone)]
pub struct WalDecoder {
    kafka: Arc<KafkaProducer>,
    metrics: Metrics,
}

impl WalDecoder {
    pub fn new(kafka_producer: Arc<KafkaProducer>, metrics: Metrics) -> Self {
        Self {
            kafka: kafka_producer,
            metrics,
        }
    }

    pub async fn send_batch(&self, records: &[WalRecord]) -> Result<Option<u64>> {
        if records.is_empty() {
            return Ok(None);
        }

        let mut by_table: std::collections::HashMap<(String, String), Vec<WalRecord>> =
            std::collections::HashMap::new();

        for record in records {
            let key = (record.table_schema.clone(), record.table_name.clone());
            by_table
                .entry(key)
                .or_insert_with(Vec::new)
                .push(record.clone());
        }

        let mut highest_lsn = 0u64;
        let batch_start = std::time::Instant::now();

        for ((_schema, _table), table_records) in by_table {
            let topic = format!("{}.{}.{}", self.kafka.topic_prefix(), _schema, _table);
            debug!("Ensuring Kafka topic '{}' exists", topic);
            if let Err(e) = self.kafka.ensure_topic(&topic) {
                warn!("Failed to ensure topic '{}': {}", topic, e);
                self.metrics.process_wal_errors_inc();
                return Err(e);
            }

            let count = table_records.len();
            let mut sent = 0usize;
            for record in table_records {
                let key = record.tx_xid.to_string();
                let value = serde_json::to_string(&record)?;

                // Retry with exponential backoff on QueueFull
                let mut retry_count = 0;
                let max_retries = 5;
                let mut backoff_ms = 10u64;

                loop {
                    match self.kafka.send(&topic, &key, &value) {
                        Ok(()) => {
                            self.metrics.kafka_messages_sent_inc();
                            self.metrics.published_record_inc(record.operation.clone());
                            self.metrics.set_last_process_lsn(record.lsn);
                            sent += 1;
                            highest_lsn = highest_lsn.max(record.lsn);
                            break;
                        }
                        Err(e) => {
                            if retry_count < max_retries && e.to_string().contains("QueueFull") {
                                warn!(
                                    "Kafka queue full for topic='{}', retrying in {}ms (attempt {}/{})",
                                    topic,
                                    backoff_ms,
                                    retry_count + 1,
                                    max_retries
                                );
                                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                                backoff_ms = (backoff_ms * 2).min(500); // Cap backoff at 500ms
                                retry_count += 1;
                            } else {
                                error!(
                                    "Kafka send failed topic='{}' xid={} (retries={}): {}",
                                    topic, key, retry_count, e
                                );
                                self.metrics.kafka_send_errors_inc();
                                self.metrics.publish_failure_inc();
                                self.metrics.process_wal_errors_inc();
                                return Err(e.into());
                            }
                        }
                    }
                }
            }

            debug!("Kafka topic='{}' sent={} total={}", topic, sent, count);
            debug!("Sent {} records to topic {}", count, topic);
        }

        self.metrics.observe_publish_duration(batch_start.elapsed());

        if let Err(err) = self.kafka.flush(Duration::from_secs(5)) {
            error!("Kafka flush failed: {}", err);
            self.metrics.publish_failure_inc();
            self.metrics.process_wal_errors_inc();
            return Err(anyhow!("Kafka flush failed: {}", err));
        }

        Ok(Some(highest_lsn))
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct DecoderConfig {
    pub batch_size: usize,
    pub flush_interval_ms: u64,
    pub max_retries: u32,
}

impl Default for DecoderConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            flush_interval_ms: 100,
            max_retries: 3,
        }
    }
}
