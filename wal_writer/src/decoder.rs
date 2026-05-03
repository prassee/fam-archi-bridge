use std::sync::Arc;

use anyhow::Result;
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

    pub async fn send_batch(&self, records: &[WalRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
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

        for ((_schema, _table), table_records) in by_table {
            let topic = format!("{}.{}.{}", self.kafka.topic_prefix(), _schema, _table);
            debug!("Ensuring Kafka topic '{}' exists", topic);
            if let Err(e) = self.kafka.ensure_topic(&topic) {
                warn!("Failed to ensure topic '{}': {}", topic, e);
                return Err(e);
            }

            let count = table_records.len();
            let mut sent = 0usize;
            let mut failed = 0usize;
            for record in table_records {
                let key = record.tx_xid.to_string();
                let value = serde_json::to_string(&record)?;

                if let Err(e) = self.kafka.send(&topic, &key, &value) {
                    error!("Kafka send failed topic='{}' xid={}: {}", topic, key, e);
                    self.metrics.kafka_send_errors_inc();
                    failed += 1;
                } else {
                    self.metrics.kafka_messages_sent_inc();
                    sent += 1;
                }
            }

            debug!(
                "Kafka topic='{}' sent={} failed={} total={}",
                topic, sent, failed, count
            );
            debug!("Sent {} records to topic {}", count, topic);
        }

        Ok(())
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
