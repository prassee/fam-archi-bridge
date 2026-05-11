#![allow(dead_code)]
use std::time::Duration;

use anyhow::Context;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use tracing::info;
use wal_common::AppConfig;

pub struct KafkaProducer {
    producer: Option<FutureProducer>,
    topic_prefix: String,
    debug_no_kafka: bool,
    debug_print_wal: bool,
}

impl KafkaProducer {
    pub fn new(config: &AppConfig) -> anyhow::Result<Self> {
        let debug_no_kafka = config.kafka.debug_no_kafka;
        let debug_print_wal = config.kafka.debug_print_wal;

        let producer = if !debug_no_kafka {
            let mut client_config = ClientConfig::new();
            client_config
                .set("bootstrap.servers", &config.kafka.brokers)
                .set("acks", &config.kafka.acks)
                .set("linger.ms", config.kafka.linger_ms.to_string())
                .set("batch.size", config.kafka.batch_size.to_string())
                .set(
                    "queue.buffering.max.ms",
                    config.kafka.queue_buffering_max_ms.to_string(),
                )
                .set("api.version.request", "true")
                .set("socket.timeout.ms", "30000");

            if let Some(ref compression) = config.kafka.compression {
                client_config.set("compression.type", compression);
            }

            Some(
                client_config
                    .create::<FutureProducer>()
                    .context("Failed to create Kafka producer")?,
            )
        } else {
            info!("Kafka debug mode enabled; Kafka sends are disabled");
            None
        };

        if !debug_no_kafka {
            info!("Kafka producer connected to {}", config.kafka.brokers);
        }

        Ok(Self {
            producer,
            topic_prefix: config.kafka.topic_prefix().to_string(),
            debug_no_kafka,
            debug_print_wal,
        })
    }

    pub fn ensure_topic(&self, _topic: &str) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn topic_prefix(&self) -> &str {
        &self.topic_prefix
    }

    pub fn is_debug_no_kafka(&self) -> bool {
        self.debug_no_kafka
    }

    pub async fn send(&self, topic: &str, key: &str, value: &str) -> Result<(), KafkaError> {
        if self.debug_no_kafka {
            if self.debug_print_wal {
                info!(
                    "WAL debug skip send topic='{}' key='{}' value='{}'",
                    topic, key, value
                );
            }
            return Ok(());
        }

        let producer = self
            .producer
            .as_ref()
            .expect("Kafka producer is not initialized");

        let record = FutureRecord::to(topic).key(key).payload(value);
        let delivery_future = producer.send(record, Duration::from_secs(0));

        match delivery_future.await {
            Ok((_partition, _offset)) => Ok(()),
            Err((e, _record)) => Err(e),
        }
    }

    pub fn flush(&self, timeout: Duration) -> anyhow::Result<()> {
        if self.debug_no_kafka {
            return Ok(());
        }

        let producer = self
            .producer
            .as_ref()
            .expect("Kafka producer is not initialized");
        producer.flush(timeout).context("Kafka flush failed")
    }
}
