#![allow(dead_code)]
use std::time::Duration;

use anyhow::Context;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::Producer;
use tracing::info;

use crate::config::AppConfig;

pub struct KafkaProducer {
    producer: rdkafka::producer::ThreadedProducer<rdkafka::producer::DefaultProducerContext>,
    topic_prefix: String,
}

impl KafkaProducer {
    pub fn new(config: &AppConfig) -> anyhow::Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.kafka.brokers)
            .set("acks", &config.kafka.acks)
            .set("linger.ms", config.kafka.linger_ms.to_string())
            .set("batch.size", config.kafka.batch_size.to_string())
            .set("queue.buffering.max.ms", "0")
            .set("api.version.request", "true")
            .set("socket.timeout.ms", "30000");

        if let Some(ref compression) = config.kafka.compression {
            client_config.set("compression.type", compression);
        }

        let producer = client_config
            .create::<rdkafka::producer::ThreadedProducer<rdkafka::producer::DefaultProducerContext>>()
            .context("Failed to create Kafka producer")?;

        info!("Kafka producer connected to {}", config.kafka.brokers);

        Ok(Self {
            producer,
            topic_prefix: config.kafka.topic_prefix().to_string(),
        })
    }

    pub fn ensure_topic(&self, _topic: &str) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn topic_prefix(&self) -> &str {
        &self.topic_prefix
    }

    pub fn send(
        &self,
        topic: &str,
        key: &str,
        value: &str,
    ) -> Result<(), KafkaError> {
        use rdkafka::producer::BaseRecord;
        
        let record = BaseRecord::to(topic)
            .key(key)
            .payload(value);
        self.producer.send(record).map_err(|(e, _)| e)
    }

    pub fn flush(&self) {
        let _ = self.producer.flush(Duration::from_millis(1000));
    }
}