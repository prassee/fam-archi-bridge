use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

mod arrow_converter;
mod cdc_message;
mod iceberg_writer;

use arrow_converter::ArrowConverter;
use cdc_message::CdcMessage;
use iceberg_writer::{IcebergWriter, PolarisCatalogConfig, StorageConfig};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ConsumerConfig {
    kafka: KafkaConfig,
    topics: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct KafkaConfig {
    brokers: String,
    #[serde(default = "default_group")]
    group: String,
    #[serde(default = "default_channel_capacity")]
    channel_capacity: usize,
}

fn default_group() -> String {
    "wal-consumer".to_string()
}

fn default_channel_capacity() -> usize {
    1000
}

/// Load configuration from TOML file
fn load_config(config_path: &str) -> Result<ConsumerConfig> {
    let config_str = fs::read_to_string(config_path)
        .context(format!("Failed to read config file: {}", config_path))?;

    let config: ConsumerConfig =
        toml::from_str(&config_str).context("Failed to parse TOML configuration")?;

    if config.topics.is_empty() {
        anyhow::bail!("No topics configured in config file");
    }

    Ok(config)
}

/// Spawn a handler task for each CDC topic with Arrow RecordBatch construction
async fn spawn_topic_handler(topic: String, mut rx: mpsc::Receiver<Vec<u8>>) {
    info!("Topic handler started for: {}", topic);
    let mut message_count = 0u64;
    let mut converter = ArrowConverter::new();
    let mut last_flush_time = std::time::Instant::now();
    const FLUSH_BATCH_SIZE: usize = 100; // Flush after 100 messages per table
    const FLUSH_INTERVAL: Duration = Duration::from_secs(5); // Flush every 5 seconds

    while let Some(payload) = rx.recv().await {
        message_count += 1;

        match serde_json::from_slice::<CdcMessage>(&payload) {
            Ok(cdc_msg) => {
                let table_key = format!("{}.{}", cdc_msg.table_schema, cdc_msg.table_name);

                // Log structured metadata
                info!(
                    topic = topic,
                    msg_num = message_count,
                    schema = cdc_msg.table_schema,
                    table = cdc_msg.table_name,
                    operation = cdc_msg.operation,
                    lsn = format!("0x{:X}", cdc_msg.lsn),
                    xid = cdc_msg.tx_xid,
                    "CDC event"
                );

                // Add to Arrow converter
                if let Err(e) = converter.add_message(&cdc_msg) {
                    warn!(
                        topic = topic,
                        table = table_key,
                        error = %e,
                        "Failed to add message to Arrow converter"
                    );
                    continue;
                }

                // Check if we should flush
                let should_flush = converter.table_row_count(&table_key) >= FLUSH_BATCH_SIZE
                    || last_flush_time.elapsed() > FLUSH_INTERVAL;

                if should_flush {
                    match converter.flush_all() {
                        Ok(batches) => {
                            for (table, batch) in batches {
                                info!(
                                    table = table,
                                    row_count = batch.num_rows(),
                                    columns = batch.num_columns(),
                                    "RecordBatch flushed"
                                );

                                // Log schema
                                for field in batch.schema().fields() {
                                    debug!(
                                        topic = topic,
                                        field_name = field.name(),
                                        field_type = ?field.data_type(),
                                        "Schema field"
                                    );
                                }
                            }
                            last_flush_time = std::time::Instant::now();
                        }
                        Err(e) => {
                            warn!(
                                topic = topic,
                                error = %e,
                                "Failed to flush Arrow RecordBatches"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    topic = topic,
                    msg_num = message_count,
                    error = %e,
                    "Failed to parse message as CDC message"
                );

                // Try to parse as generic JSON for debugging
                match serde_json::from_slice::<serde_json::Value>(&payload) {
                    Ok(generic_json) => {
                        eprintln!(
                            "\n❌ Failed to parse CDC message (msg #{})\nRaw JSON:\n{}\n",
                            message_count,
                            serde_json::to_string_pretty(&generic_json).unwrap_or_default()
                        );
                    }
                    Err(_) => {
                        eprintln!(
                            "\n❌ Failed to parse message (msg #{})\nRaw bytes: {}\n",
                            message_count,
                            String::from_utf8_lossy(&payload)
                        );
                    }
                }
            }
        }
    }

    // Final flush before exit
    if !converter.buffered_tables().is_empty() {
        match converter.flush_all() {
            Ok(batches) => {
                info!(
                    topic = topic,
                    batch_count = batches.len(),
                    "Final flush on exit"
                );
                for (table, batch) in batches {
                    info!(
                        topic = topic,
                        table = table,
                        row_count = batch.num_rows(),
                        "Final RecordBatch"
                    );
                }
            }
            Err(e) => {
                warn!(topic = topic, error = %e, "Failed final flush on exit");
            }
        }
    }

    info!(
        topic = topic,
        total_messages = message_count,
        "Topic handler exiting"
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with structured logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        ))
        .with_target(true)
        .with_thread_ids(true)
        .init();

    // Load configuration from file
    let config_path =
        env::var("WAL_CONSUMER_CONFIG").unwrap_or_else(|_| "wal_consumer_config.toml".to_string());

    info!("Loading configuration from: {}", config_path);
    let config = load_config(&config_path)?;

    let mut topics = config.topics.clone();
    topics.sort();

    info!(
        brokers = config.kafka.brokers,
        group = config.kafka.group,
        topic_count = topics.len(),
        topics = ?topics,
        "Kafka consumer initialized from config"
    );

    // Initialize Iceberg writer with Polaris catalog and S3/MinIO storage
    let polaris_config = PolarisCatalogConfig::from_env()?;
    let storage_config = StorageConfig::from_env()?;

    let _iceberg_writer = IcebergWriter::new(polaris_config, storage_config).await?;
    info!("Iceberg writer initialized with Polaris catalog backend");

    // Create Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &config.kafka.group)
        .set("bootstrap.servers", &config.kafka.brokers)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "30000")
        .set("socket.keepalive.enable", "true")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "100")
        .create::<StreamConsumer>()
        .expect("Failed to create Kafka consumer");

    // Create per-topic channels and spawn handlers
    let mut senders: HashMap<String, mpsc::Sender<Vec<u8>>> = HashMap::new();
    let mut topic_refs: Vec<&str> = Vec::new();

    for topic in &topics {
        let (tx, rx) = mpsc::channel(config.kafka.channel_capacity);
        senders.insert(topic.clone(), tx);
        topic_refs.push(topic);

        let topic_clone = topic.clone();
        tokio::spawn(spawn_topic_handler(topic_clone, rx));

        info!("Spawned handler for topic: {}", topic);
    }

    // Subscribe to topics
    consumer
        .subscribe(&topic_refs)
        .expect("Failed to subscribe to Kafka topics");

    info!("Subscribed to {} CDC topics: {:?}", topics.len(), topics);

    let mut last_stats_time = std::time::Instant::now();
    let mut total_messages = 0u64;

    // Consume messages and route to topic channels
    loop {
        match consumer.recv().await {
            Err(e) => {
                error!("Kafka consumer error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(msg) => {
                let topic = msg.topic();
                let partition = msg.partition();
                let offset = msg.offset();

                if let Some(payload) = msg.payload() {
                    total_messages += 1;

                    // Route to topic handler
                    if let Some(tx) = senders.get(topic) {
                        if let Err(e) = tx.try_send(payload.to_vec()) {
                            error!(
                                topic = topic,
                                partition = partition,
                                offset = offset,
                                "Failed to send message to topic handler: {}",
                                e
                            );
                        }
                    } else {
                        warn!(
                            topic = topic,
                            "Message received for unknown topic (not subscribed)"
                        );
                    }

                    // Commit offset
                    if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                        warn!(
                            topic = topic,
                            partition = partition,
                            offset = offset,
                            "Failed to commit offset: {}",
                            e
                        );
                    }
                }

                // Log stats every 30 seconds
                let now = std::time::Instant::now();
                if now.duration_since(last_stats_time) > Duration::from_secs(30) {
                    let elapsed = now.duration_since(last_stats_time).as_secs_f64();
                    let rate = total_messages as f64 / elapsed;
                    info!(
                        total_messages = total_messages,
                        rate_msg_per_sec = format!("{:.1}", rate),
                        "Consumer throughput"
                    );
                    last_stats_time = now;
                }
            }
        }
    }
}
