use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use tracing::{error, info};
use wal_common::AppConfig;

fn init_logging() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn kafka_group_id() -> String {
    std::env::var("WAL_CONSUMER_GROUP_ID")
        .unwrap_or_else(|_| "wal-consumer-console".to_string())
}

fn discover_topics(consumer: &StreamConsumer, topic_prefix: &str) -> anyhow::Result<Vec<String>> {
    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(5))
        .context("Failed to fetch Kafka metadata")?;

    Ok(metadata
        .topics()
        .iter()
        .map(|topic| topic.name())
        .filter(|name| name.starts_with(topic_prefix) && name.as_bytes().get(topic_prefix.len()) == Some(&b'.'))
        .map(str::to_string)
        .collect())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let config = AppConfig::from_env().context("Failed to load wal_writer config from env")?;
    let group_id = kafka_group_id();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka.brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("enable.partition.eof", "false")
        .create()
        .context("Failed to create Kafka consumer")?;

    let topics = loop {
        let topics = discover_topics(&consumer, config.kafka.topic_prefix())?;
        if !topics.is_empty() {
            break topics;
        }

        info!(
            "No Kafka topics found for prefix {}, waiting for CDC topics...",
            config.kafka.topic_prefix()
        );
        std::thread::sleep(Duration::from_secs(5));
    };

    let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&topic_refs)
        .context("Failed to subscribe to Kafka topics")?;

    info!(
        "Kafka consumer connected to {} with group {} subscribing to topics {:?}",
        config.kafka.brokers, group_id, topics
    );

    let mut stream = consumer.stream();
    while let Some(message) = stream.next().await {
        match message {
            Ok(message) => {
                let key = message
                    .key_view::<str>()
                    .transpose()
                    .context("Failed to decode Kafka key as UTF-8")?
                    .unwrap_or("");
                let payload = message
                    .payload_view::<str>()
                    .transpose()
                    .context("Failed to decode Kafka payload as UTF-8")?
                    .unwrap_or("");

                info!(
                    "Consumed message topic={} partition={} offset={} key={} payload={}",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    key,
                    payload
                );

                consumer
                    .commit_message(&message, CommitMode::Async)
                    .context("Failed to commit Kafka message")?;
            }
            Err(err) => {
                error!("Kafka consume error: {}", err);
            }
        }
    }

    Ok(())
}
