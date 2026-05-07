use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use tracing::{error, info, warn};
use wal_common::AppConfig;

const COMMIT_EVERY_MESSAGES: u64 = 500;
const COMMIT_EVERY_SECS: u64 = 1;
async fn refresh_subscription(
    consumer: &StreamConsumer,
    topic_prefix: &str,
    subscribed_topics: &mut Vec<String>,
) -> anyhow::Result<()> {
    let mut discovered = discover_topics(consumer, topic_prefix).await?;
    discovered.sort();

    if discovered.is_empty() {
        return Ok(());
    }

    if discovered != *subscribed_topics {
        let refs: Vec<&str> = discovered.iter().map(String::as_str).collect();
        consumer
            .subscribe(&refs)
            .context("Failed to refresh Kafka topic subscription")?;
        info!("Updated Kafka topic subscription: {:?}", discovered);
        *subscribed_topics = discovered;
    }

    Ok(())
}

fn init_logging() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn kafka_group_id() -> String {
    std::env::var("WAL_CONSUMER_GROUP_ID").unwrap_or_else(|_| "wal-consumer-console".to_string())
}

async fn discover_topics(consumer: &StreamConsumer, topic_prefix: &str) -> anyhow::Result<Vec<String>> {
    let consumer = consumer.clone();
    let topic_prefix = topic_prefix.to_string();
    let metadata = tokio::task::spawn_blocking(move || {
        consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .context("Failed to fetch Kafka metadata")
    })
    .await
    .context("Metadata fetch task failed")??;

    Ok(metadata
        .topics()
        .iter()
        .map(|topic| topic.name())
        .filter(|name| {
            name.starts_with(&topic_prefix) && name.as_bytes().get(topic_prefix.len()) == Some(&b'.')
        })
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
        .set("session.timeout.ms", "60000") // 60s timeout for group coordination
        .set("heartbeat.interval.ms", "2000") // 2s heartbeats (faster failure detection)
        .set("max.poll.interval.ms", "300000") // 5m max time between polls
        .set("enable.partition.eof", "false")
        .create()
        .context("Failed to create Kafka consumer")?;

    let mut topics = loop {
        let mut discovered = discover_topics(&consumer, config.kafka.topic_prefix()).await?;
        discovered.sort();
        if !discovered.is_empty() {
            break discovered;
        }

        info!(
            "No Kafka topics found for prefix {}, waiting for CDC topics...",
            config.kafka.topic_prefix()
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
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
    let mut refresh_tick = tokio::time::interval(Duration::from_secs(15));
    let mut pending_commit_messages: u64 = 0;
    let mut last_commit_at = tokio::time::Instant::now();

    loop {
        tokio::select! {
            _ = refresh_tick.tick() => {
                if let Err(err) = refresh_subscription(&consumer, config.kafka.topic_prefix(), &mut topics).await {
                    error!("Failed to refresh topics: {}", err);
                }
            }
            maybe_message = stream.next() => {
                let Some(message) = maybe_message else {
                    break;
                };
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
                    "Consumed message topic={} partition={} offset={} key={} payload_bytes={}",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    key,
                    payload.len()
                );

                pending_commit_messages += 1;
                let should_commit = pending_commit_messages >= COMMIT_EVERY_MESSAGES
                    || last_commit_at.elapsed() >= Duration::from_secs(COMMIT_EVERY_SECS);

                if should_commit {
                    match consumer.commit_consumer_state(CommitMode::Async) {
                        Ok(_) => {
                            info!("Committed {} messages to Kafka", pending_commit_messages);
                            pending_commit_messages = 0;
                            last_commit_at = tokio::time::Instant::now();
                        }
                        Err(err) => {
                            // Commit failed but we still reset counters to retry with fresh batch
                            // Async commits are automatically retried by rdkafka
                            warn!("Kafka offset commit failed (will retry): {}", err);
                            pending_commit_messages = 0;
                            last_commit_at = tokio::time::Instant::now();
                        }
                    }
                }
            }
            Err(err) => {
                error!("Kafka consume error: {}", err);
            }
                }
            }
        }
    }

    Ok(())
}
