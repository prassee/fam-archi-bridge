use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Message, OwnedMessage};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};
use wal_common::AppConfig;

/// Commit after this many messages have been dispatched since the last commit.
/// Configurable via WAL_CONSUMER_COMMIT_EVERY env var.
/// Default 5000: at 500k msg/s this commits ~100x/s instead of ~1000x/s.
const DEFAULT_COMMIT_EVERY_MESSAGES: u64 = 5000;
/// Also commit on this wall-clock cadence regardless of message count.
const COMMIT_EVERY_SECS: u64 = 1;
/// Emit throughput stats at this interval (seconds).
const STATS_INTERVAL_SECS: u64 = 10;
/// Bounded capacity of each per-worker channel.
/// 4096 messages × N workers provides ~32 ms of buffer headroom at 125k msg/s/worker.
const WORKER_CHANNEL_CAPACITY: usize = 4096;

fn worker_count() -> usize {
    std::env::var("WAL_CONSUMER_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
}

fn commit_every_messages() -> u64 {
    std::env::var("WAL_CONSUMER_COMMIT_EVERY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_COMMIT_EVERY_MESSAGES)
}

/// Process a single consumed message. This is the extension point for downstream
/// work (e.g. Iceberg writes). Runs inside a dedicated per-partition worker task,
/// so all messages from the same (topic, partition) are processed in order.
async fn process_message(msg: OwnedMessage) {
    let key = msg
        .key()
        .and_then(|k| std::str::from_utf8(k).ok())
        .unwrap_or("");
    let payload_bytes = msg.payload().map_or(0, |p| p.len());
    debug!(
        topic = msg.topic(),
        partition = msg.partition(),
        offset = msg.offset(),
        key,
        payload_bytes,
        "consumed message",
    );
}

/// Worker task: drains its dedicated channel and processes messages.
/// One worker owns all messages from a consistent subset of (topic, partition) pairs,
/// preserving CDC ordering within each partition.
async fn run_worker(worker_id: usize, mut rx: mpsc::Receiver<OwnedMessage>) {
    debug!("Worker {worker_id} started");
    while let Some(msg) = rx.recv().await {
        process_message(msg).await;
    }
    debug!("Worker {worker_id} exiting");
}
async fn refresh_subscription(
    consumer: &StreamConsumer,
    topic_prefix: &str,
    subscribed_topics: &mut Vec<String>,
) -> anyhow::Result<()> {
    // Full cluster scan — but refreshes only run every 15 s via a Delay ticker,
    // so the cost is amortised. New tables will appear within one refresh cycle.
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
        info!(
            "Kafka subscription updated: {} topics (was {})",
            discovered.len(),
            subscribed_topics.len()
        );
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

async fn discover_topics(
    consumer: &StreamConsumer,
    topic_prefix: &str,
) -> anyhow::Result<Vec<String>> {
    let metadata = tokio::task::block_in_place(|| {
        consumer
            .fetch_metadata(None, Duration::from_secs(3))
            .context("Failed to fetch Kafka metadata")
    })?;

    Ok(metadata
        .topics()
        .iter()
        .map(|topic| topic.name())
        .filter(|name| {
            name.starts_with(topic_prefix) && name.as_bytes().get(topic_prefix.len()) == Some(&b'.')
        })
        .map(str::to_string)
        .collect())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let config = AppConfig::from_env().context("Failed to load wal_writer config from env")?;
    let group_id = kafka_group_id();
    let n_workers = worker_count();
    let commit_threshold = commit_every_messages();

    info!("Starting wal_consumer: workers={n_workers} commit_every={commit_threshold}");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka.brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        // Group coordination timeouts
        .set("session.timeout.ms", "60000")
        .set("heartbeat.interval.ms", "2000")
        .set("max.poll.interval.ms", "300000")
        .set("enable.partition.eof", "false")
        // ── Fetch batching ──────────────────────────────────────────────────
        // Wait for at least 64 KiB before the broker returns a fetch response.
        .set("fetch.min.bytes", "65536")
        // Give the broker up to 50 ms to accumulate fetch.min.bytes.
        .set("fetch.wait.max.ms", "50")
        // Cap total fetch response to 50 MiB so a single round-trip doesn't
        // allocate unboundedly at 1000+ partitions.
        .set("fetch.max.bytes", "52428800")
        // 1 MiB per partition per fetch: keeps the total in check with many
        // partitions while still delivering good per-partition throughput.
        .set("max.partition.fetch.bytes", "1048576")
        // Pre-fetch buffer: 512 MiB total. At 1000 partitions × 500 msg/s ×
        // ~1 KiB each, 512 MiB provides ~1 s of headroom. Keeps stream.next()
        // returning immediately without waiting for a network round-trip.
        .set("queued.max.messages.kbytes", "524288")
        // ── Broker / network resilience ─────────────────────────────────────
        // 60 s metadata refresh: less frequent than 30 s at large topic counts,
        // but still catches broker failovers well within a minute.
        .set("metadata.max.age.ms", "60000")
        .set("socket.keepalive.enable", "true")
        .set("fetch.error.backoff.ms", "200")
        .create()
        .context("Failed to create Kafka consumer")?;

    let mut topics = loop {
        let mut discovered = discover_topics(&consumer, config.kafka.topic_prefix()).await?;
        discovered.sort();
        if !discovered.is_empty() {
            break discovered;
        }
        info!(
            "No Kafka topics found for prefix {}, waiting...",
            config.kafka.topic_prefix()
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
    };

    let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&topic_refs)
        .context("Failed to subscribe to Kafka topics")?;

    info!(
        "Kafka consumer connected: brokers={} group={} topics={}",
        config.kafka.brokers,
        group_id,
        topics.len()
    );

    // ── Worker pool ────────────────────────────────────────────────────────
    // Each worker owns a bounded channel. Messages are routed by
    // (partition % n_workers) so all messages from the same partition always
    // land in the same worker, preserving CDC ordering within a table.
    let mut senders: Vec<mpsc::Sender<OwnedMessage>> = Vec::with_capacity(n_workers);
    for id in 0..n_workers {
        let (tx, rx) = mpsc::channel::<OwnedMessage>(WORKER_CHANNEL_CAPACITY);
        senders.push(tx);
        tokio::spawn(run_worker(id, rx));
    }

    // Shared counters updated from the dispatch loop and read by the stats tick.
    let dispatched = Arc::new(AtomicU64::new(0));
    let dispatched_stats = Arc::clone(&dispatched);

    let mut stream = consumer.stream();

    // Delay: a slow block_in_place in discover_topics never causes burst catch-ups.
    let mut refresh_tick = tokio::time::interval(Duration::from_secs(15));
    refresh_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Independent commit timer — fires even when message rate drops.
    let mut commit_tick = tokio::time::interval(Duration::from_secs(COMMIT_EVERY_SECS));
    commit_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut stats_tick = tokio::time::interval(Duration::from_secs(STATS_INTERVAL_SECS));
    stats_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut pending_commit_messages: u64 = 0;

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            result = &mut ctrl_c => {
                if result.is_ok() {
                    info!("Shutdown signal received, committing final offsets");
                }
                if let Err(e) = consumer.commit_consumer_state(CommitMode::Sync) {
                    warn!("Final offset commit on shutdown failed: {}", e);
                }
                break;
            }

            _ = stats_tick.tick() => {
                let count = dispatched_stats.swap(0, Ordering::Relaxed);
                let rate = count as f64 / STATS_INTERVAL_SECS as f64;
                info!(
                    "Consumer throughput: {count} msg/{STATS_INTERVAL_SECS}s \
                     ({rate:.0} msg/s) workers={n_workers} \
                     pending_commit={pending_commit_messages}",
                );
            }

            // Time-based commit fires independently of message flow.
            _ = commit_tick.tick() => {
                if pending_commit_messages > 0 {
                    match consumer.commit_consumer_state(CommitMode::Async) {
                        Ok(_) => { pending_commit_messages = 0; }
                        Err(err) => {
                            // Do not reset — accumulate so retry fires sooner.
                            warn!("Kafka offset commit failed (will retry): {}", err);
                        }
                    }
                }
            }

            _ = refresh_tick.tick() => {
                if let Err(err) = refresh_subscription(
                    &consumer, config.kafka.topic_prefix(), &mut topics,
                ).await {
                    error!("Failed to refresh topics: {}", err);
                }
            }

            maybe_message = stream.next() => {
                let Some(result) = maybe_message else {
                    info!("Message stream ended, committing final offsets");
                    if let Err(e) = consumer.commit_consumer_state(CommitMode::Sync) {
                        warn!("Final offset commit on stream end failed: {}", e);
                    }
                    break;
                };
                match result {
                    Ok(msg) => {
                        // Route by partition so all messages from the same
                        // (topic, partition) always land in the same worker.
                        // This preserves CDC ordering within a table.
                        let worker_idx = (msg.partition().unsigned_abs() as usize) % n_workers;

                        // OwnedMessage copies the payload out of librdkafka's
                        // internal buffer so the borrowed reference is released
                        // immediately, keeping the prefetch queue draining.
                        let owned = msg.detach();
                        if let Err(e) = senders[worker_idx].send(owned).await {
                            // Channel closed — worker panicked. Log and exit.
                            error!("Worker {worker_idx} channel closed: {}", e);
                            break;
                        }

                        pending_commit_messages += 1;
                        dispatched.fetch_add(1, Ordering::Relaxed);

                        // Count-based commit: flush promptly after a burst.
                        if pending_commit_messages >= commit_threshold {
                            match consumer.commit_consumer_state(CommitMode::Async) {
                                Ok(_) => { pending_commit_messages = 0; }
                                Err(err) => {
                                    warn!("Kafka offset commit failed (will retry): {}", err);
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
