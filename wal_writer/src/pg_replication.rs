#![allow(dead_code)]
use std::sync::Arc;

use anyhow::{Context, Result};
use std::fmt::Write;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};
use wal_common::AppConfig;

use crate::decoder::WalDecoder;
use crate::kafka::KafkaProducer;
use crate::metrics::Metrics;
use crate::state::{BatchConfig, BatchQueue, BatchQueueRouter, LsnTracker, PendingBatch};
use crate::wal_parser::{WalParser, WalRecord};

use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};

pub struct WalReader {
    config: Arc<AppConfig>,
    decoder: WalDecoder,
    lsn_tracker: LsnTracker,
    metrics: Metrics,
    batch_queue_router: BatchQueueRouter,
    batch_config: BatchConfig,
}

fn hex_encode(data: &[u8]) -> String {
    let mut output = String::with_capacity(data.len() * 2);
    for byte in data {
        write!(output, "{:02x}", byte).ok();
    }
    output
}

fn parse_lsn_hex(lsn: &str) -> Option<u64> {
    let mut parts = lsn.split('/');
    let hi = parts.next()?;
    let lo = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some((hi << 32) | lo)
}

impl WalReader {
    pub fn new(
        config: Arc<AppConfig>,
        kafka_producer: Arc<KafkaProducer>,
        metrics: Metrics,
    ) -> Self {
        let state_path = config
            .state
            .directory
            .as_ref()
            .map(|d| d.join("wal_position.json"));

        let lsn_tracker = LsnTracker::new(state_path);
        let batch_queue_router =
            BatchQueueRouter::new(config.kafka.num_publishers, config.pending_batch_queue_size);

        // Read batch config from AppConfig
        let batch_config = BatchConfig {
            max_records_per_batch: config.replication.batch_size as usize,
            flush_interval_ms: config.replication.poll_interval_ms as u64,
        };

        Self {
            config,
            decoder: WalDecoder::new(kafka_producer, metrics.clone()),
            lsn_tracker,
            metrics,
            batch_queue_router,
            batch_config,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting WAL reader in replication mode");

        // Load persisted LSN state
        if let Err(e) = self.lsn_tracker.load().await {
            warn!("Failed to load LSN state: {}", e);
        }
        let slot_name = self.config.pg.slot_name().to_string();
        let start_lsn = match self.lsn_tracker.get_last_lsn(&slot_name).await {
            Some(saved_lsn) => match parse_lsn_hex(&saved_lsn) {
                Some(value) => {
                    info!(
                        "Resuming replication from persisted LSN {} for slot {}",
                        saved_lsn, slot_name
                    );
                    value
                }
                None => {
                    warn!(
                        "Invalid persisted LSN '{}' for slot {}, falling back to 0/0",
                        saved_lsn, slot_name
                    );
                    0
                }
            },
            None => 0,
        };

        // Build replication client config
        let cfg = ReplicationConfig {
            host: self.config.pg.host.clone(),
            port: self.config.pg.port,
            user: self.config.pg.user.clone(),
            password: self.config.pg.password.clone(),
            database: self.config.pg.database.clone(),
            slot: slot_name.clone(),
            publication: self.config.replication.publication.clone(),
            start_lsn: Lsn(start_lsn),
            ..Default::default()
        };

        let mut client = ReplicationClient::connect(cfg)
            .await
            .context("Failed to connect replication client")?;
        info!("Connected replication client for slot {}", slot_name);
        self.metrics.set_replication_connected(true);

        let parser = WalParser::new();
        let decoder = self.decoder.clone();
        let lsn_tracker = self.lsn_tracker.clone();
        let metrics = self.metrics.clone();
        let batch_queue_router = self.batch_queue_router.clone();

        let mut _total_wal_bytes: u64 = 0;
        let mut _total_records_parsed: u64 = 0;
        let mut _total_batches_sent: u64 = 0;
        let mut total_parse_errors: u64 = 0;
        let mut _last_log = Instant::now();
        let mut confirmed_lsn: u64 = 0;
        const LOG_INTERVAL_SECS: u64 = 10;
        let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel::<u64>();

        info!("WAL reader loop started — waiting for replication events");

        // Spawn multiple parallel batch publisher tasks
        let num_publishers = self.config.kafka.num_publishers;
        info!(
            "Spawning {} parallel batch publisher task(s) with per-topic affinity",
            num_publishers
        );

        for publisher_id in 0..num_publishers {
            let publisher_decoder = decoder.clone();
            let publisher_batch_queue = batch_queue_router.get_queue(publisher_id);
            let publisher_lsn_tracker = lsn_tracker.clone();
            let publisher_metrics = metrics.clone();
            let publisher_slot_name = slot_name.clone();
            let publisher_batch_config = self.batch_config.clone();
            let publisher_ack_sender = ack_sender.clone();

            tokio::spawn(async move {
                info!("Publisher task {} started", publisher_id);
                batch_publisher_loop(
                    publisher_decoder,
                    publisher_batch_queue,
                    publisher_lsn_tracker,
                    publisher_metrics,
                    publisher_slot_name,
                    publisher_batch_config,
                    publisher_ack_sender,
                )
                .await
            });
        }

        // Receive replication events
        loop {
            tokio::select! {
                maybe_acked_lsn = ack_receiver.recv() => {
                    if let Some(acked_lsn) = maybe_acked_lsn {
                        confirmed_lsn = confirmed_lsn.max(acked_lsn);
                        client.update_applied_lsn(Lsn(confirmed_lsn));
                        metrics.set_last_acked_lsn(confirmed_lsn);

                        debug!(
                            "Advanced replication applied LSN to {}",
                            format!("{}/{}", confirmed_lsn >> 32, confirmed_lsn & 0xFFFFFFFF)
                        );
                    } else {
                        warn!("Batch publisher acknowledgment channel closed");
                    }
                }
                replication_event = client.recv() => match replication_event
                    .context("Failed to receive replication event")?
                {
                Some(ReplicationEvent::XLogData {
                    wal_start: _,
                    wal_end,
                    data,
                    server_time_micros: _,
                }) => {
                    let bytes = data.len() as u64;
                    _total_wal_bytes += bytes;
                    debug!(
                        "Received XLogData {} bytes at LSN {}",
                        bytes,
                        wal_end.as_u64()
                    );
                    metrics.inc_by(bytes);
                    metrics.inc_wal_message("xlogdata");
                    metrics.set_last_receive_lsn(wal_end.as_u64());

                    if self.config.kafka.publish_raw_wal {
                        let topic = format!("{}.raw", self.config.kafka.topic_prefix());
                        let raw_value = hex_encode(&data);
                        match decoder.send_raw(&topic, &wal_end.as_u64().to_string(), &raw_value).await {
                            Ok(()) => {
                                confirmed_lsn = confirmed_lsn.max(wal_end.as_u64());
                                client.update_applied_lsn(Lsn(confirmed_lsn));
                                metrics.set_last_acked_lsn(confirmed_lsn);
                                let lsn = format!(
                                    "{}/{}",
                                    confirmed_lsn >> 32,
                                    confirmed_lsn & 0xFFFFFFFF
                                );
                                let _ = lsn_tracker.persist(&slot_name, &lsn).await;
                            }
                            Err(e) => {
                                error!("Failed to send raw WAL payload to Kafka: {}", e);
                                metrics.process_wal_errors_inc();
                            }
                        }
                        continue;
                    }

                    match parser.parse(&data) {
                        Ok(mut records) => {
                            for record in &mut records {
                                if record.lsn == 0 {
                                    record.lsn = wal_end.as_u64();
                                }
                            }

                            let n = records.len();
                            metrics.wal_records_parsed_inc(n as u64);
                            if !records.is_empty() {
                                _total_records_parsed += n as u64;
                                debug!(
                                    "Parsed {} WAL record(s) from {} bytes at LSN {}",
                                    n,
                                    bytes,
                                    wal_end.as_u64()
                                );

                                // Stage 1: Buffer records into pending batch queue
                                // Do NOT acknowledge LSN yet
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
                                    if table_records.is_empty() {
                                        continue;
                                    }

                                    let topic = format!("{}.{}.{}", decoder.kafka_topic_prefix(), _schema, _table);

                                    // Split records into properly-sized batches
                                    let batches = split_into_batches(
                                        &topic,
                                        table_records,
                                        self.batch_config.max_records_per_batch,
                                    );

                                    for batch in batches {
                                        let wait_iterations = batch_queue_router
                                            .enqueue_with_backpressure(batch, 5)
                                            .await
                                            .context("Failed to enqueue batch with backpressure")?;

                                        if wait_iterations > 0 {
                                            let waited_ms = wait_iterations * 5;
                                            warn!(
                                                "Applied backpressure while queueing batch for topic='{}' table='{}.{}': waited {} ms (pending={})",
                                                topic,
                                                _schema,
                                                _table,
                                                waited_ms,
                                                batch_queue_router.total_count().await
                                            );
                                        } else {
                                            debug!("Batch queued for Kafka publish (pending={})", batch_queue_router.total_count().await);
                                        }
                                    }
                                }

                                debug!("Buffered {} records into pending batches", n);
                            } else {
                                debug!(
                                    "XLogData {} bytes yielded 0 records (non-DML message)",
                                    bytes
                                );
                            }
                        }
                        Err(e) => {
                            total_parse_errors += 1;
                            warn!(
                                "Failed to parse WAL data ({} bytes): {} [total errors={}]",
                                bytes, e, total_parse_errors
                            );
                            metrics.parsing_errors_inc();
                            metrics.process_wal_errors_inc();
                        }
                    }
                }
                Some(ReplicationEvent::Begin {
                    final_lsn,
                    xid: _,
                    commit_time_micros: _,
                }) => {
                    metrics.inc_wal_message("begin");
                    debug!("Replication begin at {}", final_lsn.as_u64());
                }
                Some(ReplicationEvent::Commit {
                    lsn,
                    end_lsn,
                    commit_time_micros: _,
                }) => {
                    metrics.inc_wal_message("commit");
                    debug!(
                        "Commit at LSN {} (end LSN {}) — pending batch queue size={}",
                        lsn.as_u64(),
                        end_lsn.as_u64(),
                        batch_queue_router.total_count().await
                    );
                }
                Some(ReplicationEvent::KeepAlive {
                    wal_end,
                    reply_requested,
                    server_time_micros: _,
                }) => {
                    metrics.inc_wal_message("keepalive");
                    debug!(
                        "Keepalive at {} reply_requested={} pending_batches={}",
                        wal_end.as_u64(),
                        reply_requested,
                        batch_queue_router.total_count().await
                    );

                    // CRITICAL FIX: Send status response when PostgreSQL requests it
                    // This allows PostgreSQL to advance replication slot confirmed_flush_lsn
                    if reply_requested {
                        client.update_applied_lsn(Lsn(confirmed_lsn));
                        debug!(
                            "Sent keepalive status response to PostgreSQL (confirmed_lsn={})",
                            format!("{}/{}", confirmed_lsn >> 32, confirmed_lsn & 0xFFFFFFFF)
                        );
                    }
                }
                Some(ReplicationEvent::Message {
                    transactional: _,
                    lsn,
                    prefix,
                    content,
                }) => {
                    metrics.inc_wal_message("message");
                    debug!(
                        "Logical message {} at {} prefix={}",
                        content.len(),
                        lsn.as_u64(),
                        prefix
                    );
                }
                Some(ReplicationEvent::StoppedAt { reached }) => {
                    metrics.inc_wal_message("stopped");
                    info!("Replication stopped at {}", reached.as_u64());
                    break;
                }
                None => {
                    info!("Replication event stream closed");
                    break;
                }
                }
            }
        }

        metrics.inc_replication_loop_exits();
        info!("Shutting down WAL reader");
        Ok(())
    }

    async fn ensure_replication_slot(&self) -> Result<()> {
        let connection_string = self.config.pg.connection_string();
        let (client, _) = tokio_postgres::connect(&connection_string, NoTls)
            .await
            .context("Failed to connect to PostgreSQL")?;

        let slot_name = self.config.pg.slot_name();

        let exists: bool = client
            .query(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot_name],
            )
            .await?
            .first()
            .map(|row| row.get(0))
            .unwrap_or(false);

        if !exists {
            info!("Creating replication slot: {}", slot_name);
            client
                .execute(
                    &format!("CREATE_REPLICATION SLOT {} LOGICAL pgoutput", slot_name),
                    &[],
                )
                .await
                .context("Failed to create replication slot")?;
        } else {
            info!("Replication slot {} already exists", slot_name);
        }

        Ok(())
    }
}

/// Split records into appropriately-sized batches based on max_records_per_batch
/// Returns Vec of PendingBatch, each with at most max_records_per_batch records
fn split_into_batches(
    topic: &str,
    records: Vec<WalRecord>,
    max_records_per_batch: usize,
) -> Vec<PendingBatch> {
    if records.is_empty() {
        return Vec::new();
    }

    let mut batches = Vec::new();

    for chunk in records.chunks(max_records_per_batch) {
        let lsns: Vec<u64> = chunk.iter().map(|r| r.lsn).collect();
        let max_lsn = lsns.iter().copied().max().unwrap_or(0);

        batches.push(PendingBatch {
            records: chunk.to_vec(),
            lsns,
            max_lsn,
            created_at: Instant::now(),
            topic: topic.to_string(),
        });
    }

    batches
}

/// Background task: publishes buffered batches to Kafka and acknowledges LSNs
/// Stage 2 & 3 of batch-based write:
///   - Stage 2: Publish batch to Kafka with retry logic
///   - Stage 3: On success, acknowledge all LSNs in batch (ONLY after Kafka confirms)
async fn batch_publisher_loop(
    decoder: WalDecoder,
    batch_queue: BatchQueue,
    lsn_tracker: LsnTracker,
    metrics: Metrics,
    slot_name: String,
    batch_config: BatchConfig,
    ack_sender: mpsc::UnboundedSender<u64>,
) -> Result<()> {
    info!("Batch publisher task started");

    loop {
        // Dequeue first pending batch; this seeds a publish group.
        if let Some(first_batch) = batch_queue.dequeue().await {
            let topic = first_batch.topic.clone();
            let mut publish_records = first_batch.records;
            let mut publish_lsns = first_batch.lsns;
            let mut max_lsn = first_batch.max_lsn;
            let mut oldest_created_at = first_batch.created_at;
            let mut source_batch_count = 1usize;

            // Micro-batch across pending queue entries up to configured record cap
            // or until flush interval expires.
            let flush_deadline =
                Instant::now() + Duration::from_millis(batch_config.flush_interval_ms.max(1));
            while publish_records.len() < batch_config.max_records_per_batch
                && Instant::now() < flush_deadline
            {
                match batch_queue.dequeue().await {
                    Some(next_batch) => {
                        if next_batch.topic != topic
                            || publish_records.len() + next_batch.records.len()
                                > batch_config.max_records_per_batch
                        {
                            // Keep strict queue ordering when we cannot include this batch.
                            if let Err(e) = batch_queue.enqueue_front(next_batch).await {
                                error!("Failed to return batch to queue front: {}", e);
                                metrics.process_wal_errors_inc();
                            }
                            break;
                        }

                        max_lsn = max_lsn.max(next_batch.max_lsn);
                        oldest_created_at = oldest_created_at.min(next_batch.created_at);
                        publish_lsns.extend(next_batch.lsns);
                        publish_records.extend(next_batch.records);
                        source_batch_count += 1;
                    }
                    None => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }

            let record_count = publish_records.len();
            let latency_ms = oldest_created_at.elapsed().as_millis() as u64;

            // Stage 2: Publish combined batch to Kafka.
            match decoder.publish_batch(&topic, &publish_records).await {
                Ok(_highest_lsn) => {
                    // Stage 3: On success, acknowledge all LSNs in batch
                    metrics.batches_sent_inc();
                    let lsn_str = format!("{}/{}", max_lsn >> 32, max_lsn & 0xFFFFFFFF);
                    let _ = lsn_tracker.persist(&slot_name, &lsn_str).await;
                    if let Err(e) = ack_sender.send(max_lsn) {
                        error!("Failed to send acked LSN back to replication loop: {}", e);
                        metrics.process_wal_errors_inc();
                    }

                    info!(
                        "Batch published and LSN acked: topic={} records={} source_batches={} lsns={} max_lsn={} latency_ms={}",
                        topic,
                        record_count,
                        source_batch_count,
                        publish_lsns.len(),
                        lsn_str,
                        latency_ms
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to publish batch to topic='{}' (will retry): {} [queued for {} ms]",
                        topic, e, latency_ms
                    );
                    metrics.kafka_send_errors_inc();

                    // Re-queue combined batch for retry (exponential backoff)
                    if let Err(requeue_err) = batch_queue
                        .enqueue_front(PendingBatch {
                            records: publish_records,
                            lsns: publish_lsns,
                            max_lsn,
                            created_at: oldest_created_at,
                            topic,
                        })
                        .await
                    {
                        error!("Failed to re-queue batch for retry: {}", requeue_err);
                        metrics.process_wal_errors_inc();
                    }

                    // Sleep before next retry attempt
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        } else {
            // No batches pending, sleep briefly to avoid busy-spinning
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
