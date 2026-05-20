#![allow(dead_code)]
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use std::fmt::Write;
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
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
    kafka_producer: Arc<KafkaProducer>,
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

        let batch_config = BatchConfig {
            max_records_per_batch: config.replication.batch_size as usize,
            flush_interval_ms: config.replication.poll_interval_ms as u64,
        };

        Self {
            config,
            decoder: WalDecoder::new(kafka_producer.clone(), metrics.clone()),
            lsn_tracker,
            metrics,
            batch_queue_router,
            batch_config,
            kafka_producer,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting WAL reader in replication mode");

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

        let mut total_parse_errors: u64 = 0;
        let mut confirmed_lsn: u64 = 0;

        // Fence-based LSN tracking: wal_end → number of pending batches.
        // confirmed_lsn advances only when the LOWEST fence clears, ensuring that
        // no parallel publisher can race ahead and cause WAL recycling before all
        // batches from a given WAL position are delivered.
        let mut pending_fences: BTreeMap<u64, usize> = BTreeMap::new();

        // ACK channel: publishers send the wal_end of each successfully published batch.
        let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel::<u64>();

        // Graceful-shutdown flag shared with every publisher task.
        let shutdown = Arc::new(AtomicBool::new(false));

        // Spawn publishers and keep handles so we can drain them on shutdown.
        let num_publishers = self.config.kafka.num_publishers;
        info!(
            "Spawning {} parallel batch publisher task(s) with per-topic affinity",
            num_publishers
        );

        let mut publisher_handles: Vec<JoinHandle<()>> = Vec::with_capacity(num_publishers);
        for publisher_id in 0..num_publishers {
            let publisher_decoder = decoder.clone();
            let publisher_batch_queue = batch_queue_router.get_queue(publisher_id);
            let publisher_lsn_tracker = lsn_tracker.clone();
            let publisher_metrics = metrics.clone();
            let publisher_slot_name = slot_name.clone();
            let publisher_batch_config = self.batch_config.clone();
            let publisher_ack_sender = ack_sender.clone();
            let publisher_shutdown = shutdown.clone();

            let handle = tokio::spawn(async move {
                info!("Publisher task {} started", publisher_id);
                if let Err(e) = batch_publisher_loop(
                    publisher_id,
                    publisher_decoder,
                    publisher_batch_queue,
                    publisher_lsn_tracker,
                    publisher_metrics,
                    publisher_slot_name,
                    publisher_batch_config,
                    publisher_ack_sender,
                    publisher_shutdown,
                )
                .await
                {
                    error!("Publisher {} exited with error: {}", publisher_id, e);
                }
            });
            publisher_handles.push(handle);
        }

        info!("WAL reader loop started — waiting for replication events");

        'replication: loop {
            tokio::select! {
                // Shutdown signal: break out and drain
                _ = signal::ctrl_c() => {
                    info!("Shutdown signal received — draining pending batches");
                    break 'replication;
                }

                // ACK from publisher: decrement fence and possibly advance confirmed_lsn
                maybe_acked_wal_end = ack_receiver.recv() => {
                    if let Some(acked_wal_end) = maybe_acked_wal_end {
                        if let Some(count) = pending_fences.get_mut(&acked_wal_end) {
                            *count = count.saturating_sub(1);
                        }
                        // Sweep cleared fences from the front (smallest wal_end first).
                        // We can only advance confirmed_lsn once lower fences are clear.
                        loop {
                            match pending_fences.first_key_value() {
                                Some((&wal_end, &0)) => {
                                    pending_fences.pop_first();
                                    confirmed_lsn = wal_end;
                                    client.update_applied_lsn(Lsn(confirmed_lsn));
                                    metrics.set_last_acked_lsn(confirmed_lsn);
                                    let lsn_str = format!(
                                        "{}/{}",
                                        confirmed_lsn >> 32,
                                        confirmed_lsn & 0xFFFFFFFF
                                    );
                                    let _ = lsn_tracker.persist(&slot_name, &lsn_str).await;
                                    debug!("Advanced confirmed_lsn to {}", lsn_str);
                                }
                                _ => break,
                            }
                        }
                    } else {
                        warn!("ACK channel closed unexpectedly");
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
                        continue 'replication;
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
                                // Group by (schema, table) for per-topic routing.
                                let mut by_table: HashMap<(String, String), Vec<WalRecord>> =
                                    HashMap::new();
                                for record in records {
                                    let key = (record.table_schema.clone(), record.table_name.clone());
                                    by_table.entry(key).or_default().push(record);
                                }

                                // Count total batches that will be created for this wal_end
                                // BEFORE enqueueing, so the fence is in place before any ACK
                                // can arrive from a publisher.
                                let max_per_batch = self.batch_config.max_records_per_batch;
                                let mut total_batches = 0usize;
                                for table_records in by_table.values() {
                                    let chunks = (table_records.len() + max_per_batch - 1) / max_per_batch;
                                    total_batches += chunks;
                                }
                                if total_batches > 0 {
                                    *pending_fences.entry(wal_end.as_u64()).or_insert(0) += total_batches;
                                }

                                for ((_schema, _table), table_records) in by_table {
                                    if table_records.is_empty() {
                                        continue;
                                    }

                                    let topic = format!(
                                        "{}.{}.{}",
                                        decoder.kafka_topic_prefix(),
                                        _schema,
                                        _table
                                    );

                                    let batches = split_into_batches(
                                        &topic,
                                        table_records,
                                        max_per_batch,
                                        wal_end.as_u64(),
                                    );

                                    for batch in batches {
                                        let wait_iterations = batch_queue_router
                                            .enqueue_with_backpressure(batch, 5)
                                            .await
                                            .context("Failed to enqueue batch with backpressure")?;

                                        if wait_iterations > 0 {
                                            let waited_ms = wait_iterations * 5;
                                            warn!(
                                                "Backpressure on topic='{}' table='{}.{}': waited {}ms (pending={})",
                                                topic,
                                                _schema,
                                                _table,
                                                waited_ms,
                                                batch_queue_router.total_count()
                                            );
                                        }
                                    }
                                }

                                debug!("Buffered {} WAL records into pending batches (wal_end={}, pending_fences={})", n, wal_end.as_u64(), pending_fences.len());
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
                        "Commit at LSN {} (end LSN {}) — pending_fences={} pending_batches={}",
                        lsn.as_u64(),
                        end_lsn.as_u64(),
                        pending_fences.len(),
                        batch_queue_router.total_count()
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
                        batch_queue_router.total_count()
                    );
                    // Respond when PostgreSQL requests it so confirmed_flush_lsn advances.
                    if reply_requested {
                        client.update_applied_lsn(Lsn(confirmed_lsn));
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
                        "Logical message {} bytes at {} prefix={}",
                        content.len(),
                        lsn.as_u64(),
                        prefix
                    );
                }
                Some(ReplicationEvent::StoppedAt { reached }) => {
                    metrics.inc_wal_message("stopped");
                    info!("Replication stopped at {}", reached.as_u64());
                    break 'replication;
                }
                None => {
                    info!("Replication event stream closed");
                    break 'replication;
                }
                }
            }
        }

        metrics.inc_replication_loop_exits();
        info!(
            "Replication loop exited — signalling {} publisher(s) to drain and stop",
            publisher_handles.len()
        );

        // Signal publishers: finish their in-flight work then exit.
        shutdown.store(true, Ordering::Release);
        // Drop our copy of ack_sender so publishers can still drain; the receiver
        // is local and drops at end of this fn anyway.
        drop(ack_sender);

        for (i, handle) in publisher_handles.into_iter().enumerate() {
            if let Err(e) = handle.await {
                warn!("Publisher {} did not finish cleanly: {:?}", i, e);
            }
        }
        info!("All publishers drained");

        // Flush rdkafka's internal send queue (in-flight network I/O).
        if let Err(e) = self.kafka_producer.flush(Duration::from_secs(5)) {
            warn!("Kafka flush during shutdown returned error: {}", e);
        }

        info!("WAL reader shutdown complete");
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

/// Split records into appropriately-sized batches.
/// `wal_end` is the WAL position of the XLogData message these records came from — stored on
/// each batch for fence-based LSN tracking in the replication loop.
fn split_into_batches(
    topic: &str,
    records: Vec<WalRecord>,
    max_records_per_batch: usize,
    wal_end: u64,
) -> Vec<PendingBatch> {
    if records.is_empty() {
        return Vec::new();
    }

    records
        .chunks(max_records_per_batch)
        .map(|chunk| PendingBatch {
            records: chunk.to_vec(),
            wal_end,
            created_at: Instant::now(),
            topic: topic.to_string(),
        })
        .collect()
}

/// Background task: publishes buffered batches to Kafka then ACKs their wal_ends.
///
/// Lifecycle:
///   1. Dequeue batches and micro-batch same-topic contiguous entries.
///   2. Publish combined batch to Kafka with retry.
///   3. On success: send one ACK per distinct wal_end covered by this publish.
///   4. On shutdown signal: finish current in-flight work, drain remaining queue,
///      then return.
async fn batch_publisher_loop(
    publisher_id: usize,
    decoder: WalDecoder,
    batch_queue: BatchQueue,
    lsn_tracker: LsnTracker,
    metrics: Metrics,
    slot_name: String,
    batch_config: BatchConfig,
    ack_sender: mpsc::UnboundedSender<u64>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    info!("Batch publisher {} task started", publisher_id);

    loop {
        if let Some(first_batch) = batch_queue.dequeue().await {
            let topic = first_batch.topic.clone();
            let mut publish_records = first_batch.records;
            // Track all wal_ends covered by this micro-batch so each can be ACK'd.
            let mut covered_wal_ends: BTreeSet<u64> = BTreeSet::new();
            covered_wal_ends.insert(first_batch.wal_end);
            let mut oldest_created_at = first_batch.created_at;
            let mut source_batch_count = 1usize;

            // Micro-batch: coalesce same-topic batches up to the record cap.
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
                            // Return this batch to the front for the next iteration.
                            batch_queue.enqueue_front(next_batch).await;
                            break;
                        }

                        covered_wal_ends.insert(next_batch.wal_end);
                        oldest_created_at = oldest_created_at.min(next_batch.created_at);
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

            match decoder.publish_batch(&topic, &publish_records).await {
                Ok(_) => {
                    metrics.batches_sent_inc();

                    // Persist the highest LSN from this publish as the resume point.
                    let max_wal_end = covered_wal_ends.iter().copied().max().unwrap_or(0);
                    let lsn_str = format!("{}/{}", max_wal_end >> 32, max_wal_end & 0xFFFFFFFF);
                    let _ = lsn_tracker.persist(&slot_name, &lsn_str).await;

                    // Send one ACK per wal_end so the fence tracker can advance correctly.
                    for wal_end in &covered_wal_ends {
                        if let Err(e) = ack_sender.send(*wal_end) {
                            error!(
                                "Publisher {}: failed to send ACK for wal_end={}: {}",
                                publisher_id, wal_end, e
                            );
                            metrics.process_wal_errors_inc();
                        }
                    }

                    info!(
                        "Publisher {}: published topic={} records={} source_batches={} wal_ends={} max_lsn={} latency_ms={}",
                        publisher_id,
                        topic,
                        record_count,
                        source_batch_count,
                        covered_wal_ends.len(),
                        lsn_str,
                        latency_ms
                    );
                }
                Err(e) => {
                    error!(
                        "Publisher {}: failed to publish topic='{}' (will retry): {} [queued for {}ms]",
                        publisher_id, topic, e, latency_ms
                    );
                    metrics.kafka_send_errors_inc();

                    // Re-assemble a single batch covering all covered wal_ends.
                    // Use the smallest wal_end since this batch covers a range.
                    let wal_end = covered_wal_ends.iter().copied().next().unwrap_or(0);
                    batch_queue
                        .enqueue_front(PendingBatch {
                            records: publish_records,
                            wal_end,
                            created_at: oldest_created_at,
                            topic,
                        })
                        .await;

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        } else {
            // Empty queue: check if we should exit.
            if shutdown.load(Ordering::Acquire) {
                info!(
                    "Publisher {}: queue empty and shutdown requested — exiting",
                    publisher_id
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    Ok(())
}
