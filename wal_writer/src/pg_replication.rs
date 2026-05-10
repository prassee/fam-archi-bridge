#![allow(dead_code)]
use std::sync::Arc;

use anyhow::{Context, Result};
use std::time::Instant;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};
use wal_common::AppConfig;

use crate::decoder::WalDecoder;
use crate::kafka::KafkaProducer;
use crate::metrics::Metrics;
use crate::state::LsnTracker;
use crate::wal_parser::WalParser;

use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent};

pub struct WalReader {
    config: Arc<AppConfig>,
    decoder: WalDecoder,
    lsn_tracker: LsnTracker,
    metrics: Metrics,
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

        Self {
            config,
            decoder: WalDecoder::new(kafka_producer, metrics.clone()),
            lsn_tracker,
            metrics,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting WAL reader in replication mode");

        // Load persisted LSN state
        if let Err(e) = self.lsn_tracker.load().await {
            warn!("Failed to load LSN state: {}", e);
        }
        let slot_name = self.config.pg.slot_name().to_string();

        // Build replication client config
        let cfg = ReplicationConfig {
            host: self.config.pg.host.clone(),
            port: self.config.pg.port,
            user: self.config.pg.user.clone(),
            password: self.config.pg.password.clone(),
            database: self.config.pg.database.clone(),
            slot: slot_name.clone(),
            publication: self.config.replication.publication.clone(),
            start_lsn: 0.into(),
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

        let mut _total_wal_bytes: u64 = 0;
        let mut _total_records_parsed: u64 = 0;
        let mut total_batches_sent: u64 = 0;
        let mut total_parse_errors: u64 = 0;
        let mut _last_log = Instant::now();
        let mut confirmed_lsn: u64 = 0;
        const LOG_INTERVAL_SECS: u64 = 10;

        info!("WAL reader loop started — waiting for replication events");

        // Receive replication events
        loop {
            match client
                .recv()
                .await
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

                    match parser.parse(&data) {
                        Ok(records) => {
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
                                match decoder.send_batch(&records).await {
                                    Ok(Some(highest_lsn)) => {
                                        total_batches_sent += 1;
                                        debug!(
                                            "Kafka batch sent: {} record(s) [total batches={}]",
                                            n, total_batches_sent
                                        );
                                        confirmed_lsn =
                                            confirmed_lsn.max(highest_lsn).max(wal_end.as_u64());
                                        client.update_applied_lsn(Lsn(confirmed_lsn));
                                        metrics.set_last_acked_lsn(confirmed_lsn);
                                        let lsn = format!(
                                            "{}/{}",
                                            confirmed_lsn >> 32,
                                            confirmed_lsn & 0xFFFFFFFF
                                        );
                                        let _ = lsn_tracker.persist(&slot_name, &lsn).await;
                                        metrics.batches_sent_inc();
                                    }
                                    Ok(None) => {
                                        debug!(
                                            "XLogData {} bytes contained no WAL records to publish",
                                            bytes
                                        );
                                        confirmed_lsn = confirmed_lsn.max(wal_end.as_u64());
                                        client.update_applied_lsn(Lsn(confirmed_lsn));
                                        metrics.set_last_acked_lsn(confirmed_lsn);
                                    }
                                    Err(e) => {
                                        error!("Failed to send batch to Kafka: {}", e);
                                        metrics.process_wal_errors_inc();
                                    }
                                }
                            } else {
                                debug!(
                                    "XLogData {} bytes yielded 0 records (non-DML message)",
                                    bytes
                                );
                                confirmed_lsn = confirmed_lsn.max(wal_end.as_u64());
                                client.update_applied_lsn(Lsn(confirmed_lsn));
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
                        "Commit at LSN {} (end LSN {}) — flushing LSN feedback",
                        lsn.as_u64(),
                        end_lsn.as_u64()
                    );
                    if confirmed_lsn > 0 {
                        client.update_applied_lsn(Lsn(confirmed_lsn));
                        metrics.set_last_acked_lsn(confirmed_lsn);
                        let lsn_str =
                            format!("{}/{}", confirmed_lsn >> 32, confirmed_lsn & 0xFFFFFFFF);
                        let _ = lsn_tracker.persist(&slot_name, &lsn_str).await;
                    }
                }
                Some(ReplicationEvent::KeepAlive {
                    wal_end,
                    reply_requested,
                    server_time_micros: _,
                }) => {
                    metrics.inc_wal_message("keepalive");
                    debug!(
                        "Keepalive at {} reply_requested={}",
                        wal_end.as_u64(),
                        reply_requested
                    );
                    if confirmed_lsn > 0 && reply_requested {
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
                        "Logical message {} at {} prefix={}",
                        content.len(),
                        lsn.as_u64(),
                        prefix
                    );
                    // treat message content as an event or log it
                }
                Some(ReplicationEvent::StoppedAt { reached }) => {
                    metrics.inc_wal_message("stopped");
                    info!("Replication stopped at {}", reached.as_u64());
                    break;
                }
                None => {
                    // recv returned None: replication worker closed the channel
                    info!("Replication event stream closed");
                    break;
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
