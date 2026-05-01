#![allow(dead_code)]
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{debug, error, info, warn};
use tokio_postgres::NoTls;

use crate::config::AppConfig;
use crate::decoder::WalDecoder;
use crate::kafka::KafkaProducer;
use crate::metrics::Metrics;
use crate::state::LsnTracker;
use crate::wal_parser::WalParser;

use pgwire_replication::{ReplicationClient, ReplicationConfig, ReplicationEvent};

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
        let state_path = config.state.directory.as_ref()
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
            publication: self.config.replication.wal_position.clone().unwrap_or_else(|| self.config.pg.database.clone()),
            start_lsn: 0.into(),
            ..Default::default()
        };

        let mut client = ReplicationClient::connect(cfg).await.context("Failed to connect replication client")?;
        info!("Connected replication client for slot {}", slot_name);

        let parser = WalParser::new();
        let decoder = self.decoder.clone();
        let lsn_tracker = self.lsn_tracker.clone();
        let metrics = self.metrics.clone();

        // Receive replication events
        loop {
            match client.recv().await.context("Failed to receive replication event")? {
                Some(ReplicationEvent::XLogData { wal_start: _, wal_end, data, server_time_micros: _ }) => {
                    debug!("Received XLogData {} bytes at {}", data.len(), wal_end.as_u64());
                    metrics.inc_by(data.len() as u64);

                    match parser.parse(&data) {
                        Ok(records) => {
                            metrics.wal_records_parsed_inc(records.len() as u64);
                            if !records.is_empty() {
                                if let Err(e) = decoder.send_batch(&records).await {
                                    error!("Failed to send batch to Kafka: {}", e);
                                }
                                metrics.batches_sent_inc();

                                if let Some(record) = records.last() {
                                    let lsn = format!("{}/{}", record.lsn >> 32, record.lsn & 0xFFFFFFFF);
                                    let _ = lsn_tracker.persist(&slot_name, &lsn).await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse WAL data: {}", e);
                            metrics.parsing_errors_inc();
                        }
                    }
                }
                Some(ReplicationEvent::Begin { final_lsn, xid: _, commit_time_micros: _ }) => {
                    // Could use begin event for transactional boundary handling
                    debug!("Replication begin at {}", final_lsn.as_u64());
                }
                Some(ReplicationEvent::Commit { lsn, end_lsn, commit_time_micros: _ }) => {
                    debug!("Replication commit at {} (end {})", lsn.as_u64(), end_lsn.as_u64());
                    // Persist LSN on commit boundary
                    let lsn_str = format!("{}/{}", end_lsn.as_u64() >> 32, end_lsn.as_u64() & 0xFFFFFFFF);
                    let _ = lsn_tracker.persist(&slot_name, &lsn_str).await;
                }
                Some(ReplicationEvent::KeepAlive { wal_end, reply_requested: _, server_time_micros: _ }) => {
                    debug!("Keepalive at {}", wal_end.as_u64());
                }
                Some(ReplicationEvent::Message { transactional: _, lsn, prefix, content }) => {
                    debug!("Logical message {} at {} prefix={}", content.len(), lsn.as_u64(), prefix);
                    // treat message content as an event or log it
                }
                Some(ReplicationEvent::StoppedAt { reached }) => {
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
