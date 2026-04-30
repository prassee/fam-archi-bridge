#![allow(dead_code)]
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::decoder::WalDecoder;
use crate::kafka::KafkaProducer;
use crate::metrics::Metrics;
use crate::state::LsnTracker;
use crate::wal_parser::WalParser;

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

        self.ensure_replication_slot().await?;

        let connection_string = self.config.pg.connection_string();
        let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
            .await
            .context("Failed to connect to PostgreSQL")?;

        let slot_name = self.config.pg.slot_name();
        
        // Get last consumed position or start from beginning
        let start_lsn = self.lsn_tracker.get_last_lsn(slot_name).await
            .unwrap_or_else(|| "0/0".to_string());
        
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {}",
            slot_name,
            start_lsn
        );

        info!("Starting replication from LSN: {}", start_lsn);

        let stream = client.copy_out(&query).await?;
        let (sender, mut receiver) = mpsc::channel::<Bytes>(1000);
        
        let stream = Box::pin(stream);
        
        tokio::spawn(async move {
            let mut stream = stream;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(data) => {
                        if let Err(e) = sender.send(data).await {
                            error!("Failed to send WAL data: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Replication stream error: {}", e);
                        break;
                    }
                }
            }
        });

        let config = self.config.clone();
        let parser = WalParser::new();
        let decoder = self.decoder.clone();
        let lsn_tracker = self.lsn_tracker.clone();
        let metrics = self.metrics.clone();
        let persist_interval = std::time::Duration::from_secs(config.state.persist_interval_secs);

        tokio::spawn(async move {
            let mut batch = Vec::new();
            let mut last_batch_time = std::time::Instant::now();
            let batch_size = config.replication.batch_size as usize;
            let poll_interval =
                std::time::Duration::from_millis(config.replication.poll_interval_ms as u64);

            loop {
                let timeout = tokio::time::timeout(poll_interval, receiver.recv()).await;

                match timeout {
                    Ok(Some(data)) => {
                        debug!("Received WAL data: {} bytes", data.len());
                        metrics.inc_by(data.len() as u64);

                        match parser.parse(&data) {
                            Ok(records) => {
                                metrics.wal_records_parsed_inc(records.len() as u64);
                                batch.extend(records);

                                if batch.len() >= batch_size {
                                    if let Err(e) = decoder.send_batch(&batch).await {
                                        error!("Failed to send batch to Kafka: {}", e);
                                    }
                                    metrics.batches_sent_inc();
                                    
                                    // Persist LSN after successful batch
                                    if let Some(record) = batch.last() {
                                        let lsn = format!("{}/{}", record.lsn >> 32, record.lsn & 0xFFFFFFFF);
                                        let _ = lsn_tracker.persist("wal_writer_slot", &lsn).await;
                                    }
                                    batch.clear();
                                    last_batch_time = std::time::Instant::now();
                                }
                            }
                            Err(e) => {
                                warn!("Failed to parse WAL data: {}", e);
                                metrics.parsing_errors_inc();
                            }
                        }
                    }
                    Ok(None) => {
                        info!("Replication stream ended");
                        break;
                    }
                    Err(_) => {
                        // Timeout - flush batch if needed and persist LSN periodically
                        if !batch.is_empty() {
                            if let Err(e) = decoder.send_batch(&batch).await {
                                error!("Failed to send batch to Kafka: {}", e);
                            }
                            metrics.batches_sent_inc();
                            
                            if let Some(record) = batch.last() {
                                let lsn = format!("{}/{}", record.lsn >> 32, record.lsn & 0xFFFFFFFF);
                                let _ = lsn_tracker.persist("wal_writer_slot", &lsn).await;
                            }
                            batch.clear();
                            last_batch_time = std::time::Instant::now();
                        }
                    }
                }
            }

            // Final flush
            if !batch.is_empty() {
                let _ = decoder.send_batch(&batch).await;
            }
        });

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error: {}", e);
            }
        });

        tokio::signal::ctrl_c().await?;
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