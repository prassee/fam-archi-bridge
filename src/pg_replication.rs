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
use crate::wal_parser::WalParser;

pub struct WalReader {
    config: Arc<AppConfig>,
    decoder: WalDecoder,
    metrics: Metrics,
}

impl WalReader {
    pub fn new(
        config: Arc<AppConfig>,
        kafka_producer: Arc<KafkaProducer>,
        metrics: Metrics,
    ) -> Self {
        Self {
            config,
            decoder: WalDecoder::new(kafka_producer, metrics.clone()),
            metrics,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting WAL reader in replication mode");

        self.ensure_replication_slot().await?;

        let connection_string = self.config.pg.connection_string();
        let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
            .await
            .context("Failed to connect to PostgreSQL")?;

        let slot_name = self.config.pg.slot_name();

        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {}",
            slot_name,
            self.config.replication.wal_position.as_deref().unwrap_or("0/0")
        );

        info!("Starting replication: {}", query);

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
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut batch = Vec::new();
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
                                    batch.clear();
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
                        if !batch.is_empty() {
                            if let Err(e) = decoder.send_batch(&batch).await {
                                error!("Failed to send batch to Kafka: {}", e);
                            }
                            metrics.batches_sent_inc();
                            batch.clear();
                        }
                    }
                }
            }

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