use std::sync::Arc;

use anyhow::Result;
use tokio::signal;
use tracing::{error, info};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod config;
mod decoder;
mod kafka;
mod metrics;
mod pg_replication;
mod wal_parser;

pub use config::AppConfig;
pub use metrics::Metrics;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = AppConfig::from_env()?;

    setup_logging(&cfg)?;

    info!("Starting WAL Writer - CDC capture for PostgreSQL");
    info!(
        "Configuration loaded: pg_host={}, kafka_brokers={}",
        cfg.pg.host, cfg.kafka.brokers
    );

    let metrics = Metrics::new();
    let config = Arc::new(cfg);

    let kafka_producer = Arc::new(kafka::KafkaProducer::new(&config)?);

    let mut wal_reader = pg_replication::WalReader::new(
        config.clone(),
        kafka_producer,
        metrics,
    );

    tokio::select! {
        result = wal_reader.run() => {
            if let Err(e) = result {
                error!("Wal reader error: {}", e);
                std::process::exit(1);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }

    info!("Shutdown complete");
    Ok(())
}

fn setup_logging(config: &AppConfig) -> Result<()> {
    let file_appender = RollingFileAppender::new(
        Rotation::DAILY,
        &config.logging.directory,
        "wal-writer.log",
    );

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(EnvFilter::new(config.logging.level.as_str()))
        .with(fmt::layer().with_writer(non_blocking).with_ansi(false))
        .with(fmt::layer().with_writer(std::io::stderr))
        .init();

    std::mem::forget(_guard);
    Ok(())
}