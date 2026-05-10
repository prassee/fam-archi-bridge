use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use wal_common::AppConfig;

mod decoder;
mod kafka;
mod metrics;
mod pg_replication;
mod state;
mod wal_parser;

pub use metrics::Metrics;
pub use state::LsnTracker;

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
    if config.kafka.debug_no_kafka {
        info!("Kafka output disabled by WAL_WRITER_DEBUG_NO_KAFKA=true");
    }

    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        if let Err(err) = run_metrics_server(metrics_clone).await {
            error!("Metrics server failed: {}", err);
        }
    });

    let mut wal_reader =
        pg_replication::WalReader::new(config.clone(), kafka_producer.clone(), metrics);

    tokio::select! {
        result = wal_reader.run() => {
            if let Err(e) = result {
                error!("Wal reader error: {}", e);
                if let Err(err) = kafka_producer.flush(std::time::Duration::from_secs(5)) {
                    error!("Kafka flush failed during error shutdown: {}", err);
                }
                std::process::exit(1);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }

    info!("Flushing pending Kafka messages before shutdown");
    if let Err(err) = kafka_producer.flush(std::time::Duration::from_secs(5)) {
        error!("Kafka flush failed during shutdown: {}", err);
    }
    info!("Shutdown complete");
    Ok(())
}

async fn run_metrics_server(metrics: Metrics) -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:9090").await?;
    info!("Metrics server listening on http://0.0.0.0:9090");

    loop {
        let (mut socket, _addr) = listener.accept().await?;
        let metrics = metrics.clone();

        tokio::spawn(async move {
            let mut buffer = vec![0u8; 4096];
            let bytes_read = match socket.read(&mut buffer).await {
                Ok(0) => return,
                Ok(n) => n,
                Err(_) => return,
            };

            let request_text = String::from_utf8_lossy(&buffer[..bytes_read]);
            let request_line = request_text.lines().next().unwrap_or("");
            let path = request_line.split_whitespace().nth(1).unwrap_or("/");

            let (status, body) = match path {
                "/metrics" => ("200 OK", metrics.gather_prometheus()),
                "/health" => ("200 OK", String::from("ok")),
                _ => ("404 Not Found", String::from("not found")),
            };

            let response = format!(
                "HTTP/1.1 {}\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: {}\r\n\r\n{}",
                status,
                body.len(),
                body
            );

            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

fn setup_logging(config: &AppConfig) -> Result<()> {
    let file_appender =
        RollingFileAppender::new(Rotation::DAILY, &config.logging.directory, "wal-writer.log");

    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(EnvFilter::new(config.logging.level.as_str()))
        .with(fmt::layer().with_writer(non_blocking).with_ansi(false))
        .with(fmt::layer().with_writer(std::io::stderr))
        .init();

    std::mem::forget(_guard);
    Ok(())
}
