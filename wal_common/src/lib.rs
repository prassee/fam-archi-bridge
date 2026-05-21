use serde::Deserialize;
use std::env;
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize)]
pub struct AppConfig {
    pub pg: PostgresConfig,
    pub kafka: KafkaConfig,
    pub replication: ReplicationConfig,
    pub logging: LoggingConfig,
    pub state: StateConfig,
    pub pending_batch_queue_size: usize,
    pub iceberg: Option<IcebergConfig>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, env::VarError> {
        let state_dir = env::var("WAL_WRITER_STATE_DIR").ok().map(PathBuf::from);
        let replication_batch_size = env::var("WAL_WRITER_REPLICATION_BATCH_SIZE")
            .unwrap_or_else(|_| "5000".to_string())
            .parse()
            .unwrap_or(5000);
        let replication_poll_interval_ms = env::var("WAL_WRITER_REPLICATION_POLL_INTERVAL_MS")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .unwrap_or(10);
        let logging_directory = env::var("WAL_WRITER_LOGGING_DIRECTORY")
            .unwrap_or_else(|_| "/var/log/wal-writer".to_string());
        let logging_level =
            env::var("WAL_WRITER_LOGGING_LEVEL").unwrap_or_else(|_| "info".to_string());

        let pending_batch_queue_size = env::var("WAL_WRITER_PENDING_BATCH_QUEUE_SIZE")
            .unwrap_or_else(|_| "4000".to_string())
            .parse()
            .unwrap_or(4000);

        Ok(Self {
            pg: PostgresConfig {
                host: env::var("WAL_WRITER_PG_HOST").unwrap_or_else(|_| "localhost".to_string()),
                port: env::var("WAL_WRITER_PG_PORT")
                    .unwrap_or_else(|_| "5432".to_string())
                    .parse()
                    .unwrap_or(5432),
                user: env::var("WAL_WRITER_PG_USER").unwrap_or_else(|_| "postgres".to_string()),
                password: env::var("WAL_WRITER_PG_PASSWORD").unwrap_or_else(|_| "".to_string()),
                database: env::var("WAL_WRITER_PG_DATABASE")
                    .unwrap_or_else(|_| "postgres".to_string()),
                slot_name: env::var("WAL_WRITER_PG_SLOT_NAME").ok(),
            },
            kafka: KafkaConfig {
                brokers: env::var("WAL_WRITER_KAFKA_BROKERS")
                    .unwrap_or_else(|_| "localhost:9092".to_string()),
                topic_prefix: env::var("WAL_WRITER_KAFKA_TOPIC_PREFIX")
                    .unwrap_or_else(|_| "cdc".to_string()),
                acks: env::var("WAL_WRITER_KAFKA_ACKS").unwrap_or_else(|_| "all".to_string()),
                linger_ms: env::var("WAL_WRITER_KAFKA_LINGER_MS")
                    .unwrap_or_else(|_| "5".to_string())
                    .parse()
                    .unwrap_or(5),
                batch_size: env::var("WAL_WRITER_KAFKA_BATCH_SIZE")
                    .unwrap_or_else(|_| "65536".to_string())
                    .parse()
                    .unwrap_or(65536),
                queue_buffering_max_ms: env::var("WAL_WRITER_KAFKA_QUEUE_BUFFERING_MAX_MS")
                    .unwrap_or_else(|_| "10".to_string())
                    .parse()
                    .unwrap_or(10),
                compression: env::var("WAL_WRITER_KAFKA_COMPRESSION").ok(),
                debug_no_kafka: env::var("WAL_WRITER_DEBUG_NO_KAFKA")
                    .map(|v| v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
                debug_print_wal: env::var("WAL_WRITER_DEBUG_PRINT_WAL")
                    .map(|v| v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
                publish_raw_wal: env::var("WAL_WRITER_PUBLISH_RAW_WAL")
                    .map(|v| v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
                max_publish_retries: env::var("WAL_WRITER_MAX_PUBLISH_RETRIES")
                    .unwrap_or_else(|_| "5".to_string())
                    .parse()
                    .unwrap_or(5),
                enable_dlq: env::var("WAL_WRITER_ENABLE_DLQ")
                    .map(|v| v.eq_ignore_ascii_case("true"))
                    .unwrap_or(true),
                num_publishers: env::var("WAL_WRITER_NUM_PUBLISHERS")
                    .unwrap_or_else(|_| "4".to_string())
                    .parse()
                    .unwrap_or(4),
            },
            replication: ReplicationConfig {
                publication: env::var("WAL_WRITER_PUBLICATION")
                    .unwrap_or_else(|_| "wal_writer_publication".to_string()),
                wal_position: None,
                batch_size: replication_batch_size,
                poll_interval_ms: replication_poll_interval_ms,
            },
            logging: LoggingConfig {
                directory: logging_directory,
                level: logging_level,
            },
            state: StateConfig {
                directory: state_dir,
                persist_interval_secs: env::var("WAL_WRITER_STATE_PERSIST_INTERVAL_SECS")
                    .unwrap_or_else(|_| "60".to_string())
                    .parse()
                    .unwrap_or(60),
            },
            pending_batch_queue_size,
            iceberg: IcebergConfig::from_env().ok(),
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub slot_name: Option<String>,
}

impl PostgresConfig {
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }

    pub fn slot_name(&self) -> &str {
        self.slot_name.as_deref().unwrap_or("wal_writer_slot")
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic_prefix: String,
    pub acks: String,
    pub linger_ms: u32,
    pub batch_size: u32,
    pub queue_buffering_max_ms: u32,
    pub compression: Option<String>,
    pub debug_no_kafka: bool,
    pub debug_print_wal: bool,
    pub publish_raw_wal: bool,
    pub max_publish_retries: u32,
    pub enable_dlq: bool,
    pub num_publishers: usize,
}

impl KafkaConfig {
    pub fn topic_prefix(&self) -> &str {
        &self.topic_prefix
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReplicationConfig {
    pub publication: String,
    pub wal_position: Option<String>,
    pub batch_size: u32,
    pub poll_interval_ms: u32,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            publication: String::from("wal_writer_publication"),
            wal_position: None,
            batch_size: 100,
            poll_interval_ms: 10,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct LoggingConfig {
    pub directory: String,
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            directory: String::from("/var/log/wal-writer"),
            level: String::from("info"),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct StateConfig {
    pub directory: Option<PathBuf>,
    pub persist_interval_secs: u64,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            directory: None,
            persist_interval_secs: 60,
        }
    }
}

/// Iceberg catalog configuration (for Phase 3 consumer)
#[derive(Clone, Debug, Deserialize)]
pub struct IcebergConfig {
    pub polaris_endpoint: String,
    pub polaris_realm: String,
    pub polaris_catalog: String,
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_database: String,
}

impl IcebergConfig {
    pub fn from_env() -> Result<Self, env::VarError> {
        Ok(Self {
            polaris_endpoint: env::var("WAL_CONSUMER_POLARIS_ENDPOINT")
                .unwrap_or_else(|_| "http://polaris:8181".to_string()),
            polaris_realm: env::var("WAL_CONSUMER_POLARIS_REALM")
                .unwrap_or_else(|_| "POLARIS".to_string()),
            polaris_catalog: env::var("WAL_CONSUMER_POLARIS_CATALOG")
                .unwrap_or_else(|_| "quickstart_catalog".to_string()),
            pg_host: env::var("WAL_CONSUMER_PG_HOST").unwrap_or_else(|_| "localhost".to_string()),
            pg_port: env::var("WAL_CONSUMER_PG_PORT")
                .unwrap_or_else(|_| "5432".to_string())
                .parse()
                .unwrap_or(5432),
            pg_user: env::var("WAL_CONSUMER_PG_USER").unwrap_or_else(|_| "postgres".to_string()),
            pg_password: env::var("WAL_CONSUMER_PG_PASSWORD").unwrap_or_else(|_| "".to_string()),
            pg_database: env::var("WAL_CONSUMER_PG_DATABASE")
                .unwrap_or_else(|_| "postgres".to_string()),
        })
    }

    pub fn pg_connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.pg_host, self.pg_port, self.pg_user, self.pg_password, self.pg_database
        )
    }
}
