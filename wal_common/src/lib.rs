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
}

impl AppConfig {
    pub fn from_env() -> Result<Self, env::VarError> {
        let state_dir = env::var("WAL_WRITER_STATE_DIR").ok().map(PathBuf::from);

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
                    .unwrap_or_else(|_| "16384".to_string())
                    .parse()
                    .unwrap_or(16384),
                compression: env::var("WAL_WRITER_KAFKA_COMPRESSION").ok(),
            },
            replication: ReplicationConfig {
                publication: env::var("WAL_WRITER_PUBLICATION")
                    .unwrap_or_else(|_| "wal_writer_publication".to_string()),
                ..Default::default()
            },
            logging: LoggingConfig::default(),
            state: StateConfig {
                directory: state_dir,
                persist_interval_secs: env::var("WAL_WRITER_STATE_PERSIST_INTERVAL_SECS")
                    .unwrap_or_else(|_| "60".to_string())
                    .parse()
                    .unwrap_or(60),
            },
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
    pub compression: Option<String>,
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
