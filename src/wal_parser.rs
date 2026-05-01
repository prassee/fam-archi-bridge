#![allow(dead_code)]
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use pg_walstream::LogicalReplicationParser;
use pg_walstream::LogicalReplicationMessage;
use pg_walstream::TupleData;
use pg_walstream::ColumnData;
use pg_walstream::RelationInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub lsn: u64,
    pub table_schema: String,
    pub table_name: String,
    pub operation: Operation,
    pub oid: u32,
    pub new_tuple: Option<RowData>,
    pub old_tuple: Option<RowData>,
    pub tx_commit_time: i64,
    pub tx_xid: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    Insert,
    Update,
    Delete,
    Truncate,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Insert => write!(f, "INSERT"),
            Operation::Update => write!(f, "UPDATE"),
            Operation::Delete => write!(f, "DELETE"),
            Operation::Truncate => write!(f, "TRUNCATE"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
    pub value: Option<ColumnValue>,
    pub is_null: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
}

pub struct WalParser {}

impl WalParser {
    pub fn new() -> Self {
        Self {}
    }

    /// Parse pgoutput bytes into WalRecord items. Attaches relation metadata
    /// where Relation messages were observed earlier in the same payload. The
    /// parser attempts to attach xid/commit_time/lsn when Begin/Commit are
    /// present in the payload.
    pub fn parse(&self, wal_data: &Bytes) -> Result<Vec<WalRecord>> {
        let mut records: Vec<WalRecord> = Vec::new();

        if wal_data.is_empty() {
            return Ok(records);
        }

        let mut parser = LogicalReplicationParser::with_protocol_version(1);
        let mut relations: HashMap<u32, RelationInfo> = HashMap::new();

        let mut current_xid: u64 = 0;
        let mut current_commit_time: i64 = 0;
        let mut current_lsn: u64 = 0;

        // Parse a single message from the provided bytes. In most replication
        // setups pgwire-replication will hand a single logical message per
        // payload. The parser currently doesn't expose the consumed offset, so
        // we parse once and return the records discovered.
        match parser.parse_wal_message_bytes(wal_data.clone()) {
            Ok(streaming) => {
                match streaming.message {
                    LogicalReplicationMessage::Relation(rel) => {
                        relations.insert(rel.rel_id(), rel.into());
                    }
                    LogicalReplicationMessage::Begin { final_lsn, commit_time_micros, xid } => {
                        current_lsn = final_lsn;
                        current_commit_time = commit_time_micros as i64;
                        current_xid = xid as u64;
                    }
                    LogicalReplicationMessage::Commit { flags: _, commit_lsn, commit_time_micros, xid: _ } => {
                        current_lsn = commit_lsn;
                        current_commit_time = commit_time_micros as i64;
                    }
                    LogicalReplicationMessage::Insert { relation_id, tuple } => {
                        let (schema, name, oid) = relation_info_for(&relations, relation_id);
                        let row = tuple_to_rowdata(&tuple, relations.get(&relation_id));
                        records.push(WalRecord {
                            lsn: current_lsn,
                            table_schema: schema,
                            table_name: name,
                            operation: Operation::Insert,
                            oid,
                            new_tuple: Some(row),
                            old_tuple: None,
                            tx_commit_time: current_commit_time,
                            tx_xid: current_xid,
                        });
                    }
                    LogicalReplicationMessage::Update { relation_id, old_tuple, new_tuple, .. } => {
                        let (schema, name, oid) = relation_info_for(&relations, relation_id);
                        let new_row = new_tuple.as_ref().map(|t| tuple_to_rowdata(t, relations.get(&relation_id)));
                        let old_row = old_tuple.as_ref().map(|t| tuple_to_rowdata(t, relations.get(&relation_id)));
                        records.push(WalRecord {
                            lsn: current_lsn,
                            table_schema: schema,
                            table_name: name,
                            operation: Operation::Update,
                            oid,
                            new_tuple: new_row,
                            old_tuple: old_row,
                            tx_commit_time: current_commit_time,
                            tx_xid: current_xid,
                        });
                    }
                    LogicalReplicationMessage::Delete { relation_id, old_tuple, .. } => {
                        let (schema, name, oid) = relation_info_for(&relations, relation_id);
                        let old_row = old_tuple.as_ref().map(|t| tuple_to_rowdata(t, relations.get(&relation_id)));
                        records.push(WalRecord {
                            lsn: current_lsn,
                            table_schema: schema,
                            table_name: name,
                            operation: Operation::Delete,
                            oid,
                            new_tuple: None,
                            old_tuple: old_row,
                            tx_commit_time: current_commit_time,
                            tx_xid: current_xid,
                        });
                    }
                    LogicalReplicationMessage::Truncate { relation_ids, .. } => {
                        for rid in relation_ids {
                            let (schema, name, oid) = relation_info_for(&relations, rid);
                            records.push(WalRecord {
                                lsn: current_lsn,
                                table_schema: schema.clone(),
                                table_name: name.clone(),
                                operation: Operation::Truncate,
                                oid,
                                new_tuple: None,
                                old_tuple: None,
                                tx_commit_time: current_commit_time,
                                tx_xid: current_xid,
                            });
                        }
                    }
                    _ => {}
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("pgoutput parse error: {}", e));
            }
        }

        Ok(records)
    }
}

fn relation_info_for(relations: &HashMap<u32, RelationInfo>, relid: u32) -> (String, String, u32) {
    if let Some(rel) = relations.get(&relid) {
        (
            rel.namespace().unwrap_or("public").to_string(),
            rel.name().to_string(),
            rel.rel_id(),
        )
    } else {
        ("public".to_string(), relid.to_string(), relid)
    }
}

fn tuple_to_rowdata(tuple: &TupleData, relation: Option<&RelationInfo>) -> RowData {
    let mut cols: Vec<Column> = Vec::new();

    for (idx, col) in tuple.columns().iter().enumerate() {
        let (name, type_oid) = if let Some(rel) = relation {
            if let Some(cinfo) = rel.columns().get(idx) {
                (cinfo.name().to_string(), cinfo.type_oid())
            } else {
                (format!("col{}", idx + 1), 0)
            }
        } else {
            (format!("col{}", idx + 1), 0)
        };

        let (value, is_null) = match col {
            ColumnData::Null => (None, true),
            ColumnData::Text(t) => (Some(ColumnValue::String(t.clone())), false),
            ColumnData::Binary(b) => (Some(ColumnValue::Bytes(b.clone())), false),
            ColumnData::Int16(v) => (Some(ColumnValue::Integer(*v as i64)), false),
            ColumnData::Int32(v) => (Some(ColumnValue::Integer(*v as i64)), false),
            ColumnData::Int64(v) => (Some(ColumnValue::Integer(*v)), false),
            ColumnData::Float32(v) => (Some(ColumnValue::Float(*v as f64)), false),
            ColumnData::Float64(v) => (Some(ColumnValue::Float(*v)), false),
            ColumnData::Bool(v) => (Some(ColumnValue::Boolean(*v)), false),
            ColumnData::Json(j) => (Some(ColumnValue::Json(j.clone())), false),
            ColumnData::Other(s) => (Some(ColumnValue::String(s.clone())), false),
        };

        cols.push(Column {
            name,
            type_oid,
            value,
            is_null,
        });
    }

    RowData { columns: cols }
}

impl Default for WalParser {
    fn default() -> Self {
        Self::new()
    }
}
