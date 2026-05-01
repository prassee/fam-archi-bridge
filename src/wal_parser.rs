#![allow(dead_code)]
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use pg_walstream::LogicalReplicationParser;
use pg_walstream::LogicalReplicationMessage;
use pg_walstream::TupleData;
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

        // Parse messages repeatedly from the provided bytes. The parser works
        // with a BufferReader internally — to iterate multiple messages we use
        // BufferReader by creating a copy and repeatedly parsing from the
        // remaining slice. The parse_wal_message_bytes helper expects a Bytes
        // value; we emulate streaming by slicing the Bytes as we consume data.
        let mut buf = wal_data.clone();
        loop {
            if buf.is_empty() {
                break;
            }

            match parser.parse_wal_message_bytes(buf.clone()) {
                Ok(streaming) => {
                    match streaming.message {
                        LogicalReplicationMessage::Relation { relation_id, namespace, relation_name, replica_identity, columns } => {
                            let rel = RelationInfo::new(relation_id, namespace, relation_name, replica_identity, columns);
                            relations.insert(relation_id, rel);
                        }
                        LogicalReplicationMessage::Begin { final_lsn, timestamp, xid } => {
                            current_lsn = final_lsn;
                            current_commit_time = timestamp as i64;
                            current_xid = xid as u64;
                        }
                        LogicalReplicationMessage::Commit { flags: _, commit_lsn, end_lsn: _, timestamp } => {
                            current_lsn = commit_lsn;
                            current_commit_time = timestamp as i64;
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
                            let new_row = Some(tuple_to_rowdata(&new_tuple, relations.get(&relation_id)));
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
                            let old_row = Some(tuple_to_rowdata(&old_tuple, relations.get(&relation_id)));
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

                    // The parser consumes from the start of the Bytes but does
                    // not expose how many bytes were consumed. The streaming
                    // helper (BufferReader) places the remaining unread bytes in
                    // reader; however parse_wal_message_bytes hides that. To
                    // advance we detect whether there are more messages by
                    // attempting to parse the remainder using the internal
                    // streaming flag. Practically, replication sends single
                    // messages per payload so we break after the first one.
                    // If multi-message payloads are required, we should use the
                    // lower-level BufferReader API directly. For now break.
                    break;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("pgoutput parse error: {}", e));
                }
            }
        }

        Ok(records)
    }
}

fn relation_info_for(relations: &HashMap<u32, RelationInfo>, relid: u32) -> (String, String, u32) {
    if let Some(rel) = relations.get(&relid) {
        (
            rel.namespace.to_string(),
            rel.relation_name.to_string(),
            rel.relation_id,
        )
    } else {
        ("public".to_string(), relid.to_string(), relid)
    }
}

fn tuple_to_rowdata(tuple: &TupleData, relation: Option<&RelationInfo>) -> RowData {
    let mut cols: Vec<Column> = Vec::new();

    for (idx, col) in tuple.columns.iter().enumerate() {
        let (name, type_oid) = if let Some(rel) = relation {
            if let Some(cinfo) = rel.get_column_by_index(idx) {
                (cinfo.name.to_string(), cinfo.type_id)
            } else {
                (format!("col{}", idx + 1), 0)
            }
        } else {
            (format!("col{}", idx + 1), 0)
        };

        let (value, is_null) = if col.is_null() {
            (None, true)
        } else if col.is_text() {
            (col.as_string().map(ColumnValue::String), false)
        } else if col.is_binary() {
            (Some(ColumnValue::Bytes(col.raw_bytes().to_vec())), false)
        } else {
            // Fallback to string if possible
            (col.as_string().map(ColumnValue::String), false)
        };

        cols.push(Column { name, type_oid, value, is_null });
    }

    RowData { columns: cols }
}

impl Default for WalParser {
    fn default() -> Self {
        Self::new()
    }
}
