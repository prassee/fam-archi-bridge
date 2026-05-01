#![allow(dead_code)]
use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use pg_walstream::BufferReader;
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

        let mut relations: HashMap<u32, RelationInfo> = HashMap::new();

        let mut current_xid: u64 = 0;
        let mut current_commit_time: i64 = 0;
        let mut current_lsn: u64 = 0;

        // Use a BufferReader directly to iterate all messages contained in the
        // Bytes payload. This consumes messages sequentially until we've read
        // the entire buffer.
        let mut reader = BufferReader::from_bytes(wal_data.clone());

        while reader.remaining() > 0 {
            let msg_type = match reader.read_u8() {
                Ok(b) => b as char,
                Err(e) => return Err(anyhow::anyhow!("buffer read error: {}", e)),
            };

            match msg_type {
                'B' => {
                    let final_lsn = reader.read_u64()?;
                    let timestamp = reader.read_i64()?;
                    let xid = reader.read_u32()?;
                    current_lsn = final_lsn;
                    current_commit_time = timestamp as i64;
                    current_xid = xid as u64;
                }
                'C' => {
                    let _flags = reader.read_u8()?;
                    let commit_lsn = reader.read_u64()?;
                    let _end_lsn = reader.read_u64()?;
                    let timestamp = reader.read_i64()?;
                    current_lsn = commit_lsn;
                    current_commit_time = timestamp as i64;
                }
                'R' => {
                    let relation_id = reader.read_u32()?;
                    let namespace = reader.read_cstring()?;
                    let relation_name = reader.read_cstring()?;
                    let replica_identity = reader.read_u8()?;
                    let column_count = reader.read_u16()? as usize;

                    let mut columns = Vec::with_capacity(column_count);
                    for _ in 0..column_count {
                        let flags = reader.read_u8()?;
                        let name = reader.read_cstring()?;
                        let type_id = reader.read_u32()?;
                        let type_modifier = reader.read_i32()?;
                        // ColumnInfo is internal to pg_walstream::protocol; we
                        // reuse RelationInfo via its public constructor that
                        // accepts ColumnInfo (the types are exported). Build a
                        // ColumnInfo compatible struct using the public type.
                        columns.push(pg_walstream::ColumnInfo::new(flags, name, type_id, type_modifier));
                    }
                    let rel = RelationInfo::new(relation_id, namespace, relation_name, replica_identity, columns);
                    relations.insert(relation_id, rel);
                }
                'I' => {
                    let relation_id = reader.read_u32()?;
                    let tuple_type = reader.read_u8()? as char;
                    if tuple_type != 'N' {
                        return Err(anyhow::anyhow!("unexpected tuple type in INSERT: {}", tuple_type));
                    }
                    let row = parse_tuple_to_row(&mut reader, relations.get(&relation_id))?;
                    let (schema, name, oid) = relation_info_for(&relations, relation_id);
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
                'U' => {
                    let relation_id = reader.read_u32()?;
                    // optional old tuple
                    let mut old_row: Option<RowData> = None;
                    if reader.remaining() > 0 {
                        let peek = reader.peek_u8()? as char;
                        if peek == 'K' || peek == 'O' {
                            let _ = reader.read_u8()?; // consume type
                            old_row = Some(parse_tuple_to_row(&mut reader, relations.get(&relation_id))?);
                        }
                    }
                    let new_tuple_type = reader.read_u8()? as char;
                    if new_tuple_type != 'N' {
                        return Err(anyhow::anyhow!("unexpected new tuple type in UPDATE: {}", new_tuple_type));
                    }
                    let new_row = parse_tuple_to_row(&mut reader, relations.get(&relation_id))?;
                    let (schema, name, oid) = relation_info_for(&relations, relation_id);
                    records.push(WalRecord {
                        lsn: current_lsn,
                        table_schema: schema,
                        table_name: name,
                        operation: Operation::Update,
                        oid,
                        new_tuple: Some(new_row),
                        old_tuple: old_row,
                        tx_commit_time: current_commit_time,
                        tx_xid: current_xid,
                    });
                }
                'D' => {
                    let relation_id = reader.read_u32()?;
                    let _key_type = reader.read_u8()? as char;
                    let old_row = parse_tuple_to_row(&mut reader, relations.get(&relation_id))?;
                    let (schema, name, oid) = relation_info_for(&relations, relation_id);
                    records.push(WalRecord {
                        lsn: current_lsn,
                        table_schema: schema,
                        table_name: name,
                        operation: Operation::Delete,
                        oid,
                        new_tuple: None,
                        old_tuple: Some(old_row),
                        tx_commit_time: current_commit_time,
                        tx_xid: current_xid,
                    });
                }
                'T' => {
                    let relation_count = reader.read_u32()?;
                    let _flags = reader.read_u8()?;
                    for _ in 0..relation_count {
                        let rid = reader.read_u32()?;
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
                _ => {
                    // Unknown/unsupported message type: best-effort skip or break
                    break;
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

// tuple_to_rowdata removed - parser now uses parse_tuple_to_row(reader, relation)

fn parse_tuple_to_row(reader: &mut BufferReader, relation: Option<&RelationInfo>) -> Result<RowData> {
    // mirror parse_tuple_data logic from pg_walstream::protocol
    let column_count = reader.read_u16()? as usize;
    let mut cols: Vec<Column> = Vec::with_capacity(column_count);

    for idx in 0..column_count {
        let column_type = reader.read_u8()? as char;

        let (value, is_null, name, type_oid) = match column_type {
            'n' => (
                None,
                true,
                relation
                    .and_then(|r| r.get_column_by_index(idx).map(|c| c.name.to_string()))
                    .unwrap_or_else(|| format!("col{}", idx + 1)),
                relation
                    .and_then(|r| r.get_column_by_index(idx).map(|c| c.type_id))
                    .unwrap_or(0),
            ),
            'u' => (
                None,
                false,
                relation
                    .and_then(|r| r.get_column_by_index(idx).map(|c| c.name.to_string()))
                    .unwrap_or_else(|| format!("col{}", idx + 1)),
                relation
                    .and_then(|r| r.get_column_by_index(idx).map(|c| c.type_id))
                    .unwrap_or(0),
            ),
            't' => {
                let length = reader.read_u32()? as usize;
                let data = reader.read_bytes_buf(length)?;
                let s = match std::str::from_utf8(data.as_ref()) {
                    Ok(v) => v.to_string(),
                    Err(_) => String::from_utf8_lossy(data.as_ref()).into_owned(),
                };
                (
                    Some(ColumnValue::String(s)),
                    false,
                    relation
                        .and_then(|r| r.get_column_by_index(idx).map(|c| c.name.to_string()))
                        .unwrap_or_else(|| format!("col{}", idx + 1)),
                    relation
                        .and_then(|r| r.get_column_by_index(idx).map(|c| c.type_id))
                        .unwrap_or(0),
                )
            }
            'b' => {
                let length = reader.read_u32()? as usize;
                let data = reader.read_bytes_buf(length)?;
                (
                    Some(ColumnValue::Bytes(data.to_vec())),
                    false,
                    relation
                        .and_then(|r| r.get_column_by_index(idx).map(|c| c.name.to_string()))
                        .unwrap_or_else(|| format!("col{}", idx + 1)),
                    relation
                        .and_then(|r| r.get_column_by_index(idx).map(|c| c.type_id))
                        .unwrap_or(0),
                )
            }
            other => {
                return Err(anyhow::anyhow!("Unknown column data type: {}", other));
            }
        };

        cols.push(Column {
            name,
            type_oid,
            value,
            is_null,
        });
    }

    Ok(RowData { columns: cols })
}

impl Default for WalParser {
    fn default() -> Self {
        Self::new()
    }
}
