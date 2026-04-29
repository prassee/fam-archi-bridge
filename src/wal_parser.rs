#![allow(dead_code)]
use anyhow::{Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

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

pub struct WalParser {
    decode_datum: fn(u32, &[u8], bool) -> Result<ColumnValue>,
}

impl WalParser {
    pub fn new() -> Self {
        Self {
            decode_datum: |_type_oid, _data, _is_toasted| -> Result<ColumnValue> {
                Ok(ColumnValue::Bytes(_data.to_vec()))
            },
        }
    }

    pub fn parse(&self, wal_data: &Bytes) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        self.parse_wal_data(wal_data, &mut records)?;
        Ok(records)
    }

    fn parse_wal_data(&self, wal_data: &Bytes, records: &mut Vec<WalRecord>) -> Result<()> {
        let mut offset = 0;
        while offset < wal_data.len() {
            let (consumed, rec) = self.parse_single_record(&wal_data[offset..])?;
            if let Some(rec) = rec {
                records.push(rec);
            }
            offset += consumed;
        }
        Ok(())
    }

    fn parse_single_record(&self, data: &[u8]) -> Result<(usize, Option<WalRecord>)> {
        if data.len() < 4 {
            return Ok((data.len(), None));
        }

        let msg_type = data[0];
        match msg_type {
            b'W' | b'I' | b'U' | b'D' | b'T' => self.parse_logical_decoded(data),
            b'B' => Ok((5, None)),
            _ => Ok((data.len(), None)),
        }
    }

    fn parse_logical_decoded(&self, data: &[u8]) -> Result<(usize, Option<WalRecord>)> {
        if data.len() < 25 {
            return Ok((0, None));
        }

        let lsn = u64::from_be_bytes([
            data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
        ]);
        let tx_xid = u64::from_be_bytes([
            data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16],
        ]);
        let tx_commit_time = i64::from_be_bytes([
            data[17], data[18], data[19], data[20], data[21], data[22], data[23], data[24],
        ]);

        let remaining = &data[25..];
        let mut pos = 0;

        let (table_schema, consumed) = self.read_cstring(remaining)?;
        pos += consumed;

        let (table_name, consumed) = self.read_cstring(&remaining[pos..])?;
        pos += consumed;

        let operation = match data[0] {
            b'I' => Operation::Insert,
            b'U' => Operation::Update,
            b'D' => Operation::Delete,
            b'T' => Operation::Truncate,
            _ => return Ok((pos + 25, None)),
        };

        let oid = if remaining.len() > pos + 4 {
            u32::from_be_bytes([
                remaining[pos],
                remaining[pos + 1],
                remaining[pos + 2],
                remaining[pos + 3],
            ])
        } else {
            0
        };

        Ok((
            pos + 25,
            Some(WalRecord {
                lsn,
                table_schema,
                table_name,
                operation,
                oid,
                new_tuple: None,
                old_tuple: None,
                tx_commit_time,
                tx_xid,
            }),
        ))
    }

    fn read_cstring(&self, data: &[u8]) -> Result<(String, usize)> {
        let end = data
            .iter()
            .position(|&b| b == 0)
            .context("Missing null terminator")?;
        let s = String::from_utf8_lossy(&data[..end]).to_string();
        Ok((s, end + 1))
    }

    #[allow(dead_code)]
    pub fn decode_tuple(&self, data: &[u8], type_oids: &[u32]) -> Result<RowData> {
        let mut columns = Vec::new();
        let mut offset = 0;

        for &type_oid in type_oids {
            let (value, consumed) = self.decode_column(data, offset, type_oid)?;
            offset += consumed;
            columns.push(Column {
                name: String::new(),
                type_oid,
                value: value.clone(),
                is_null: value.is_none(),
            });
        }

        Ok(RowData { columns })
    }

    #[allow(dead_code)]
    fn decode_column(
        &self,
        data: &[u8],
        offset: usize,
        type_oid: u32,
    ) -> Result<(Option<ColumnValue>, usize)> {
        if offset >= data.len() {
            return Ok((None, 0));
        }

        let null_flag = data[offset];
        if null_flag == b'N' {
            return Ok((None, 1));
        }

        let is_toasted = null_flag == b't';
        let value_data = &data[offset + 1..];

        let value = (self.decode_datum)(type_oid, value_data, is_toasted).ok();
        let consumed = 1 + value_data.len();

        Ok((value, consumed))
    }
}

impl Default for WalParser {
    fn default() -> Self {
        Self::new()
    }
}