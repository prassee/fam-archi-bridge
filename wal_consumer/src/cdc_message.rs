use serde::{Deserialize, Serialize};
use std::fmt;

/// CDC Change Data Capture message from wal-writer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcMessage {
    pub lsn: u64,
    pub table_schema: String,
    pub table_name: String,
    pub operation: String,
    pub oid: u32,
    pub new_tuple: Option<Tuple>,
    pub old_tuple: Option<Tuple>,
    pub tx_commit_time: i64,
    pub tx_xid: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tuple {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
    pub value: Option<serde_json::Value>,
    pub is_null: bool,
}

impl fmt::Display for CdcMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "[{}.{}] {} | LSN=0x{:X} XID={} OID={}",
            self.table_schema, self.table_name, self.operation, self.lsn, self.tx_xid, self.oid
        )?;

        if let Some(new_tuple) = &self.new_tuple {
            writeln!(f, "NEW_TUPLE:")?;
            for col in &new_tuple.columns {
                let val_str = if col.is_null {
                    "NULL".to_string()
                } else {
                    col.value
                        .as_ref()
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "?".to_string())
                };
                writeln!(f, "  {} (OID {}): {}", col.name, col.type_oid, val_str)?;
            }
        }

        if let Some(old_tuple) = &self.old_tuple {
            writeln!(f, "OLD_TUPLE:")?;
            for col in &old_tuple.columns {
                let val_str = if col.is_null {
                    "NULL".to_string()
                } else {
                    col.value
                        .as_ref()
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "?".to_string())
                };
                writeln!(f, "  {} (OID {}): {}", col.name, col.type_oid, val_str)?;
            }
        }

        Ok(())
    }
}
