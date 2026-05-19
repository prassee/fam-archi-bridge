use crate::cdc_message::CdcMessage;
use anyhow::{anyhow, Result};
use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

/// Maps PostgreSQL type OID to Arrow DataType
pub fn pg_oid_to_arrow_type(oid: u32) -> DataType {
    match oid {
        16 => DataType::Boolean,  // boolean
        17 => DataType::Binary,   // bytea
        20 => DataType::Int64,    // bigint
        21 => DataType::Int32,    // smallint
        23 => DataType::Int32,    // integer
        25 => DataType::Utf8,     // text
        700 => DataType::Float32, // real
        701 => DataType::Float64, // double precision
        1700 => DataType::Utf8,   // numeric (stored as string)
        1114 => DataType::Utf8,   // timestamp without time zone
        1184 => DataType::Utf8,   // timestamp with time zone
        2950 => DataType::Utf8,   // uuid
        _ => DataType::Utf8,      // default to text for unknown types
    }
}

/// Row metadata for CDC operations
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RowMetadata {
    pub operation: String,
    pub lsn: u64,
    pub tx_xid: u64,
    pub tx_commit_time: i64,
}

/// Accumulates rows for a single table, building Arrow RecordBatches
pub struct TableBuffer {
    schema: Arc<Schema>,
    builders: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
    row_metadata: Vec<RowMetadata>,
}

impl TableBuffer {
    /// Create a new table buffer from schema discovered from first CDC message
    pub fn from_cdc_message(msg: &CdcMessage) -> Result<Self> {
        // Infer schema from new_tuple (for INSERT/UPDATE) or old_tuple (for DELETE)
        let tuple = msg
            .new_tuple
            .as_ref()
            .or(msg.old_tuple.as_ref())
            .ok_or_else(|| anyhow!("No tuple data for schema inference"))?;

        // Build Arrow fields and corresponding builders
        let mut fields = Vec::new();
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

        for col in &tuple.columns {
            let arrow_type = pg_oid_to_arrow_type(col.type_oid);
            fields.push(Field::new(&col.name, arrow_type.clone(), true)); // nullable

            // Create corresponding builder
            let builder: Box<dyn ArrayBuilder> = match arrow_type {
                DataType::Boolean => Box::new(BooleanBuilder::new()),
                DataType::Int32 => Box::new(Int32Builder::new()),
                DataType::Int64 => Box::new(Int64Builder::new()),
                DataType::Float32 => Box::new(Float64Builder::new()), // promote to f64
                DataType::Float64 => Box::new(Float64Builder::new()),
                DataType::Utf8 => Box::new(StringBuilder::new()),
                DataType::Binary => Box::new(StringBuilder::new()), // placeholder
                _ => Box::new(StringBuilder::new()),
            };
            builders.push(builder);
        }

        let schema = Arc::new(Schema::new(fields));

        Ok(TableBuffer {
            schema,
            builders,
            row_count: 0,
            row_metadata: Vec::new(),
        })
    }

    /// Get the schema
    #[allow(dead_code)]
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Get number of accumulated rows
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Add a row from CdcMessage (prioritize new_tuple, fallback to old_tuple)
    pub fn add_row(&mut self, msg: &CdcMessage) -> Result<()> {
        let tuple = msg
            .new_tuple
            .as_ref()
            .or(msg.old_tuple.as_ref())
            .ok_or_else(|| anyhow!("No tuple data in CDC message"))?;

        if tuple.columns.len() != self.builders.len() {
            return Err(anyhow!(
                "Column count mismatch: expected {}, got {}",
                self.builders.len(),
                tuple.columns.len()
            ));
        }

        // Add each column value to its corresponding builder
        for (idx, col) in tuple.columns.iter().enumerate() {
            append_value_to_builder(
                &mut self.builders[idx],
                &col.value,
                col.is_null,
                &self.schema.field(idx).data_type(),
            )?;
        }

        self.row_count += 1;
        self.row_metadata.push(RowMetadata {
            operation: msg.operation.clone(),
            lsn: msg.lsn,
            tx_xid: msg.tx_xid,
            tx_commit_time: msg.tx_commit_time,
        });

        Ok(())
    }

    /// Build a RecordBatch from accumulated rows, consuming this buffer
    pub fn build(mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();

        RecordBatch::try_new(self.schema, arrays).map_err(|e| anyhow!("RecordBatch error: {}", e))
    }

    /// Get row metadata (operation, LSN, etc.)
    #[allow(dead_code)]
    pub fn row_metadata(&self) -> &[RowMetadata] {
        &self.row_metadata
    }
}

/// Append a value to the appropriate builder
fn append_value_to_builder(
    builder: &mut Box<dyn ArrayBuilder>,
    value: &Option<Value>,
    is_null: bool,
    arrow_type: &DataType,
) -> Result<()> {
    if is_null {
        // All builders support appending null
        if let Some(bool_builder) = builder.as_any_mut().downcast_mut::<BooleanBuilder>() {
            bool_builder.append_null();
        } else if let Some(i32_builder) = builder.as_any_mut().downcast_mut::<Int32Builder>() {
            i32_builder.append_null();
        } else if let Some(i64_builder) = builder.as_any_mut().downcast_mut::<Int64Builder>() {
            i64_builder.append_null();
        } else if let Some(f64_builder) = builder.as_any_mut().downcast_mut::<Float64Builder>() {
            f64_builder.append_null();
        } else if let Some(str_builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
            str_builder.append_null();
        }
        return Ok(());
    }

    // Handle non-null values
    let val = value
        .as_ref()
        .ok_or_else(|| anyhow!("Value is None but is_null is false"))?;

    match arrow_type {
        DataType::Boolean => {
            let bool_builder = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| anyhow!("Expected BooleanBuilder"))?;
            let b = val
                .as_bool()
                .ok_or_else(|| anyhow!("Expected boolean value"))?;
            bool_builder.append_value(b);
        }
        DataType::Int32 => {
            let i32_builder = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .ok_or_else(|| anyhow!("Expected Int32Builder"))?;
            let i = val
                .as_i64()
                .ok_or_else(|| anyhow!("Expected integer value"))? as i32;
            i32_builder.append_value(i);
        }
        DataType::Int64 => {
            let i64_builder = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| anyhow!("Expected Int64Builder"))?;
            let i = val.as_i64().ok_or_else(|| anyhow!("Expected i64 value"))?;
            i64_builder.append_value(i);
        }
        DataType::Float32 | DataType::Float64 => {
            let f64_builder = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| anyhow!("Expected Float64Builder"))?;
            let f = val
                .as_f64()
                .or_else(|| val.as_i64().map(|i| i as f64))
                .ok_or_else(|| anyhow!("Expected numeric value"))?;
            f64_builder.append_value(f);
        }
        DataType::Utf8 | DataType::Binary => {
            let str_builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| anyhow!("Expected StringBuilder"))?;
            let s = val.to_string();
            str_builder.append_value(&s);
        }
        _ => {
            let str_builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| anyhow!("Expected StringBuilder for fallback"))?;
            str_builder.append_value(val.to_string());
        }
    }

    Ok(())
}

/// Converter managing Arrow RecordBatches per table
pub struct ArrowConverter {
    buffers: HashMap<String, TableBuffer>, // Key: schema.table
}

impl ArrowConverter {
    pub fn new() -> Self {
        Self {
            buffers: HashMap::new(),
        }
    }

    /// Add a CDC message, buffering the row
    pub fn add_message(&mut self, msg: &CdcMessage) -> Result<()> {
        let table_key = format!("{}.{}", msg.table_schema, msg.table_name);

        if !self.buffers.contains_key(&table_key) {
            let buffer = TableBuffer::from_cdc_message(msg)?;
            debug!("Created buffer for table: {}", table_key);
            self.buffers.insert(table_key.clone(), buffer);
        }

        let buffer = self
            .buffers
            .get_mut(&table_key)
            .ok_or_else(|| anyhow!("Buffer missing for table: {}", table_key))?;

        match msg.operation.as_str() {
            "INSERT" | "UPDATE" => {
                // Both INSERT and UPDATE add a new row (Iceberg-style merge on read)
                buffer.add_row(msg)?;
            }
            "DELETE" => {
                // For DELETE, add the old_tuple as a deletion marker
                buffer.add_row(msg)?;
            }
            "TRUNCATE" => {
                // For TRUNCATE, clear the buffer
                debug!("TRUNCATE operation on table: {}", table_key);
                self.buffers.remove(&table_key);
            }
            op => {
                warn!("Unknown operation type: {}", op);
            }
        }

        Ok(())
    }

    /// Build and flush RecordBatch for a specific table
    pub fn flush_table(&mut self, table_key: &str) -> Result<Option<RecordBatch>> {
        if let Some(buffer) = self.buffers.remove(table_key) {
            let batch = buffer.build()?;
            debug!("Flushed {} rows for table: {}", batch.num_rows(), table_key);
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    /// Flush all tables and return RecordBatches
    pub fn flush_all(&mut self) -> Result<HashMap<String, RecordBatch>> {
        let mut result = HashMap::new();
        let table_keys: Vec<_> = self.buffers.keys().cloned().collect();

        for key in table_keys {
            if let Some(batch) = self.flush_table(&key)? {
                result.insert(key, batch);
            }
        }

        Ok(result)
    }

    /// Get current buffer row count for a table
    pub fn table_row_count(&self, table_key: &str) -> usize {
        self.buffers
            .get(table_key)
            .map(|b| b.row_count())
            .unwrap_or(0)
    }

    /// Get list of tables with buffered data
    pub fn buffered_tables(&self) -> Vec<String> {
        self.buffers.keys().cloned().collect()
    }
}

impl Default for ArrowConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_oid_to_arrow_type() {
        assert_eq!(pg_oid_to_arrow_type(16), DataType::Boolean);
        assert_eq!(pg_oid_to_arrow_type(20), DataType::Int64);
        assert_eq!(pg_oid_to_arrow_type(23), DataType::Int32);
        assert_eq!(pg_oid_to_arrow_type(25), DataType::Utf8);
        assert_eq!(pg_oid_to_arrow_type(701), DataType::Float64);
        assert_eq!(pg_oid_to_arrow_type(9999), DataType::Utf8); // unknown defaults to Utf8
    }
}
