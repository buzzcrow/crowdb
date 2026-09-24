use super::{ColumnValue, Error, FileBlockStore, FileRecord, ParquetColumnReader, ParquetPageLimits};
use crate::file::{read_parquet_metadata, ParquetMetadataLimits};
use std::sync::Arc;

/// # Errors
/// Rejects malformed, non-required/non-integer columns and exhausted test row budgets.
pub async fn read_parquet_integer_column_for_tests(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    metadata_limits: ParquetMetadataLimits,
    page_limits: ParquetPageLimits,
    rows: usize,
) -> Result<Vec<i64>, Error> {
    let metadata = read_parquet_metadata(store.clone(), record, metadata_limits).await?;
    if metadata.schema.len() != 2 || metadata.schema[1].repetition != Some(0) {
        return Err(Error::Invalid);
    }
    let physical = metadata.schema[1].physical_type.ok_or(Error::Invalid)?;
    if !matches!(physical, 1 | 2) || metadata.rows > rows as u64 {
        return Err(Error::Bounds);
    }
    let mut values = Vec::new();
    for group in &metadata.groups {
        let column = group.columns.first().ok_or(Error::Invalid)?;
        let mut reader = ParquetColumnReader::new(store.clone(), record, column, physical, page_limits)?;
        while let Some(value) = reader.next().await? {
            let ColumnValue::Long(value) = value else {
                return Err(Error::Invalid);
            };
            if values.len() >= rows {
                return Err(Error::Bounds);
            }
            values.push(value);
        }
    }
    Ok(values)
}

/// # Errors
/// Rejects malformed or repeated columns and exhausted test row budgets.
pub async fn read_parquet_nullable_integer_column_for_tests(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    metadata_limits: ParquetMetadataLimits,
    page_limits: ParquetPageLimits,
    rows: usize,
) -> Result<Vec<Option<i64>>, Error> {
    let metadata = read_parquet_metadata(store.clone(), record, metadata_limits).await?;
    if metadata.rows > rows as u64 {
        return Err(Error::Bounds);
    }
    let mut values = Vec::new();
    for group in &metadata.groups {
        if group.columns.len() != 1 {
            return Err(Error::Invalid);
        }
        let column = &group.columns[0];
        let physical = metadata.schema[column.schema_index]
            .physical_type
            .ok_or(Error::Invalid)?;
        let mut reader = ParquetColumnReader::new(store.clone(), record, column, physical, page_limits)?;
        while let Some(value) = reader.next().await? {
            if values.len() >= rows {
                return Err(Error::Bounds);
            }
            values.push(match value {
                ColumnValue::Long(value) => Some(value),
                ColumnValue::Null => None,
                ColumnValue::Bytes(_) => return Err(Error::Invalid),
            });
        }
    }
    Ok(values)
}
