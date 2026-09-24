use std::sync::Arc;

use super::{
    probe_parquet_footer, ByteRange, FileBlockStore, FileIoError, FileReader, FileRecord, FormatProbeError,
};

mod compact;
mod logical;
mod metadata;
mod pages;
pub use pages::ParquetPageLimits;
#[cfg(feature = "test-util")]
pub use pages::{read_parquet_integer_column_for_tests, read_parquet_nullable_integer_column_for_tests};
pub(crate) use pages::{ParquetColumnReader, ParquetColumnValue};
mod schema;

pub use logical::{ParquetLogicalType, ParquetTimeUnit};
pub use schema::ParquetSchemaElement;

#[derive(Clone, Copy, Debug)]
pub struct ParquetMetadataLimits {
    pub footer_bytes: usize,
    pub values: usize,
    pub depth: usize,
    pub schema_elements: usize,
    pub row_groups: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum ParquetMetadataError {
    #[error(transparent)]
    Probe(#[from] FormatProbeError),
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error("invalid Parquet compact metadata, schema or row-group totals")]
    Invalid,
    #[error("Parquet metadata resource limit exceeded")]
    Bounds,
    #[error("unsupported Parquet encryption, codec or encoding")]
    Unsupported,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ParquetMetadata {
    pub rows: u64,
    pub row_groups: usize,
    pub schema: Vec<ParquetSchemaElement>,
    pub groups: Vec<ParquetRowGroup>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ParquetRowGroup {
    pub rows: u64,
    pub columns: Vec<ParquetColumnChunk>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetColumnChunk {
    pub schema_index: usize,
    pub definition_level: u8,
    pub repeated: bool,
    pub offset: u64,
    pub length: u64,
    pub data_offset: u64,
    pub compression: i32,
    pub values: u64,
}

/// Decodes bounded plaintext footer metadata from canonical bytes, ignoring cached hints.
/// Checks structural schema and row-group totals, not data pages or Iceberg selected-use
/// type compatibility. No footer or column directory is persisted.
/// # Errors
/// Rejects malformed compact encoding, inconsistent totals and unsupported encryption.
pub async fn read_parquet_metadata(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    limits: ParquetMetadataLimits,
) -> Result<ParquetMetadata, ParquetMetadataError> {
    limits.validate()?;
    let footer = probe_parquet_footer(store.clone(), record).await?;
    if footer.length > limits.footer_bytes as u64 {
        return Err(ParquetMetadataError::Bounds);
    }
    let range = ByteRange {
        start: footer.offset,
        end: footer.offset + footer.length,
    };
    let mut reader = FileReader::new(store, record.clone(), Some(range), 16 * 1024)?;
    let mut bytes =
        Vec::with_capacity(usize::try_from(footer.length).map_err(|_| ParquetMetadataError::Bounds)?);
    while let Some(frame) = reader.next().await? {
        bytes.extend(frame);
    }
    metadata::decode(&bytes, footer.offset, limits)
}

impl ParquetMetadataLimits {
    fn validate(self) -> Result<(), ParquetMetadataError> {
        if self.footer_bytes == 0
            || self.footer_bytes > 1024 * 1024
            || self.values == 0
            || self.values > 100_000
            || self.depth == 0
            || self.depth > 32
            || self.schema_elements == 0
            || self.schema_elements > 4096
            || self.row_groups == 0
            || self.row_groups > 100_000
        {
            return Err(ParquetMetadataError::Bounds);
        }
        Ok(())
    }
}
