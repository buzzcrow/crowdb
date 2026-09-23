use std::collections::BTreeMap;
use std::sync::Arc;

use super::{ContentFormat, FileBlockStore, FileIoError, FileReader, FileRecord, FormatHint};

mod codec;
mod input;
mod records;
mod schema;
pub use codec::AvroCodec;
use input::Input;
pub use records::{AvroDecodedBlock, AvroRecords};
pub use schema::{AvroDatumLimits, AvroSchema};

#[derive(Debug, thiserror::Error)]
pub enum AvroContainerError {
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error("invalid Avro container framing")]
    Framing,
    #[error("Avro container resource bound exceeded")]
    Bounds,
    #[error("Avro container reader previously failed or was cancelled")]
    Failed,
    #[error("unsupported Avro compression codec")]
    Codec,
    #[error("invalid Avro writer schema or binary datum")]
    Schema,
}

#[derive(Clone, Copy, Debug)]
pub struct AvroLimits {
    pub header_bytes: usize,
    pub metadata_entries: usize,
    pub block_bytes: usize,
    pub records_per_block: u64,
}

impl AvroLimits {
    fn validate(self) -> Result<(), AvroContainerError> {
        if self.header_bytes == 0
            || self.header_bytes > 1024 * 1024
            || self.metadata_entries == 0
            || self.metadata_entries > 1024
            || self.block_bytes == 0
            || self.block_bytes > 8 * 1024 * 1024
            || self.records_per_block == 0
            || self.records_per_block > 1_000_000
        {
            return Err(AvroContainerError::Bounds);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct AvroBlock {
    pub records: u64,
    pub payload: FormatHint,
    pub encoded: Vec<u8>,
}

pub struct AvroBlocks {
    input: Input,
    metadata: BTreeMap<String, Vec<u8>>,
    sync: Vec<u8>,
    limits: AvroLimits,
    header: FormatHint,
    failed: bool,
}

impl AvroBlocks {
    /// Opens bounded OCF framing without decoding schemas or compressed records.
    /// # Errors
    /// Rejects malformed framing, missing schema, invalid UTF-8 and resource excess.
    pub async fn open(
        store: Arc<dyn FileBlockStore>,
        record: FileRecord,
        limits: AvroLimits,
    ) -> Result<Self, AvroContainerError> {
        limits.validate()?;
        if record.format != ContentFormat::Avro {
            return Err(AvroContainerError::Framing);
        }
        let length = record.length;
        let mut input = Input::new(FileReader::new(store, record, None, 16 * 1024)?, length);
        input.end = length.min(limits.header_bytes as u64);
        if input.take(4).await? != b"Obj\x01" {
            return Err(AvroContainerError::Framing);
        }
        let metadata = read_metadata(&mut input, limits).await?;
        let schema = metadata.get("avro.schema").ok_or(AvroContainerError::Framing)?;
        if schema.is_empty()
            || std::str::from_utf8(schema).is_err()
            || metadata
                .get("avro.codec")
                .is_some_and(|codec| std::str::from_utf8(codec).is_err())
        {
            return Err(AvroContainerError::Framing);
        }
        let sync = input.take(16).await?;
        let header = FormatHint {
            offset: 0,
            length: input.position,
        };
        input.end = length;
        Ok(Self {
            input,
            metadata,
            sync,
            limits,
            header,
            failed: false,
        })
    }

    #[must_use]
    pub fn metadata(&self) -> &BTreeMap<String, Vec<u8>> {
        &self.metadata
    }

    #[must_use]
    pub fn header_hint(&self) -> FormatHint {
        self.header
    }

    #[must_use]
    pub fn codec(&self) -> &str {
        self.metadata
            .get("avro.codec")
            .and_then(|codec| std::str::from_utf8(codec).ok())
            .unwrap_or("null")
    }

    /// Returns one encoded block only when requested; callers own decoded limits.
    /// # Errors
    /// Permanently stops on malformed framing, sync mismatch, cancellation or excess.
    pub async fn next(&mut self) -> Result<Option<AvroBlock>, AvroContainerError> {
        if self.failed {
            return Err(AvroContainerError::Failed);
        }
        if self.input.position == self.input.end {
            return Ok(None);
        }
        self.failed = true;
        let records = u64::try_from(self.input.long().await?).map_err(|_| AvroContainerError::Framing)?;
        if records > self.limits.records_per_block {
            return Err(AvroContainerError::Bounds);
        }
        let length = self.input.size(self.limits.block_bytes).await?;
        let payload = FormatHint {
            offset: self.input.position,
            length: length as u64,
        };
        let encoded = self.input.take(length).await?;
        if self.input.take(16).await? != self.sync {
            return Err(AvroContainerError::Framing);
        }
        self.failed = false;
        Ok(Some(AvroBlock {
            records,
            payload,
            encoded,
        }))
    }
}

async fn read_metadata(
    input: &mut Input,
    limits: AvroLimits,
) -> Result<BTreeMap<String, Vec<u8>>, AvroContainerError> {
    let mut metadata = BTreeMap::new();
    loop {
        let count = input.long().await?;
        if count == 0 {
            return Ok(metadata);
        }
        let entries = count
            .checked_abs()
            .and_then(|count| usize::try_from(count).ok())
            .filter(|count| *count <= limits.metadata_entries - metadata.len())
            .ok_or(AvroContainerError::Bounds)?;
        let outer_end = input.end;
        let sized = count < 0;
        if sized {
            let size = input.size(limits.header_bytes).await?;
            input.end = input
                .position
                .checked_add(size as u64)
                .filter(|end| *end <= outer_end)
                .ok_or(AvroContainerError::Bounds)?;
        }
        for _ in 0..entries {
            let length = input.size(limits.header_bytes).await?;
            let key =
                String::from_utf8(input.take(length).await?).map_err(|_| AvroContainerError::Framing)?;
            let length = input.size(limits.header_bytes).await?;
            let value = input.take(length).await?;
            if metadata.insert(key, value).is_some() {
                return Err(AvroContainerError::Framing);
            }
        }
        if sized && input.position != input.end {
            return Err(AvroContainerError::Framing);
        }
        input.end = outer_end;
    }
}
