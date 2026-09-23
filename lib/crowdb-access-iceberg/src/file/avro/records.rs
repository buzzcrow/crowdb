use std::collections::BTreeMap;
use std::sync::Arc;

use super::{
    AvroBlocks, AvroCodec, AvroContainerError, AvroDatumLimits, AvroLimits, AvroSchema, FileBlockStore,
    FileRecord, FormatHint,
};

#[derive(Debug)]
pub struct AvroDecodedBlock {
    pub records: u64,
    pub payload: FormatHint,
    pub bytes: Vec<u8>,
}

pub struct AvroRecords {
    blocks: AvroBlocks,
    schema: AvroSchema,
    codec: AvroCodec,
    decoded_bytes: usize,
    limits: AvroDatumLimits,
    failed: bool,
}

impl AvroRecords {
    #[must_use]
    pub fn schema(&self) -> &AvroSchema {
        &self.schema
    }

    #[must_use]
    pub fn metadata(&self) -> &BTreeMap<String, Vec<u8>> {
        self.blocks.metadata()
    }

    #[must_use]
    pub fn header_hint(&self) -> FormatHint {
        self.blocks.header_hint()
    }

    /// Compiles the container's writer schema once, independently of Iceberg manifest semantics.
    /// # Errors
    /// Rejects invalid schemas, unsupported codecs and independently excessive resource bounds.
    pub async fn open(
        store: Arc<dyn FileBlockStore>,
        record: FileRecord,
        framing: AvroLimits,
        limits: AvroDatumLimits,
        decoded_bytes: usize,
    ) -> Result<Self, AvroContainerError> {
        limits.validate()?;
        if decoded_bytes == 0 || decoded_bytes > 8 * 1024 * 1024 {
            return Err(AvroContainerError::Bounds);
        }
        let blocks = AvroBlocks::open(store, record, framing).await?;
        let schema = AvroSchema::parse(
            blocks
                .metadata()
                .get("avro.schema")
                .ok_or(AvroContainerError::Schema)?,
        )?;
        let codec = AvroCodec::parse(blocks.codec())?;
        Ok(Self {
            blocks,
            schema,
            codec,
            decoded_bytes,
            limits,
            failed: false,
        })
    }

    /// Pulls one decoded block only after every declared datum has passed binary validation.
    /// # Errors
    /// Permanently stops after malformed data, storage/codec errors or cancelled reads.
    pub async fn next(&mut self) -> Result<Option<AvroDecodedBlock>, AvroContainerError> {
        if self.failed {
            return Err(AvroContainerError::Failed);
        }
        self.failed = true;
        let result = if let Some(block) = self.blocks.next().await? {
            let records = block.records;
            let payload = block.payload;
            let bytes = block.decode_validated(self.codec, self.decoded_bytes, &self.schema, self.limits)?;
            Some(AvroDecodedBlock {
                records,
                payload,
                bytes,
            })
        } else {
            None
        };
        self.failed = false;
        Ok(result)
    }
}
