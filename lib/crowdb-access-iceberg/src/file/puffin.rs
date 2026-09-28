use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;

use super::{
    probe_puffin_footer, ByteRange, FileBlockStore, FileIoError, FileLocation, FileReader, FileRecord,
    FormatHint, FormatProbeError,
};

mod bounds;
mod codec;

#[derive(Debug, thiserror::Error)]
pub enum PuffinMetadataError {
    #[error(transparent)]
    Probe(#[from] FormatProbeError),
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error("invalid Puffin metadata or compression frame")]
    Invalid,
    #[error("Puffin metadata resource bound exceeded")]
    Bounds,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PuffinMetadata {
    #[serde(deserialize_with = "bounds::sequence")]
    pub blobs: Vec<PuffinBlob>,
    #[serde(default, deserialize_with = "bounds::properties")]
    pub properties: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct PuffinBlob {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(deserialize_with = "bounds::sequence")]
    pub fields: Vec<i32>,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub offset: u64,
    pub length: u64,
    pub compression_codec: Option<String>,
    #[serde(default, deserialize_with = "bounds::properties")]
    pub properties: BTreeMap<String, String>,
}

/// Reads canonical footer metadata into independently bounded, memory-only structures.
/// # Errors
/// Rejects malformed framing, oversized compressed/decoded payloads and invalid blob spans.
pub async fn read_puffin_metadata(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    max_encoded_bytes: usize,
    max_decoded_bytes: usize,
) -> Result<PuffinMetadata, PuffinMetadataError> {
    if max_encoded_bytes == 0
        || max_encoded_bytes > 1024 * 1024
        || max_decoded_bytes == 0
        || max_decoded_bytes > 1024 * 1024
    {
        return Err(PuffinMetadataError::Bounds);
    }
    let footer = probe_puffin_footer(store.clone(), record).await?;
    if footer.payload.length > max_encoded_bytes as u64 {
        return Err(PuffinMetadataError::Bounds);
    }
    let range = ByteRange {
        start: footer.payload.offset,
        end: footer.payload.offset + footer.payload.length,
    };
    let mut reader = FileReader::new(store, record.clone(), Some(range), 16 * 1024)?;
    let mut bytes =
        Vec::with_capacity(usize::try_from(footer.payload.length).map_err(|_| PuffinMetadataError::Bounds)?);
    while let Some(frame) = reader.next().await? {
        bytes.extend(frame);
    }
    if footer.compressed {
        bytes = codec::decode(&bytes, max_decoded_bytes)?;
    }
    if bytes.len() > max_decoded_bytes {
        return Err(PuffinMetadataError::Bounds);
    }
    let metadata: PuffinMetadata =
        serde_json::from_slice(&bytes).map_err(|_| PuffinMetadataError::Invalid)?;
    metadata.validate(footer.payload.offset - 4)?;
    Ok(metadata)
}

impl PuffinMetadata {
    fn validate(&self, footer_start: u64) -> Result<(), PuffinMetadataError> {
        if self.blobs.len() > 4096 || !bounds::valid_properties(&self.properties) {
            return Err(PuffinMetadataError::Bounds);
        }
        let mut spans = Vec::with_capacity(self.blobs.len());
        for blob in &self.blobs {
            blob.validate(footer_start)?;
            if blob.length != 0 {
                spans.push((blob.offset, blob.offset + blob.length));
            }
        }
        spans.sort_unstable();
        if spans.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(PuffinMetadataError::Invalid);
        }
        Ok(())
    }

    /// Checks the exact manifest-to-footer descriptor; bitmap bytes need separate validation.
    /// # Errors
    /// Rejects missing or mismatched spans, referenced files and cardinalities.
    pub fn deletion_vector_at(
        &self,
        referenced: &FileLocation,
        span: FormatHint,
        cardinality: u64,
    ) -> Result<&PuffinBlob, PuffinMetadataError> {
        self.validate(u64::MAX)?;
        let blob = self
            .blobs
            .iter()
            .find(|blob| blob.offset == span.offset && blob.length == span.length)
            .ok_or(PuffinMetadataError::Invalid)?;
        if blob.kind != "deletion-vector-v1"
            || blob.cardinality()? != cardinality
            || blob.properties.get("referenced-data-file") != Some(&referenced.to_string())
        {
            return Err(PuffinMetadataError::Invalid);
        }
        blob.validate(u64::MAX)?;
        Ok(blob)
    }
}

impl PuffinBlob {
    fn validate(&self, footer_start: u64) -> Result<(), PuffinMetadataError> {
        if self.fields.len() > 4096 || !bounds::valid_properties(&self.properties) {
            return Err(PuffinMetadataError::Bounds);
        }
        if self.kind.is_empty()
            || self.kind.len() > 128
            || self.fields.iter().any(|field| *field <= 0)
            || self.offset < 4
            || self
                .offset
                .checked_add(self.length)
                .map_or(true, |end| end > footer_start)
            || self
                .compression_codec
                .as_deref()
                .is_some_and(|codec| codec != "lz4" && codec != "zstd")
        {
            return Err(PuffinMetadataError::Invalid);
        }
        if self.kind == "deletion-vector-v1" {
            if self.snapshot_id != -1
                || self.sequence_number != -1
                || self.compression_codec.is_some()
                || self.length < 20
                || self
                    .properties
                    .get("referenced-data-file")
                    .map_or(true, String::is_empty)
            {
                return Err(PuffinMetadataError::Invalid);
            }
            self.cardinality()?;
        }
        Ok(())
    }

    fn cardinality(&self) -> Result<u64, PuffinMetadataError> {
        let value = self
            .properties
            .get("cardinality")
            .ok_or(PuffinMetadataError::Invalid)?;
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(PuffinMetadataError::Invalid);
        }
        value
            .parse::<i64>()
            .ok()
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(PuffinMetadataError::Invalid)
    }
}
