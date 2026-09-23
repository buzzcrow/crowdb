use std::sync::Arc;

use super::{
    read_puffin_metadata, ByteRange, FileBlockStore, FileIoError, FileLocation, FileReader, FileRecord,
    FormatHint, PuffinMetadataError,
};

mod bitmap;
mod input;

#[derive(Clone, Debug)]
pub struct DeletionVectorReference {
    pub referenced: FileLocation,
    pub span: FormatHint,
    pub cardinality: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct DeletionVectorLimits {
    pub blob_bytes: u64,
    pub bitmaps: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeletionVectorStats {
    pub cardinality: u64,
    pub maximum_position: Option<u64>,
    pub bitmaps: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum DeletionVectorError {
    #[error(transparent)]
    Metadata(#[from] PuffinMetadataError),
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error("invalid deletion vector framing, bitmap or checksum")]
    Invalid,
    #[error("deletion vector byte or bitmap limit exceeded")]
    Bounds,
}

/// Validates one canonical descriptor and streams its bitmap without collecting deleted positions.
/// # Errors
/// Rejects foreign descriptors, malformed bitmap containers, count mismatches and bad checksums.
pub async fn validate_deletion_vector(
    store: Arc<dyn FileBlockStore>,
    record: &FileRecord,
    reference: &DeletionVectorReference,
    limits: DeletionVectorLimits,
) -> Result<DeletionVectorStats, DeletionVectorError> {
    if limits.blob_bytes < 20
        || limits.blob_bytes > u64::from(u32::MAX) + 8
        || limits.bitmaps == 0
        || limits.bitmaps > 1_000_000
        || reference.span.length > limits.blob_bytes
    {
        return Err(DeletionVectorError::Bounds);
    }
    if record.location.table() != reference.referenced.table() {
        return Err(DeletionVectorError::Invalid);
    }
    let metadata = read_puffin_metadata(store.clone(), record, 1024 * 1024, 1024 * 1024).await?;
    metadata.deletion_vector_at(&reference.referenced, reference.span, reference.cardinality)?;
    let end = reference
        .span
        .offset
        .checked_add(reference.span.length)
        .ok_or(DeletionVectorError::Invalid)?;
    let reader = FileReader::new(
        store,
        record.clone(),
        Some(ByteRange {
            start: reference.span.offset,
            end,
        }),
        16 * 1024,
    )?;
    let mut input = input::Input::new(reader, reference.span.length);
    let length = u64::from(u32::from_be_bytes(input.take::<4>().await?));
    if length.checked_add(8) != Some(reference.span.length) {
        return Err(DeletionVectorError::Invalid);
    }
    if input.take::<4>().await? != [0xd1, 0xd3, 0x39, 0x64] {
        return Err(DeletionVectorError::Invalid);
    }
    let stats = bitmaps(&mut input, limits.bitmaps).await?;
    let actual_crc = input.crc();
    let expected_crc = u32::from_be_bytes(input.take::<4>().await?);
    if input.position != reference.span.length
        || actual_crc != expected_crc
        || stats.cardinality != reference.cardinality
    {
        return Err(DeletionVectorError::Invalid);
    }
    Ok(stats)
}

async fn bitmaps(input: &mut input::Input, limit: u32) -> Result<DeletionVectorStats, DeletionVectorError> {
    let count = input.u64().await?;
    if count > u64::from(limit) {
        return Err(DeletionVectorError::Bounds);
    }
    let mut stats = DeletionVectorStats {
        cardinality: 0,
        maximum_position: None,
        bitmaps: u32::try_from(count).map_err(|_| DeletionVectorError::Bounds)?,
    };
    let mut previous = None;
    for _ in 0..count {
        let key = input.u32().await?;
        if key > i32::MAX as u32 || previous.is_some_and(|previous| key <= previous) {
            return Err(DeletionVectorError::Invalid);
        }
        previous = Some(key);
        let bitmap = bitmap::validate(input).await?;
        stats.cardinality = stats
            .cardinality
            .checked_add(bitmap.cardinality)
            .ok_or(DeletionVectorError::Bounds)?;
        if let Some(maximum) = bitmap.maximum {
            stats.maximum_position = Some((u64::from(key) << 32) | u64::from(maximum));
        }
    }
    Ok(stats)
}
