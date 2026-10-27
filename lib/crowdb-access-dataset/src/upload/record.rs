// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    decode, OperationId, UploadError, UploadRecord, UploadState, MAX_OWNERS, MAX_PARTS, MAX_RETIRED_OWNERS,
    MAX_UPLOAD_PART_BYTES,
};

// Storage encoding is independent of UploadRecord's public serde representation
// and logical version. Older JSON-only writers reject this prefix.
const MAGIC: &[u8; 8] = b"DSUP\0\0\0\x06";
const HEADER_BYTES: usize = 52;
const ID_BYTES: usize = 16;

pub(super) fn encode_session(record: &UploadRecord) -> Result<Vec<u8>, UploadError> {
    let size = encoded_size(record.owners.len(), record.retired_owners.len())?;
    let owners = u16::try_from(record.owners.len()).map_err(|_| UploadError::Integrity)?;
    let retired = u16::try_from(record.retired_owners.len()).map_err(|_| UploadError::Integrity)?;
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&record.version.to_le_bytes());
    bytes.push(match record.state {
        UploadState::Open => 0,
        UploadState::Complete => 1,
        UploadState::Claimed => 2,
        UploadState::Aborted => 3,
    });
    bytes.push(u8::from(record.cleanup_complete));
    bytes.extend_from_slice(&record.length.to_le_bytes());
    bytes.extend_from_slice(&record.part_size.to_le_bytes());
    bytes.extend_from_slice(&record.expires_at.to_le_bytes());
    bytes.extend_from_slice(&record.md5);
    bytes.extend_from_slice(&owners.to_le_bytes());
    bytes.extend_from_slice(&retired.to_le_bytes());
    for owner in record.owners.iter().chain(&record.retired_owners) {
        bytes.extend_from_slice(owner.as_bytes());
    }
    debug_assert_eq!(bytes.len(), size);
    Ok(bytes)
}

pub(super) fn decode_session(bytes: &[u8]) -> Result<UploadRecord, UploadError> {
    if bytes == crate::authority::dataset_lifecycle::RETIRED_RESOURCE {
        return Err(UploadError::Conflict);
    }
    let mut record = if bytes.starts_with(MAGIC) {
        decode_binary(bytes)?
    } else {
        decode(bytes)?
    };
    validate(&record)?;
    record.version = record.version.max(4);
    Ok(record)
}

#[cfg(feature = "test-util")]
impl UploadRecord {
    /// Decodes a stored upload resource while preserving its normal validation.
    /// # Errors
    /// Rejects unsupported, truncated or malformed resource records.
    pub fn decode_stored_for_tests(bytes: &[u8]) -> Result<Self, UploadError> {
        decode_session(bytes)
    }

    /// Encodes resource bytes without changing the public serde representation.
    /// # Errors
    /// Rejects owner counts beyond the stored resource limits.
    pub fn encode_stored_for_tests(&self) -> Result<Vec<u8>, UploadError> {
        encode_session(self)
    }
}

fn decode_binary(bytes: &[u8]) -> Result<UploadRecord, UploadError> {
    if bytes.len() < HEADER_BYTES {
        return Err(UploadError::Integrity);
    }
    let owners = usize::from(u16::from_le_bytes(array(&bytes[48..50])?));
    let retired = usize::from(u16::from_le_bytes(array(&bytes[50..52])?));
    if encoded_size(owners, retired)? != bytes.len() {
        return Err(UploadError::Integrity);
    }
    let state = match bytes[10] {
        0 => UploadState::Open,
        1 => UploadState::Complete,
        2 => UploadState::Claimed,
        3 => UploadState::Aborted,
        _ => return Err(UploadError::Integrity),
    };
    let cleanup_complete = match bytes[11] {
        0 => false,
        1 => true,
        _ => return Err(UploadError::Integrity),
    };
    let split = HEADER_BYTES + owners * ID_BYTES;
    Ok(UploadRecord {
        version: u16::from_le_bytes(array(&bytes[8..10])?),
        length: u64::from_le_bytes(array(&bytes[12..20])?),
        part_size: u32::from_le_bytes(array(&bytes[20..24])?),
        md5: array(&bytes[32..48])?,
        expires_at: u64::from_le_bytes(array(&bytes[24..32])?),
        state,
        parts: Vec::new(),
        owners: identities(&bytes[HEADER_BYTES..split]),
        retired_owners: identities(&bytes[split..]),
        cleanup_complete,
    })
}

fn encoded_size(owners: usize, retired: usize) -> Result<usize, UploadError> {
    if owners > MAX_OWNERS || retired > MAX_RETIRED_OWNERS {
        return Err(UploadError::Integrity);
    }
    // Both counts have been bounded before arithmetic or allocation.
    Ok(HEADER_BYTES + (owners + retired) * ID_BYTES)
}

fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], UploadError> {
    bytes.try_into().map_err(|_| UploadError::Integrity)
}

fn identities(bytes: &[u8]) -> Vec<OperationId> {
    bytes
        .chunks_exact(ID_BYTES)
        .map(|bytes| {
            let mut id = [0; ID_BYTES];
            id.copy_from_slice(bytes);
            OperationId::from_stored_bytes(id)
        })
        .collect()
}

fn validate(record: &UploadRecord) -> Result<(), UploadError> {
    if !matches!(record.version, 2..=5)
        || (record.version == 2 && !record.retired_owners.is_empty())
        || (record.version == 5 && record.retired_owners.len() != MAX_RETIRED_OWNERS)
        || record.retired_owners.len() > MAX_RETIRED_OWNERS
        || record.retired_owners.windows(2).any(|pair| pair[0] >= pair[1])
        || record
            .owners
            .iter()
            .any(|owner| record.retired_owners.binary_search(owner).is_ok())
        || record.length == 0
        || record.part_size == 0
        || record.part_size as usize > MAX_UPLOAD_PART_BYTES
        || record.length.div_ceil(u64::from(record.part_size)) > MAX_PARTS
        || record.owners.len() > MAX_OWNERS
    {
        return Err(UploadError::Integrity);
    }
    Ok(())
}
