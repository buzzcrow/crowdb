// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Protocol-neutral part selection and reservation invariants.

/// Durable multipart transition phase shared by S3 and Iceberg.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MultipartPhase {
    Open,
    Completing,
    Publishing,
    Published,
    Aborted,
    Conflicted,
}

/// Protocol-neutral admission bounds for a durable multipart upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MultipartBounds {
    pub max_parts: u16,
    pub max_part_bytes: u64,
    pub max_object_bytes: u64,
    pub max_staged_bytes: u64,
}

impl MultipartBounds {
    #[must_use]
    pub const fn valid(self) -> bool {
        self.max_parts > 0
            && self.max_parts <= 10_000
            && self.max_part_bytes > 0
            && self.max_part_bytes <= self.max_object_bytes
            && self.max_object_bytes <= self.max_staged_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SelectedPart {
    pub number: u16,
    pub revision: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartAccounting {
    pub count: u16,
    pub staged_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StateError {
    #[error("multipart selection has no parts")]
    EmptySelection,
    #[error("multipart part number is invalid or out of order")]
    InvalidPartNumber,
    #[error("multipart selected revision is zero")]
    InvalidRevision,
    #[error("multipart part count exceeds its limit")]
    PartLimit,
    #[error("multipart staged byte total exceeds its limit")]
    StagedLimit,
    #[error("multipart part counters are inconsistent")]
    InvalidAccounting,
}

/// Validates one ordered completion selection independent of wire format.
///
/// # Errors
/// Rejects empty, duplicate, descending, oversized or zero-revision entries.
pub fn validate_selected_parts(parts: &[SelectedPart], max_parts: u16) -> Result<(), StateError> {
    if parts.is_empty() {
        return Err(StateError::EmptySelection);
    }
    if parts.len() > usize::from(max_parts.min(10_000)) {
        return Err(StateError::PartLimit);
    }
    let mut previous = 0;
    for part in parts {
        if part.number <= previous || part.number > max_parts.min(10_000) {
            return Err(StateError::InvalidPartNumber);
        }
        if part.revision == 0 {
            return Err(StateError::InvalidRevision);
        }
        previous = part.number;
    }
    Ok(())
}

/// Computes the counters to fence one new or replacement part publication.
///
/// # Errors
/// Rejects count, byte and arithmetic limit violations before any metadata
/// mutation. A replacement keeps the part count and subtracts its old bytes.
pub fn reserve_part_accounting(
    current: PartAccounting,
    old_length: Option<u64>,
    new_length: u64,
    max_parts: u16,
    max_staged_bytes: u64,
) -> Result<PartAccounting, StateError> {
    let count = current
        .count
        .checked_add(u16::from(old_length.is_none()))
        .filter(|count| *count <= max_parts.min(10_000))
        .ok_or(StateError::PartLimit)?;
    let staged_bytes = current
        .staged_bytes
        .checked_sub(old_length.unwrap_or(0))
        .ok_or(StateError::InvalidAccounting)?
        .checked_add(new_length)
        .ok_or(StateError::StagedLimit)?;
    if staged_bytes > max_staged_bytes {
        return Err(StateError::StagedLimit);
    }
    Ok(PartAccounting { count, staged_bytes })
}
