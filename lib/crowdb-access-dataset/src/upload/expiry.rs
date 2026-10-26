// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode_session, DatasetUploads, UploadError, UploadState};
use crate::{MetadataScan, MetadataScanBudget, ReadCancellation};

const PREFIX: &[u8] = b"DS\0U\0";

mod cursor;
mod discovery;
mod window;
pub use cursor::{UploadExpiryCursor, UploadExpiryStep};

// Qualified identity = length-prefixed namespace segments, 0xff, name;
// the upload ID follows it. Part and index records append six bytes.
fn is_session_key(key: &[u8]) -> Result<bool, UploadError> {
    let mut rest = key.strip_prefix(PREFIX).ok_or(UploadError::Integrity)?;
    while rest.first() != Some(&0xff) {
        rest = skip_name(rest)?;
    }
    rest = skip_name(&rest[1..])?;
    let id = rest.get(..16).ok_or(UploadError::Integrity)?;
    crate::OperationId::from_bytes(id).map_err(|_| UploadError::Integrity)?;
    match &rest[16..] {
        [] => Ok(true),
        [0, b'P' | b'I', _, _, _, _] => Ok(false),
        _ => Err(UploadError::Integrity),
    }
}

fn skip_name(bytes: &[u8]) -> Result<&[u8], UploadError> {
    let length: [u8; 2] = bytes
        .get(..2)
        .ok_or(UploadError::Integrity)?
        .try_into()
        .map_err(|_| UploadError::Integrity)?;
    let length = usize::from(u16::from_be_bytes(length));
    if length == 0 || length > 255 {
        return Err(UploadError::Integrity);
    }
    bytes.get(2 + length..).ok_or(UploadError::Integrity)
}
