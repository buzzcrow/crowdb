// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{SplitError, MAX_SPLIT_RECORD_BYTES};
use serde::{de::DeserializeOwned, Serialize};

pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, SplitError> {
    let mut writer = BoundedWriter(Vec::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| SplitError::Limit)?;
    Ok(writer.0)
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SplitError> {
    if bytes.len() as u64 > MAX_SPLIT_RECORD_BYTES {
        return Err(SplitError::Limit);
    }
    // JSON's recursion guard rejects malicious recursive predicates before the
    // domain's tighter validation. Legacy bincode records remain untouched.
    serde_json::from_slice(bytes).map_err(|_| SplitError::InvalidRecord)
}

struct BoundedWriter(Vec<u8>);

impl std::io::Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) as u64 > MAX_SPLIT_RECORD_BYTES {
            return Err(std::io::Error::other("Split metadata limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
