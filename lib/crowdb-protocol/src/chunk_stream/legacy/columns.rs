// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{ChunkId, StreamDirectoryError, StreamName};

pub(super) struct LegacyColumns<'a> {
    remaining: &'a [u8],
}

impl<'a> LegacyColumns<'a> {
    pub(super) const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], StreamDirectoryError> {
        let head = self
            .remaining
            .get(..count)
            .ok_or(StreamDirectoryError::InvalidBounds)?;
        self.remaining = &self.remaining[count..];
        Ok(head)
    }

    pub(super) fn u64(&mut self) -> Result<u64, StreamDirectoryError> {
        number(self.take(8)?)
    }
    pub(super) fn u32(&mut self) -> Result<u32, StreamDirectoryError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| StreamDirectoryError::InvalidBounds)?,
        ))
    }
    pub(super) fn boolean(&mut self) -> Result<bool, StreamDirectoryError> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StreamDirectoryError::InvalidBounds),
        }
    }
    pub(super) fn name(&mut self) -> Result<StreamName, StreamDirectoryError> {
        Ok(StreamName {
            high: self.u64()?,
            low: self.u64()?,
        })
    }
    pub(super) fn chunk(&mut self) -> Result<ChunkId, StreamDirectoryError> {
        Ok(ChunkId {
            high: self.u64()?,
            low: self.u64()?,
        })
    }
    pub(super) fn sequence(
        &mut self,
        width: usize,
        expected: Option<usize>,
    ) -> Result<&'a [u8], StreamDirectoryError> {
        let count = usize::try_from(self.u64()?).map_err(|_| StreamDirectoryError::InvalidBounds)?;
        if expected.is_some_and(|expected| expected != count) {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        let bytes = count
            .checked_mul(width)
            .ok_or(StreamDirectoryError::InvalidBounds)?;
        self.take(bytes)
    }
    pub(super) fn finish(self) -> Result<(), StreamDirectoryError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(StreamDirectoryError::InvalidBounds)
        }
    }
}

pub(super) fn number(bytes: &[u8]) -> Result<u64, StreamDirectoryError> {
    Ok(u64::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| StreamDirectoryError::InvalidBounds)?,
    ))
}

pub(super) fn element(bytes: &[u8], index: usize, width: usize) -> Result<&[u8], StreamDirectoryError> {
    let start = index
        .checked_mul(width)
        .ok_or(StreamDirectoryError::InvalidBounds)?;
    let end = start
        .checked_add(width)
        .ok_or(StreamDirectoryError::InvalidBounds)?;
    bytes.get(start..end).ok_or(StreamDirectoryError::InvalidBounds)
}
