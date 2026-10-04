// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded decoding of actual immutable base frames for management inspection.

use crate::CtError;

#[derive(Debug)]
pub struct PageInspection {
    pub version: u64,
    pub root: u64,
    pub page: u64,
    pub deltas: u32,
    pub inner: bool,
    frame: Vec<u8>,
    count: usize,
    inline_deltas: usize,
}

#[derive(Debug)]
pub struct PageEntry<'a> {
    pub key: Option<&'a [u8]>,
    pub child: Option<u64>,
    pub cell: Option<&'a [u8]>,
    pub inline_delta: bool,
}

impl PageInspection {
    pub(crate) fn decode(
        version: u64,
        root: u64,
        page: u64,
        deltas: u32,
        frame: Vec<u8>,
    ) -> Result<Self, CtError> {
        if frame.len() < 72 || frame.len() > 1024 * 1024 || !matches!(frame[4], 1 | 2) {
            return Err(CtError::Corruption);
        }
        let inner = frame[4] == 2;
        let count = word(&frame, 8)? as usize;
        let inline_deltas = if inner { 0 } else { word(&frame, 20)? as usize };
        let value = Self {
            version,
            root,
            page,
            deltas,
            inner,
            frame,
            count,
            inline_deltas,
        };
        // A corrupt count must never turn management inspection into unbounded work.
        if value.len() > value.frame.len() / 8 {
            return Err(CtError::Corruption);
        }
        Ok(value)
    }

    pub fn len(&self) -> usize {
        self.count + if self.inner { 1 } else { self.inline_deltas }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn frame_bytes(&self) -> usize {
        self.frame.len()
    }
    pub fn fingerprint(&self) -> u32 {
        crate::crc::crc32c(&self.frame[..self.frame.len() - 8])
    }

    /// Resolves one structural child/separator or physical leaf record.
    pub fn entry(&self, index: usize) -> Result<PageEntry<'_>, CtError> {
        if index >= self.len() {
            return Err(CtError::InvalidArgument);
        }
        if self.inner {
            let key = if index == 0 {
                None
            } else {
                let slot = 64 + (self.count + 1) * 8 + (index - 1) * 8;
                Some(bytes(
                    &self.frame,
                    word(&self.frame, slot)? as usize,
                    word(&self.frame, slot + 4)? as usize,
                )?)
            };
            return Ok(PageEntry {
                key,
                child: Some(wide(&self.frame, 64 + index * 8)?),
                cell: None,
                inline_delta: false,
            });
        }
        let inline_delta = index >= self.count;
        let slot = if inline_delta {
            word(&self.frame, 12)? as usize + (index - self.count) * 12
        } else {
            64 + index * 12
        };
        let start = word(&self.frame, slot)? as usize;
        let length = word(&self.frame, slot + 4)? as usize;
        Ok(PageEntry {
            key: Some(bytes(&self.frame, start, length)?),
            child: None,
            cell: Some(bytes(
                &self.frame,
                start.checked_add(length).ok_or(CtError::Corruption)?,
                word(&self.frame, slot + 8)? as usize,
            )?),
            inline_delta,
        })
    }
}

fn bytes(frame: &[u8], offset: usize, length: usize) -> Result<&[u8], CtError> {
    frame
        .get(offset..offset.checked_add(length).ok_or(CtError::Corruption)?)
        .ok_or(CtError::Corruption)
}
fn word(frame: &[u8], offset: usize) -> Result<u32, CtError> {
    Ok(u32::from_le_bytes(
        bytes(frame, offset, 4)?
            .try_into()
            .map_err(|_| CtError::Corruption)?,
    ))
}
fn wide(frame: &[u8], offset: usize) -> Result<u64, CtError> {
    Ok(u64::from_le_bytes(
        bytes(frame, offset, 8)?
            .try_into()
            .map_err(|_| CtError::Corruption)?,
    ))
}
