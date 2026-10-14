// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Incremental Dataset batch framing. Completion requires the final frame and EOF.

use bytes::{Buf, Bytes, BytesMut};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::Error;

pub const BATCH_MAGIC: &[u8; 8] = b"CDBDS001";
pub const MAX_DATA_FRAME: usize = 4 * 1024 * 1024;
pub const MAX_HEADER_FRAME: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldState {
    Value,
    Missing,
    Tombstone,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BatchField {
    pub sample: String,
    pub field: String,
    pub state: FieldState,
    pub length: u64,
    #[serde(deserialize_with = "required_option")]
    pub md5: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BatchHeader {
    pub snapshot: String,
    pub sample_ids: Vec<String>,
    pub fields: Vec<BatchField>,
    #[serde(deserialize_with = "required_option")]
    pub next: Option<String>,
    pub end: bool,
}

#[derive(Debug)]
pub enum BatchEvent {
    Header(Arc<BatchHeader>),
    Data(Bytes),
    FieldEnd,
    End,
}

/// The caller admits input before allocation and drains each event before
/// fetching more bytes. Payload ownership transfers to each emitted event.
#[derive(Default)]
pub struct BatchDecoder {
    magic: bool,
    header: Option<Arc<BatchHeader>>,
    field: usize,
    length: u64,
    hash: Md5,
    ended: bool,
    failed: bool,
}

impl BatchDecoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// # Errors
    /// Rejects unknown versions, malformed framing, bounds and integrity failures.
    pub fn decode(&mut self, input: &mut BytesMut) -> Result<Option<BatchEvent>, Error> {
        if self.failed {
            return Err(Error::Integrity);
        }
        let result = self.decode_inner(input);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn decode_inner(&mut self, input: &mut BytesMut) -> Result<Option<BatchEvent>, Error> {
        if self.ended {
            return if input.is_empty() {
                Ok(None)
            } else {
                Err(Error::Integrity)
            };
        }
        if !self.magic {
            if input.len() < BATCH_MAGIC.len() {
                return Ok(None);
            }
            if &input[..8] != BATCH_MAGIC {
                return Err(Error::Integrity);
            }
            input.advance(8);
            self.magic = true;
        }
        if input.len() < 5 {
            return Ok(None);
        }
        let kind = input[0];
        let length = u32::from_be_bytes(input[1..5].try_into().map_err(|_| Error::Integrity)?) as usize;
        let valid = match kind {
            1 => self.header.is_none() && length > 0 && length <= MAX_HEADER_FRAME,
            2 => self.header.is_some() && length > 16 && length <= MAX_DATA_FRAME + 16,
            0 | 3 => self.header.is_some() && length == 0,
            4 => self.header.is_some() && length > 0 && length <= 64 * 1024,
            _ => false,
        };
        if !valid {
            return Err(Error::Integrity);
        }
        if input.len() < 5 + length {
            return Ok(None);
        }
        input.advance(5);
        let mut body = input.split_to(length);
        match kind {
            1 => {
                let header: BatchHeader = serde_json::from_slice(&body)?;
                validate_header(&header)?;
                let header = Arc::new(header);
                self.header = Some(Arc::clone(&header));
                self.skip_missing();
                Ok(Some(BatchEvent::Header(header)))
            }
            2 => {
                let expected: &[u8] = &body[..16];
                if Md5::digest(&body[16..]).as_slice() != expected {
                    return Err(Error::Integrity);
                }
                body.advance(16);
                let field = self.current()?;
                let length = self
                    .length
                    .checked_add(body.len() as u64)
                    .ok_or(Error::Integrity)?;
                if length > field.length {
                    return Err(Error::Integrity);
                }
                self.length = length;
                self.hash.update(&body);
                Ok(Some(BatchEvent::Data(body.freeze())))
            }
            3 => {
                let field = self.current()?;
                if self.length != field.length
                    || field.md5.as_deref() != Some(hex::encode(self.hash.clone().finalize()).as_str())
                {
                    return Err(Error::Integrity);
                }
                self.field += 1;
                self.length = 0;
                self.hash = Md5::new();
                self.skip_missing();
                Ok(Some(BatchEvent::FieldEnd))
            }
            0 => {
                let header = self.header.as_ref().ok_or(Error::Integrity)?;
                if self.field != header.fields.len() {
                    return Err(Error::Integrity);
                }
                self.ended = true;
                if !input.is_empty() {
                    return Err(Error::Integrity);
                }
                Ok(Some(BatchEvent::End))
            }
            4 => Err(Error::Stream {
                error: serde_json::from_slice(&body)?,
            }),
            _ => Err(Error::Integrity),
        }
    }

    /// Called only at HTTP EOF after draining decoder events.
    /// # Errors
    /// Rejects incomplete streams, including a complete field without batch end.
    pub fn finish(&self) -> Result<(), Error> {
        if self.ended && !self.failed {
            Ok(())
        } else {
            Err(Error::Integrity)
        }
    }

    fn current(&self) -> Result<&BatchField, Error> {
        self.header
            .as_ref()
            .and_then(|h| h.fields.get(self.field))
            .ok_or(Error::Integrity)
    }
    fn skip_missing(&mut self) {
        if let Some(header) = &self.header {
            while header
                .fields
                .get(self.field)
                .is_some_and(|f| !matches!(f.state, FieldState::Value))
            {
                self.field += 1;
            }
        }
    }
}

fn validate_header(header: &BatchHeader) -> Result<(), Error> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    if !valid_id(&header.snapshot) || header.sample_ids.len() > 10_000 || header.fields.len() > 2_560_000 {
        return Err(Error::Integrity);
    }
    let ids: std::collections::BTreeSet<_> = header.sample_ids.iter().collect();
    if ids.len() != header.sample_ids.len() {
        return Err(Error::Integrity);
    }
    for id in &header.sample_ids {
        if id.len() > 1368 {
            return Err(Error::Integrity);
        }
        let bytes = STANDARD.decode(id).map_err(|_| Error::Integrity)?;
        if bytes.is_empty() || bytes.len() > 1024 || STANDARD.encode(bytes) != *id {
            return Err(Error::Integrity);
        }
    }
    let mut fields = std::collections::BTreeSet::new();
    for field in &header.fields {
        if !ids.contains(&field.sample)
            || field.field.is_empty()
            || field.field.len() > 255
            || !fields.insert((&field.sample, &field.field))
        {
            return Err(Error::Integrity);
        }
        match field.state {
            FieldState::Value => {
                if !field.md5.as_deref().is_some_and(valid_digest) {
                    return Err(Error::Integrity);
                }
            }
            _ => {
                if field.length != 0 || field.md5.is_some() {
                    return Err(Error::Integrity);
                }
            }
        }
    }
    Ok(())
}
fn valid_digest(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn valid_id(value: &str) -> bool {
    valid_digest(value) && value.bytes().any(|b| b != b'0')
}
fn required_option<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
}

/// A convenience result limited by the HTTP client's maximum response bytes.
/// Each field's frames retain their byte owners without concatenating payloads.
#[derive(Debug)]
pub struct VerifiedBatch {
    pub header: Arc<BatchHeader>,
    pub fields: Vec<Vec<Bytes>>,
}

/// Decodes one bounded complete HTTP body without copying owned payload frames.
/// # Errors
/// Rejects malformed, truncated, unsupported or corrupt bodies.
pub fn decode_batch(bytes: Bytes) -> Result<VerifiedBatch, Error> {
    let mut input = match bytes.try_into_mut() {
        Ok(input) => input,
        Err(bytes) => BytesMut::from(bytes.as_ref()),
    };
    let mut decoder = BatchDecoder::new();
    let mut header = None;
    let mut fields = Vec::new();
    let mut values = Vec::new();
    let mut current = 0;
    while let Some(event) = decoder.decode(&mut input)? {
        match event {
            BatchEvent::Header(value) => {
                fields.resize_with(value.fields.len(), Vec::new);
                values = value
                    .fields
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| matches!(f.state, FieldState::Value))
                    .map(|(index, _)| index)
                    .collect();
                header = Some(value);
            }
            BatchEvent::Data(data) => {
                let index = *values.get(current).ok_or(Error::Integrity)?;
                fields[index].push(data);
            }
            BatchEvent::FieldEnd => current += 1,
            BatchEvent::End => {}
        }
    }
    decoder.finish()?;
    Ok(VerifiedBatch {
        header: header.ok_or(Error::Integrity)?,
        fields,
    })
}
