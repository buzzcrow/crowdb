// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded S3 completion XML shared by the Iceberg and S3 protocol modules.

use quick_xml::events::Event;
use quick_xml::Reader;

const MAX_COMPLETE_XML_BYTES: usize = 2 * 1024 * 1024;
const MAX_COMPLETE_PARTS: usize = 10_000;
const S3_NAMESPACE: &[u8] = b"http://s3.amazonaws.com/doc/2006-03-01/";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletePart {
    pub number: u16,
    pub etag: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteSelection {
    parts: Vec<CompletePart>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CompleteRequestError {
    #[error("invalid multipart completion XML")]
    InvalidRequest,
    #[error("multipart parts are not in ascending order")]
    InvalidPartOrder,
}

impl CompleteSelection {
    /// Parses a bounded S3 `CompleteMultipartUpload` body. The caller must match
    /// each selected digest to the current durable part revision before freezing.
    /// # Errors
    /// Rejects malformed XML, extra fields and unordered or duplicate parts.
    pub fn parse(bytes: &[u8]) -> Result<Self, CompleteRequestError> {
        if bytes.is_empty() || bytes.len() > MAX_COMPLETE_XML_BYTES {
            return Err(CompleteRequestError::InvalidRequest);
        }
        let mut reader = Reader::from_reader(bytes);
        let mut state = State::Start;
        let mut parts = Vec::new();
        let mut number = None;
        let mut digest = None;
        let mut etag = Vec::new();
        loop {
            match reader
                .read_event()
                .map_err(|_| CompleteRequestError::InvalidRequest)?
            {
                Event::Decl(_) if state == State::Start => {}
                Event::Start(event) if valid_attributes(state, &event)? => {
                    state = match (state, event.name().as_ref()) {
                        (State::Start, b"CompleteMultipartUpload") => State::Root,
                        (State::Root, b"Part") if parts.len() < MAX_COMPLETE_PARTS => State::Part,
                        (State::Part, b"PartNumber") if number.is_none() => State::Number,
                        (State::Part, b"ETag") if digest.is_none() => State::Etag,
                        _ => return Err(CompleteRequestError::InvalidRequest),
                    };
                }
                Event::Text(event) => match state {
                    State::Number if number.is_none() => {
                        let value: &[u8] = event.as_ref();
                        if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
                            return Err(CompleteRequestError::InvalidRequest);
                        }
                        number = Some(
                            std::str::from_utf8(value)
                                .map_err(|_| CompleteRequestError::InvalidRequest)?
                                .parse::<u16>()
                                .map_err(|_| CompleteRequestError::InvalidRequest)?,
                        );
                    }
                    State::Etag => append_etag(&mut etag, &event)?,
                    State::Start | State::Root | State::Part | State::Done
                        if event.iter().all(u8::is_ascii_whitespace) => {}
                    _ => return Err(CompleteRequestError::InvalidRequest),
                },
                Event::GeneralRef(event) if state == State::Etag => {
                    if event.len() > 16 {
                        return Err(CompleteRequestError::InvalidRequest);
                    }
                    let name =
                        std::str::from_utf8(&event).map_err(|_| CompleteRequestError::InvalidRequest)?;
                    let encoded = format!("&{name};");
                    let decoded = quick_xml::escape::unescape(&encoded)
                        .map_err(|_| CompleteRequestError::InvalidRequest)?;
                    append_etag(&mut etag, decoded.as_bytes())?;
                }
                Event::End(event) => {
                    state = match (state, event.name().as_ref()) {
                        (State::Number, b"PartNumber") if number.is_some() => State::Part,
                        (State::Etag, b"ETag") => {
                            digest = Some(parse_etag(&etag)?);
                            etag.clear();
                            State::Part
                        }
                        (State::Part, b"Part") => {
                            let number = number.take().ok_or(CompleteRequestError::InvalidRequest)?;
                            let etag = digest.take().ok_or(CompleteRequestError::InvalidRequest)?;
                            if number == 0 || number > 10_000 {
                                return Err(CompleteRequestError::InvalidRequest);
                            }
                            if parts
                                .last()
                                .is_some_and(|part: &CompletePart| part.number >= number)
                            {
                                return Err(CompleteRequestError::InvalidPartOrder);
                            }
                            parts.push(CompletePart { number, etag });
                            State::Root
                        }
                        (State::Root, b"CompleteMultipartUpload") if !parts.is_empty() => State::Done,
                        _ => return Err(CompleteRequestError::InvalidRequest),
                    };
                }
                Event::Eof if state == State::Done => return Ok(Self { parts }),
                _ => return Err(CompleteRequestError::InvalidRequest),
            }
        }
    }

    #[must_use]
    pub fn parts(&self) -> &[CompletePart] {
        &self.parts
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum State {
    Start,
    Root,
    Part,
    Number,
    Etag,
    Done,
}

fn parse_etag(bytes: &[u8]) -> Result<String, CompleteRequestError> {
    let hex = bytes
        .strip_prefix(b"\"")
        .and_then(|bytes| bytes.strip_suffix(b"\""))
        .ok_or(CompleteRequestError::InvalidRequest)?;
    if hex.len() != 32 && hex.len() != 64 {
        return Err(CompleteRequestError::InvalidRequest);
    }
    for pair in hex.chunks_exact(2) {
        let _ = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    String::from_utf8(hex.to_vec()).map_err(|_| CompleteRequestError::InvalidRequest)
}

fn append_etag(etag: &mut Vec<u8>, bytes: &[u8]) -> Result<(), CompleteRequestError> {
    if etag.len().saturating_add(bytes.len()) > 66 {
        return Err(CompleteRequestError::InvalidRequest);
    }
    etag.extend_from_slice(bytes);
    Ok(())
}

fn hex_digit(byte: u8) -> Result<u8, CompleteRequestError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(CompleteRequestError::InvalidRequest),
    }
}

fn valid_attributes(
    state: State,
    event: &quick_xml::events::BytesStart<'_>,
) -> Result<bool, CompleteRequestError> {
    let mut attributes = event.attributes();
    let Some(attribute) = attributes.next() else {
        return Ok(true);
    };
    let attribute = attribute.map_err(|_| CompleteRequestError::InvalidRequest)?;
    Ok(state == State::Start
        && event.name().as_ref() == b"CompleteMultipartUpload"
        && attribute.key.as_ref() == b"xmlns"
        && attribute.value.as_ref() == S3_NAMESPACE
        && attributes.next().is_none())
}
