// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{DeleteResults, MAX_DELETE_BODY};
use crate::error::S3ErrorCode;
use quick_xml::{events::Event, Reader};

#[derive(Debug, Eq, PartialEq)]
pub struct DeleteSelection {
    pub keys: Vec<Vec<u8>>,
    pub quiet: bool,
}

impl DeleteSelection {
    /// Validates the complete selection before the caller mutates any key.
    /// Duplicates remain in input order and each receives an independent result.
    ///
    /// # Errors
    /// Rejects malformed XML, extensions, empty selections and exceeded bounds.
    pub fn parse(bytes: &[u8]) -> Result<Self, S3ErrorCode> {
        if bytes.is_empty() || bytes.len() > MAX_DELETE_BODY {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let mut reader = Reader::from_reader(bytes);
        let mut state = ParseState::default();
        loop {
            let event = reader.read_event().map_err(|_| S3ErrorCode::InvalidRequest)?;
            if matches!(event, Event::Eof) {
                if !state.done || state.keys.is_empty() {
                    return Err(S3ErrorCode::InvalidRequest);
                }
                return Ok(Self {
                    keys: state.keys,
                    quiet: state.quiet.unwrap_or(false),
                });
            }
            state.read(event)?;
        }
    }

    /// Executes one independent deletion at a time in input order, retaining errors.
    /// Dropping the future cancels remaining work; completed mutations are not rolled back.
    /// # Errors
    /// Rejects a manually constructed invalid selection before invoking the callback.
    pub async fn execute<F, Fut>(&self, mut delete: F) -> Result<DeleteResults, S3ErrorCode>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = Result<(), S3ErrorCode>>,
    {
        if self.keys.is_empty() || self.keys.len() > 1000 {
            return Err(S3ErrorCode::InvalidRequest);
        }
        for key in &self.keys {
            validate_key(key)?;
        }
        let mut results = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            let result = delete(key.clone()).await;
            results.push((key.clone(), result));
        }
        Ok(results)
    }
}

fn validate_key(bytes: &[u8]) -> Result<(), S3ErrorCode> {
    let text = std::str::from_utf8(bytes).map_err(|_| S3ErrorCode::InvalidRequest)?;
    if text.is_empty() || bytes.len() > 1024 || text.chars().any(|c| !matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}')) {
        return Err(S3ErrorCode::InvalidRequest);
    }
    Ok(())
}

#[derive(Default)]
struct ParseState {
    stack: Vec<Vec<u8>>,
    keys: Vec<Vec<u8>>,
    key: Option<Vec<u8>>,
    text: String,
    quiet: Option<bool>,
    started: bool,
    done: bool,
    seen_event: bool,
}

impl ParseState {
    fn read(&mut self, event: Event<'_>) -> Result<(), S3ErrorCode> {
        match event {
            Event::Decl(event) if !self.seen_event => {
                if event.version().map_err(|_| S3ErrorCode::InvalidRequest)?.as_ref() != b"1.0"
                    || event
                        .encoding()
                        .transpose()
                        .map_err(|_| S3ErrorCode::InvalidRequest)?
                        .is_some_and(|encoding| !encoding.eq_ignore_ascii_case(b"UTF-8"))
                {
                    return Err(S3ErrorCode::InvalidRequest);
                }
            }
            Event::Start(event) => self.start(&event)?,
            Event::End(event) => self.end(event.name().as_ref())?,
            Event::Text(event) => {
                self.append(&event.xml_content().map_err(|_| S3ErrorCode::InvalidRequest)?)?;
            }
            Event::CData(event) => {
                self.append(&event.xml_content().map_err(|_| S3ErrorCode::InvalidRequest)?)?;
            }
            Event::GeneralRef(event) => {
                let name = std::str::from_utf8(&event).map_err(|_| S3ErrorCode::InvalidRequest)?;
                let encoded = format!("&{name};");
                let value = quick_xml::escape::unescape(&encoded).map_err(|_| S3ErrorCode::InvalidRequest)?;
                self.append(&value)?;
            }
            _ => return Err(S3ErrorCode::InvalidRequest),
        }
        self.seen_event = true;
        Ok(())
    }

    fn start(&mut self, event: &quick_xml::events::BytesStart<'_>) -> Result<(), S3ErrorCode> {
        let name = event.name().as_ref().to_vec();
        let valid = match self.stack.as_slice() {
            [] => !self.started && name == b"Delete",
            [root] if root == b"Delete" => {
                (name == b"Object" && self.keys.len() < 1000) || (name == b"Quiet" && self.quiet.is_none())
            }
            [root, object] if root == b"Delete" && object == b"Object" => {
                name == b"Key" && self.key.is_none()
            }
            _ => false,
        };
        if !valid || self.done {
            return Err(S3ErrorCode::InvalidRequest);
        }
        for attribute in event.attributes() {
            let attribute = attribute.map_err(|_| S3ErrorCode::InvalidRequest)?;
            if self.started
                || attribute.key.as_ref() != b"xmlns"
                || attribute.value.as_ref() != b"http://s3.amazonaws.com/doc/2006-03-01/"
            {
                return Err(S3ErrorCode::InvalidRequest);
            }
        }
        self.started = true;
        self.stack.push(name);
        Ok(())
    }

    fn end(&mut self, expected: &[u8]) -> Result<(), S3ErrorCode> {
        let name = self.stack.pop().ok_or(S3ErrorCode::InvalidRequest)?;
        if name != expected {
            return Err(S3ErrorCode::InvalidRequest);
        }
        match name.as_slice() {
            b"Key" => {
                validate_key(self.text.as_bytes())?;
                self.key = Some(std::mem::take(&mut self.text).into_bytes());
            }
            b"Quiet" => {
                self.quiet = Some(match self.text.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(S3ErrorCode::InvalidRequest),
                });
                self.text.clear();
            }
            b"Object" => self
                .keys
                .push(self.key.take().ok_or(S3ErrorCode::InvalidRequest)?),
            b"Delete" => self.done = true,
            _ => return Err(S3ErrorCode::InvalidRequest),
        }
        Ok(())
    }

    fn append(&mut self, value: &str) -> Result<(), S3ErrorCode> {
        if self
            .stack
            .last()
            .is_some_and(|name| name == b"Key" || name == b"Quiet")
        {
            if self.text.len().saturating_add(value.len()) > 1024 {
                return Err(S3ErrorCode::InvalidRequest);
            }
            self.text.push_str(value);
        } else if !value.bytes().all(|byte| byte.is_ascii_whitespace()) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        Ok(())
    }
}
