// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! RFC byte-range parsing for the one contiguous range supported by S3 GET.

/// A resolved, half-open logical object interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeError {
    Invalid,
    Multiple,
    Unsatisfiable,
}

/// Resolves one `Range: bytes=...` value against an object length.
///
/// A missing header is represented by `None`; the returned end is exclusive.
/// S3 basic GET deliberately rejects multi-range requests.
///
/// # Errors
///
/// Returns malformed, multi-range, or unsatisfiable range failures.
pub fn resolve_range(value: Option<&str>, length: u64) -> Result<Option<ByteRange>, RangeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let spec = value.strip_prefix("bytes=").ok_or(RangeError::Invalid)?;
    if spec.contains(',') {
        return Err(RangeError::Multiple);
    }
    let (first, last) = spec.split_once('-').ok_or(RangeError::Invalid)?;
    if first.is_empty() {
        let suffix = parse_nonzero(last)?;
        if length == 0 {
            return Err(RangeError::Unsatisfiable);
        }
        let start = length.saturating_sub(suffix);
        return Ok(Some(ByteRange { start, end: length }));
    }
    let start = first.parse::<u64>().map_err(|_| RangeError::Invalid)?;
    if start >= length {
        return Err(RangeError::Unsatisfiable);
    }
    let end = if last.is_empty() {
        length
    } else {
        parse_number(last)?.saturating_add(1).min(length)
    };
    if end <= start {
        return Err(RangeError::Unsatisfiable);
    }
    Ok(Some(ByteRange { start, end }))
}

fn parse_nonzero(value: &str) -> Result<u64, RangeError> {
    match parse_number(value) {
        Ok(0) => Err(RangeError::Unsatisfiable),
        Ok(value) => Ok(value),
        Err(error) => Err(error),
    }
}

fn parse_number(value: &str) -> Result<u64, RangeError> {
    value.parse::<u64>().map_err(|_| RangeError::Invalid)
}
