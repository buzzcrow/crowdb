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

/// # Errors
/// Rejects malformed, multiple or unsatisfiable ranges. The interval is half-open.
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
        let suffix = number(last)?;
        if suffix == 0 || length == 0 {
            return Err(RangeError::Unsatisfiable);
        }
        return Ok(Some(ByteRange {
            start: length.saturating_sub(suffix),
            end: length,
        }));
    }
    let start = number(first)?;
    let end = if last.is_empty() {
        length
    } else {
        number(last)?.saturating_add(1).min(length)
    };
    if start >= length || end <= start {
        return Err(RangeError::Unsatisfiable);
    }
    Ok(Some(ByteRange { start, end }))
}

fn number(value: &str) -> Result<u64, RangeError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RangeError::Invalid);
    }
    value.parse().map_err(|_| RangeError::Invalid)
}
