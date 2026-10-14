use hyper::header::{HeaderMap, IF_MATCH, IF_NONE_MATCH, IF_RANGE};

use super::file_response::FileS3ErrorCode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileReadCondition {
    NotModified,
    Read { use_range: bool },
}

/// # Errors
/// Rejects malformed condition lists and mismatched strong object preconditions.
pub fn file_read_condition(headers: &HeaderMap, etag: &str) -> Result<FileReadCondition, FileS3ErrorCode> {
    if let Some(matches) = condition(headers, IF_MATCH, etag, false)? {
        if !matches {
            return Err(FileS3ErrorCode::PreconditionFailed);
        }
    }
    if condition(headers, IF_NONE_MATCH, etag, true)? == Some(true) {
        return Ok(FileReadCondition::NotModified);
    }
    let use_range = match headers.get_all(IF_RANGE).iter().collect::<Vec<_>>().as_slice() {
        [] => true,
        [value] => value.to_str().map_err(|_| FileS3ErrorCode::InvalidRequest)? == etag,
        _ => return Err(FileS3ErrorCode::InvalidRequest),
    };
    Ok(FileReadCondition::Read { use_range })
}

fn condition(
    headers: &HeaderMap,
    name: hyper::header::HeaderName,
    etag: &str,
    weak: bool,
) -> Result<Option<bool>, FileS3ErrorCode> {
    let mut result = None;
    let mut count = 0;
    let mut bytes = 0;
    for value in headers.get_all(name) {
        let value = value.to_str().map_err(|_| FileS3ErrorCode::InvalidRequest)?;
        bytes += value.len();
        if bytes > 4096 {
            return Err(FileS3ErrorCode::InvalidRequest);
        }
        for tag in value.split(',') {
            count += 1;
            let tag = tag.trim();
            if count > 64 {
                return Err(FileS3ErrorCode::InvalidRequest);
            }
            let comparison = if weak {
                tag.strip_prefix("W/").unwrap_or(tag)
            } else {
                tag
            };
            let syntax = tag.strip_prefix("W/").unwrap_or(tag);
            if syntax != "*"
                && (syntax.len() < 2
                    || !syntax.starts_with('"')
                    || !syntax.ends_with('"')
                    || syntax[1..syntax.len() - 1]
                        .bytes()
                        .any(|byte| byte == b'"' || byte <= 0x20 || byte == 0x7f))
            {
                return Err(FileS3ErrorCode::InvalidRequest);
            }
            result = Some(result.unwrap_or(false) || comparison == "*" || comparison == etag);
        }
    }
    Ok(result)
}
