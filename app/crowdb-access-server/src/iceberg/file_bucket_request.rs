use hyper::{Method, Uri};

use super::file_request::{decode, query, FileRequestError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileBucketRequest {
    Head(String),
    Location(String),
}

/// # Errors
/// Rejects unsupported bucket selectors and malformed transport encodings.
pub fn parse_file_bucket(method: &Method, uri: &Uri) -> Result<Option<FileBucketRequest>, FileRequestError> {
    if uri
        .path_and_query()
        .map_or(true, |value| value.as_str().len() > 8192)
    {
        return Err(FileRequestError::Invalid);
    }
    if uri.path().trim_end_matches('/').split('/').count() != 2 {
        return Ok(None);
    }
    let path = decode(uri.path())?;
    let bucket = path.strip_prefix('/').ok_or(FileRequestError::Invalid)?;
    let bucket = bucket.strip_suffix('/').unwrap_or(bucket);
    if bucket.is_empty() || bucket.contains('/') {
        return Ok(None);
    }
    let mut fields = query(uri.query())?;
    if method == Method::HEAD {
        if !fields.is_empty() {
            return Err(FileRequestError::Unsupported);
        }
        return Ok(Some(FileBucketRequest::Head(bucket.to_owned())));
    }
    if method == Method::GET && fields.remove("location").as_deref() == Some("") {
        if !fields.is_empty() {
            return Err(FileRequestError::Unsupported);
        }
        return Ok(Some(FileBucketRequest::Location(bucket.to_owned())));
    }
    Ok(None)
}
