//! Bucket-shaped native list requests require an explicit table prefix.

use super::file_request::{decode, query, FileRequestError};
use crowdb_access_iceberg::file::{FileListRequest, TableLocation};
use hyper::{Method, Uri};

/// Parses `ListObjectsV2` independently of exact-object `FileIO`.
/// # Errors
/// Rejects ambiguous parameters, catalog-wide discovery and unsupported selectors.
pub fn parse_file_list(method: &Method, uri: &Uri) -> Result<Option<FileListRequest>, FileRequestError> {
    if uri
        .path_and_query()
        .map_or(true, |value| value.as_str().len() > 8192)
    {
        return Err(FileRequestError::Invalid);
    }
    let mut fields = query(uri.query())?;
    let Some(list_type) = fields.remove("list-type") else {
        return Ok(None);
    };
    if method != Method::GET || list_type != "2" {
        return Err(FileRequestError::Invalid);
    }
    let path = decode(uri.path())?;
    let path = path.strip_prefix('/').ok_or(FileRequestError::Invalid)?;
    let bucket = path.strip_suffix('/').unwrap_or(path);
    if bucket.contains('/') {
        return Err(FileRequestError::Invalid);
    }
    let prefix = fields.remove("prefix").ok_or(FileRequestError::Invalid)?;
    let table_prefix = prefix.get(..35).ok_or(FileRequestError::Invalid)?;
    let table: TableLocation = format!("s3://{bucket}/{table_prefix}")
        .parse()
        .map_err(|_| FileRequestError::Invalid)?;
    let max_keys = fields.remove("max-keys").map_or(Ok(1000), |value| {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(FileRequestError::Invalid);
        }
        value.parse::<u16>().map_err(|_| FileRequestError::Invalid)
    })?;
    let delimiter = fields.remove("delimiter").filter(|value| !value.is_empty());
    let encoding_url = match fields.remove("encoding-type").as_deref() {
        None => false,
        Some("url") => true,
        _ => return Err(FileRequestError::Unsupported),
    };
    if let Some(owner) = fields.remove("fetch-owner") {
        if owner != "false" {
            return Err(FileRequestError::Unsupported);
        }
    }
    let request = FileListRequest {
        table,
        prefix,
        delimiter,
        encoding_url,
        max_keys,
        continuation_token: fields.remove("continuation-token"),
        start_after: fields.remove("start-after"),
    };
    if !fields.is_empty() {
        return Err(FileRequestError::Unsupported);
    }
    request.validate().map_err(|_| FileRequestError::Invalid)?;
    Ok(Some(request))
}
