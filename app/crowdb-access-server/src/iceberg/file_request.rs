use std::collections::BTreeMap;

use crowdb_access_iceberg::file::{FileLocation, FileOperation};
use hyper::{Method, Uri};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MultipartRequest {
    Create,
    Upload {
        upload_id: String,
        part_number: u16,
    },
    List {
        upload_id: String,
        marker: u16,
        max_parts: u16,
    },
    Complete {
        upload_id: String,
    },
    Abort {
        upload_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRequest {
    pub location: FileLocation,
    pub operation: FileOperation,
    pub multipart: Option<MultipartRequest>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FileRequestError {
    #[error("malformed native file request")]
    Invalid,
    #[error("unsupported native file operation")]
    Unsupported,
}

impl FileRequest {
    /// Parses the native path-style object surface without general S3 routing.
    /// Authentication must use the original URI before these fields are decoded.
    /// # Errors
    /// Rejects escaped paths, ambiguous query values and unsupported operations.
    pub fn parse(method: &Method, uri: &Uri) -> Result<Self, FileRequestError> {
        let target = uri.path_and_query().ok_or(FileRequestError::Invalid)?.as_str();
        if target.len() > 8192 {
            return Err(FileRequestError::Invalid);
        }
        let path = decode(uri.path())?;
        let path = path.strip_prefix('/').ok_or(FileRequestError::Invalid)?;
        let location = format!("s3://{path}")
            .parse()
            .map_err(|_| FileRequestError::Invalid)?;
        let mut query = query(uri.query())?;
        let multipart = multipart(method, &mut query)?;
        if !query.is_empty() {
            return Err(FileRequestError::Unsupported);
        }
        let operation = match &multipart {
            Some(MultipartRequest::Create) => FileOperation::CreateMultipart,
            Some(MultipartRequest::Upload { .. }) => FileOperation::UploadPart,
            Some(MultipartRequest::List { .. }) => FileOperation::ListParts,
            Some(MultipartRequest::Complete { .. }) => FileOperation::CompleteMultipart,
            Some(MultipartRequest::Abort { .. }) => FileOperation::AbortMultipart,
            None if method == Method::GET => FileOperation::Get,
            None if method == Method::HEAD => FileOperation::Head,
            None if method == Method::PUT => FileOperation::Put,
            None => return Err(FileRequestError::Unsupported),
        };
        Ok(Self {
            location,
            operation,
            multipart,
        })
    }
}

fn multipart(
    method: &Method,
    query: &mut BTreeMap<String, String>,
) -> Result<Option<MultipartRequest>, FileRequestError> {
    if let Some(value) = query.remove("uploads") {
        if method != Method::POST || !value.is_empty() || !query.is_empty() {
            return Err(FileRequestError::Invalid);
        }
        return Ok(Some(MultipartRequest::Create));
    }
    let Some(upload_id) = query.remove("uploadId") else {
        return Ok(None);
    };
    if upload_id.is_empty() || upload_id.len() > 256 || !upload_id.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(FileRequestError::Invalid);
    }
    let request = if method == Method::PUT {
        let part_number = number(query.remove("partNumber"), None, 1, 10_000)?;
        MultipartRequest::Upload {
            upload_id,
            part_number,
        }
    } else if method == Method::GET {
        let marker = number(query.remove("part-number-marker"), Some(0), 0, 10_000)?;
        let max_parts = number(query.remove("max-parts"), Some(1000), 1, 1000)?;
        MultipartRequest::List {
            upload_id,
            marker,
            max_parts,
        }
    } else if method == Method::POST {
        MultipartRequest::Complete { upload_id }
    } else if method == Method::DELETE {
        MultipartRequest::Abort { upload_id }
    } else {
        return Err(FileRequestError::Unsupported);
    };
    Ok(Some(request))
}

fn number(value: Option<String>, default: Option<u16>, min: u16, max: u16) -> Result<u16, FileRequestError> {
    let value = match value {
        Some(value) if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
            value.parse::<u16>().map_err(|_| FileRequestError::Invalid)?
        }
        Some(_) => return Err(FileRequestError::Invalid),
        None => default.ok_or(FileRequestError::Invalid)?,
    };
    if !(min..=max).contains(&value) {
        return Err(FileRequestError::Invalid);
    }
    Ok(value)
}

fn query(query: Option<&str>) -> Result<BTreeMap<String, String>, FileRequestError> {
    let mut fields = BTreeMap::new();
    let Some(query) = query else {
        return Ok(fields);
    };
    for field in query.split('&') {
        let (name, value) = field.split_once('=').unwrap_or((field, ""));
        let name = decode(name)?;
        let value = decode(value)?;
        if name.is_empty() || fields.len() == 16 || fields.insert(name, value).is_some() {
            return Err(FileRequestError::Invalid);
        }
    }
    for name in [
        "X-Amz-Algorithm",
        "X-Amz-Credential",
        "X-Amz-Date",
        "X-Amz-Expires",
        "X-Amz-Security-Token",
        "X-Amz-SignedHeaders",
        "X-Amz-Signature",
    ] {
        fields.remove(name);
    }
    Ok(fields)
}

fn decode(value: &str) -> Result<String, FileRequestError> {
    let bytes = value.as_bytes();
    for (offset, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && !bytes
                .get(offset + 1..offset + 3)
                .is_some_and(|pair| pair.iter().all(u8::is_ascii_hexdigit))
        {
            return Err(FileRequestError::Invalid);
        }
    }
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| FileRequestError::Invalid)
}
