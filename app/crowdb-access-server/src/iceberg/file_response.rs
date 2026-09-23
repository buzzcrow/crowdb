use std::fmt::Write;

use chrono::{DateTime, SecondsFormat, Utc};
use crowdb_access_iceberg::file::{
    FileRecord, MultipartPart, MultipartPartPage, MultipartPhase, MultipartSession,
};
use hyper::http::header::{HeaderValue, CONTENT_TYPE, ETAG};
use hyper::{Response, StatusCode};

const XML_TYPE: &str = "application/xml";
const XML_PREFIX: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
const XML_NAMESPACE: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

#[derive(Debug, thiserror::Error)]
pub enum FileResponseError {
    #[error("multipart response state is invalid")]
    Invalid,
    #[error("multipart part timestamp is out of range")]
    Timestamp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileS3ErrorCode {
    AccessDenied,
    NoSuchUpload,
    InvalidPart,
    EntityTooLarge,
    InvalidRequest,
    InternalError,
    NoSuchKey,
    InvalidRange,
    SlowDown,
    Conflict,
    BadDigest,
    EntityTooSmall,
}

impl FileS3ErrorCode {
    const fn status(self) -> StatusCode {
        match self {
            Self::AccessDenied => StatusCode::FORBIDDEN,
            Self::NoSuchUpload | Self::NoSuchKey => StatusCode::NOT_FOUND,
            Self::InvalidPart | Self::InvalidRequest | Self::BadDigest | Self::EntityTooSmall => {
                StatusCode::BAD_REQUEST
            }
            Self::EntityTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::SlowDown => StatusCode::SERVICE_UNAVAILABLE,
            Self::Conflict => StatusCode::CONFLICT,
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::AccessDenied => "Access Denied",
            Self::NoSuchUpload => "The specified upload does not exist",
            Self::InvalidPart => "One or more parts are invalid",
            Self::EntityTooLarge => "The request exceeds the allowed size",
            Self::InvalidRequest => "The request is invalid",
            Self::InternalError => "The service could not complete the request",
            Self::NoSuchKey => "The specified key does not exist",
            Self::InvalidRange => "The requested range cannot be satisfied",
            Self::SlowDown => "The service is temporarily unavailable",
            Self::Conflict => "The immutable object already exists with different content",
            Self::BadDigest => "The supplied digest does not match the uploaded content",
            Self::EntityTooSmall => "A nonfinal upload part is smaller than 5 MiB",
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::NoSuchUpload => "NoSuchUpload",
            Self::InvalidPart => "InvalidPart",
            Self::EntityTooLarge => "EntityTooLarge",
            Self::InvalidRequest => "InvalidRequest",
            Self::InternalError => "InternalError",
            Self::NoSuchKey => "NoSuchKey",
            Self::InvalidRange => "InvalidRange",
            Self::SlowDown => "SlowDown",
            Self::Conflict => "OperationAborted",
            Self::BadDigest => "BadDigest",
            Self::EntityTooSmall => "EntityTooSmall",
        }
    }
}

pub struct MultipartResponses;

impl MultipartResponses {
    /// # Errors
    /// Rejects invalid durable session state before emitting a success response.
    pub fn create(session: &MultipartSession) -> Result<Response<Vec<u8>>, FileResponseError> {
        session.validate().map_err(|_| FileResponseError::Invalid)?;
        if session.phase != MultipartPhase::Open {
            return Err(FileResponseError::Invalid);
        }
        let mut body = start("InitiateMultipartUploadResult");
        location_fields(&mut body, session);
        element(&mut body, "UploadId", &session.upload.to_string());
        end(&mut body, "InitiateMultipartUploadResult");
        Ok(xml(StatusCode::OK, body))
    }

    /// # Errors
    /// Rejects a part that lacks durable identity or an upload timestamp.
    pub fn upload_part(part: &MultipartPart) -> Result<Response<Vec<u8>>, FileResponseError> {
        part.validate().map_err(|_| FileResponseError::Invalid)?;
        let mut response = Response::new(Vec::new());
        response.headers_mut().insert(
            ETAG,
            HeaderValue::from_str(&etag(part.tree.digest)).map_err(|_| FileResponseError::Invalid)?,
        );
        Ok(response)
    }

    /// # Errors
    /// Rejects incoherent requested markers, pages, part bindings or timestamps.
    pub fn list_parts(
        session: &MultipartSession,
        page: &MultipartPartPage,
        marker: u16,
        max_parts: u16,
    ) -> Result<Response<Vec<u8>>, FileResponseError> {
        session.validate().map_err(|_| FileResponseError::Invalid)?;
        if marker > 10_000 || max_parts == 0 || max_parts > 1000 || page.parts.len() > max_parts as usize {
            return Err(FileResponseError::Invalid);
        }
        let mut body = start("ListPartsResult");
        location_fields(&mut body, session);
        element(&mut body, "UploadId", &session.upload.to_string());
        element(&mut body, "PartNumberMarker", &marker.to_string());
        if let Some(next) = page.next_marker {
            if page.parts.last().map(|part| part.number) != Some(next) {
                return Err(FileResponseError::Invalid);
            }
            element(&mut body, "NextPartNumberMarker", &next.to_string());
        }
        element(&mut body, "MaxParts", &max_parts.to_string());
        element(
            &mut body,
            "IsTruncated",
            if page.next_marker.is_some() {
                "true"
            } else {
                "false"
            },
        );
        let mut previous = marker;
        for part in &page.parts {
            part.validate_for(session)
                .map_err(|_| FileResponseError::Invalid)?;
            if part.number <= previous
                || part.modified_ms < session.created_ms
                || part.modified_ms >= session.expires_ms
            {
                return Err(FileResponseError::Invalid);
            }
            previous = part.number;
            body.push_str("<Part>");
            element(&mut body, "PartNumber", &part.number.to_string());
            let timestamp = DateTime::<Utc>::from_timestamp_millis(
                i64::try_from(part.modified_ms).map_err(|_| FileResponseError::Timestamp)?,
            )
            .ok_or(FileResponseError::Timestamp)?;
            element(
                &mut body,
                "LastModified",
                &timestamp.to_rfc3339_opts(SecondsFormat::Millis, true),
            );
            element(&mut body, "ETag", &etag(part.tree.digest));
            element(&mut body, "Size", &part.tree.length.to_string());
            body.push_str("</Part>");
        }
        end(&mut body, "ListPartsResult");
        Ok(xml(StatusCode::OK, body))
    }

    /// # Errors
    /// Rejects completion without a matching, durable published file record.
    pub fn complete(
        session: &MultipartSession,
        record: &FileRecord,
        response_url: &str,
    ) -> Result<Response<Vec<u8>>, FileResponseError> {
        session.validate().map_err(|_| FileResponseError::Invalid)?;
        record.validate().map_err(|_| FileResponseError::Invalid)?;
        if session.phase != MultipartPhase::Published
            || session.published != Some(record.file)
            || session.location != record.location
            || response_url.len() > 2048
            || !(response_url.starts_with("https://") || response_url.starts_with("http://"))
            || session
                .completion
                .as_ref()
                .and_then(|completion| completion.candidate.as_ref())
                .map_or(true, |candidate| {
                    candidate.length != record.length || candidate.digest != record.digest
                })
        {
            return Err(FileResponseError::Invalid);
        }
        let mut body = start("CompleteMultipartUploadResult");
        element(&mut body, "Location", response_url);
        location_fields(&mut body, session);
        element(&mut body, "ETag", &etag(record.digest));
        end(&mut body, "CompleteMultipartUploadResult");
        Ok(xml(StatusCode::OK, body))
    }

    #[must_use]
    pub fn abort() -> Response<Vec<u8>> {
        let mut response = Response::new(Vec::new());
        *response.status_mut() = StatusCode::NO_CONTENT;
        response
    }

    /// Formats a bounded S3 error body using a stable code and request identity.
    /// # Errors
    /// Rejects unbounded or invalid resource and request identifiers.
    pub fn error(
        code: FileS3ErrorCode,
        resource: &str,
        request_id: &str,
    ) -> Result<Response<Vec<u8>>, FileResponseError> {
        if resource.len() > 2048 || request_id.len() > 128 || request_id.is_empty() {
            return Err(FileResponseError::Invalid);
        }
        let mut body = format!("{XML_PREFIX}<Error>");
        element(&mut body, "Code", code.name());
        element(&mut body, "Message", code.message());
        element(&mut body, "Resource", resource);
        element(&mut body, "RequestId", request_id);
        end(&mut body, "Error");
        Ok(xml(code.status(), body))
    }
}

fn xml(status: StatusCode, body: String) -> Response<Vec<u8>> {
    let mut response = Response::new(body.into_bytes());
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(XML_TYPE));
    response
}

fn etag(digest: [u8; 32]) -> String {
    let mut tag = String::with_capacity(66);
    tag.push('"');
    for byte in digest {
        write!(tag, "{byte:02x}").expect("string writes do not fail");
    }
    tag.push('"');
    tag
}

fn start(root: &str) -> String {
    format!("{XML_PREFIX}<{root} xmlns=\"{XML_NAMESPACE}\">")
}

fn end(body: &mut String, root: &str) {
    body.push_str("</");
    body.push_str(root);
    body.push('>');
}

fn location_fields(body: &mut String, session: &MultipartSession) {
    element(body, "Bucket", &session.location.table().bucket());
    element(body, "Key", &session.location.object_key());
}

fn element(body: &mut String, name: &str, value: &str) {
    body.push('<');
    body.push_str(name);
    body.push('>');
    for character in value.chars() {
        match character {
            '&' => body.push_str("&amp;"),
            '<' => body.push_str("&lt;"),
            '>' => body.push_str("&gt;"),
            '"' => body.push_str("&quot;"),
            '\'' => body.push_str("&apos;"),
            _ => body.push(character),
        }
    }
    body.push_str("</");
    body.push_str(name);
    body.push('>');
}
