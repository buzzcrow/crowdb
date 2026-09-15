// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Stable S3 REST error responses.

use hyper::StatusCode;

const NOT_IMPLEMENTED_MESSAGE: &str = "A header you provided implies functionality that is not implemented.";

/// Stable public S3 error classes. Lower-layer details never cross this boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum S3ErrorCode {
    NotImplemented,
    NoSuchBucket,
    NoSuchKey,
    BucketNotEmpty,
    InvalidRequest,
    InvalidRange,
    PreconditionFailed,
    SlowDown,
    InternalError,
    ServiceUnavailable,
    AccessDenied,
    InvalidAccessKeyId,
    SignatureDoesNotMatch,
    RequestTimeTooSkewed,
    InvalidDigest,
    BadDigest,
    XAmzContentSHA256Mismatch,
}

impl S3ErrorCode {
    const fn code(self) -> &'static str {
        match self {
            Self::NotImplemented => "NotImplemented",
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::InvalidRequest => "InvalidRequest",
            Self::InvalidRange => "InvalidRange",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::SlowDown => "SlowDown",
            Self::InternalError => "InternalError",
            Self::ServiceUnavailable => "ServiceUnavailable",
            Self::AccessDenied => "AccessDenied",
            Self::InvalidAccessKeyId => "InvalidAccessKeyId",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::RequestTimeTooSkewed => "RequestTimeTooSkewed",
            Self::InvalidDigest => "InvalidDigest",
            Self::BadDigest => "BadDigest",
            Self::XAmzContentSHA256Mismatch => "XAmzContentSHA256Mismatch",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::NotImplemented => NOT_IMPLEMENTED_MESSAGE,
            Self::NoSuchBucket => "The specified bucket does not exist.",
            Self::NoSuchKey => "The specified key does not exist.",
            Self::BucketNotEmpty => "The bucket you tried to delete is not empty.",
            Self::InvalidRequest => "The request is not valid for this service.",
            Self::InvalidRange => "The requested range is not satisfiable.",
            Self::PreconditionFailed => "At least one precondition failed.",
            Self::SlowDown => "Please reduce your request rate.",
            Self::ServiceUnavailable => "Service is temporarily unavailable.",
            Self::InternalError => "We encountered an internal error. Please try again.",
            Self::AccessDenied => "Access Denied.",
            Self::InvalidAccessKeyId => "The AWS access key ID you provided does not exist in our records.",
            Self::SignatureDoesNotMatch => {
                "The request signature we calculated does not match the signature you provided."
            }
            Self::RequestTimeTooSkewed => {
                "The difference between the request time and the server's time is too large."
            }
            Self::InvalidDigest => "The Content-MD5 you specified is not valid.",
            Self::BadDigest => "The Content-MD5 you specified did not match what we received.",
            Self::XAmzContentSHA256Mismatch => {
                "The provided x-amz-content-sha256 header does not match what was computed."
            }
        }
    }

    const fn status(self) -> StatusCode {
        match self {
            Self::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            Self::NoSuchBucket | Self::NoSuchKey => StatusCode::NOT_FOUND,
            Self::BucketNotEmpty => StatusCode::CONFLICT,
            Self::InvalidRequest
            | Self::RequestTimeTooSkewed
            | Self::InvalidDigest
            | Self::BadDigest
            | Self::XAmzContentSHA256Mismatch => StatusCode::BAD_REQUEST,
            Self::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            Self::SlowDown | Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            Self::AccessDenied | Self::InvalidAccessKeyId | Self::SignatureDoesNotMatch => {
                StatusCode::FORBIDDEN
            }
        }
    }
}

/// A stable S3 error response safe to expose to an external client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3Error {
    code: S3ErrorCode,
    resource: String,
    request_id: String,
    host_id: String,
}

impl S3Error {
    /// Creates the standard S3 response for an unsupported operation.
    #[must_use]
    pub fn not_implemented(resource: String, request_id: String, host_id: String) -> Self {
        Self::new(S3ErrorCode::NotImplemented, resource, request_id, host_id)
    }

    /// Constructs a bounded, topology-safe S3 failure response.
    #[must_use]
    pub fn new(code: S3ErrorCode, resource: String, request_id: String, host_id: String) -> Self {
        Self {
            code,
            resource: bounded(resource, 1024),
            request_id: bounded(request_id, 128),
            host_id: bounded(host_id, 128),
        }
    }

    /// Returns the HTTP status for this error.
    #[must_use]
    pub const fn status_code(&self) -> StatusCode {
        self.code.status()
    }

    /// Returns the S3 XML media type.
    #[must_use]
    pub const fn content_type(&self) -> &'static str {
        "application/xml"
    }

    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    #[must_use]
    pub fn host_id(&self) -> &str {
        &self.host_id
    }

    #[must_use]
    pub const fn retry_after_seconds(&self) -> Option<u64> {
        match self.code {
            S3ErrorCode::SlowDown => Some(1),
            _ => None,
        }
    }

    /// Serializes the standard S3 REST error XML body.
    #[must_use]
    pub fn to_xml(&self) -> String {
        let mut output =
            String::with_capacity(self.resource.len() + self.request_id.len() + self.host_id.len() + 224);
        output.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        output.push_str("<Error><Code>");
        output.push_str(self.code.code());
        output.push_str("</Code><Message>");
        output.push_str(self.code.message());
        output.push_str("</Message><Resource>");
        push_xml_escaped(&mut output, &self.resource);
        output.push_str("</Resource><RequestId>");
        push_xml_escaped(&mut output, &self.request_id);
        output.push_str("</RequestId><HostId>");
        push_xml_escaped(&mut output, &self.host_id);
        output.push_str("</HostId></Error>");
        output
    }
}

fn bounded(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

fn push_xml_escaped(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '\"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            _ => output.push(character),
        }
    }
}
