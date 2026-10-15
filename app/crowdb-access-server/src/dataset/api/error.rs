// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::dataset::DatasetBody;
use crowdb_access_dataset::AuthorityError;
use hyper::{body::Bytes, Response, StatusCode};

#[derive(Debug)]
pub(super) struct ApiError {
    status: StatusCode,
    code: &'static str,
    retryable: bool,
}
impl ApiError {
    pub(super) fn conflict() -> Self {
        Self::new(StatusCode::CONFLICT, "conflict")
    }
    pub(super) fn capacity() -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "capacity",
            retryable: true,
        }
    }
    pub(super) fn invalid() -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request")
    }
    pub(super) fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found")
    }
    pub(super) fn integrity() -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "integrity")
    }
    pub(super) fn unsupported() -> Self {
        Self::new(StatusCode::NOT_IMPLEMENTED, "unsupported")
    }
    pub(super) fn too_large() -> Self {
        Self::new(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large")
    }
    pub(super) fn timeout() -> Self {
        Self::new(StatusCode::GATEWAY_TIMEOUT, "timeout")
    }
    pub(super) fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized")
    }
    pub(super) fn new(status: StatusCode, code: &'static str) -> Self {
        Self {
            status,
            code,
            retryable: status == StatusCode::SERVICE_UNAVAILABLE,
        }
    }
    pub(super) fn code(&self) -> &'static str {
        self.code
    }
    pub(super) fn retryable(&self) -> bool {
        self.retryable
    }
    pub(super) fn response(&self, request_id: &str) -> Response<DatasetBody> {
        let body = serde_json::json!({"code":self.code,"message":self.code,"request_id":request_id,"retryable":self.retryable});
        Response::builder()
            .status(self.status)
            .header("content-type", "application/json")
            .header("x-request-id", request_id)
            .body(crate::dataset::body(Bytes::from(body.to_string())))
            .expect("static response")
    }
}

impl From<crowdb_access_dataset::UploadError> for ApiError {
    fn from(error: crowdb_access_dataset::UploadError) -> Self {
        use crowdb_access_dataset::UploadError;
        match error {
            UploadError::Invalid => Self::invalid(),
            UploadError::NotFound => Self::not_found(),
            UploadError::Conflict => Self::conflict(),
            UploadError::Integrity => Self::integrity(),
            UploadError::Capacity => Self::capacity(),
            UploadError::Storage(_) | UploadError::Store(_) => {
                tracing::error!(%error, "Dataset upload failed; returning retryable unavailable response");
                Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable")
            }
            UploadError::Pending => Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
        }
    }
}
impl From<AuthorityError> for ApiError {
    fn from(error: AuthorityError) -> Self {
        if matches!(
            &error,
            AuthorityError::Invalid(crowdb_access_dataset::DatasetError::ReadCancelled)
        ) {
            return Self::new(StatusCode::from_u16(499).expect("valid status"), "cancelled");
        }
        if matches!(
            &error,
            AuthorityError::Invalid(
                crowdb_access_dataset::DatasetError::SampleNotFound
                    | crowdb_access_dataset::DatasetError::PayloadNotFound
            )
        ) {
            return Self::not_found();
        }
        if matches!(
            &error,
            AuthorityError::Invalid(
                crowdb_access_dataset::DatasetError::ReadLimitExceeded
                    | crowdb_access_dataset::DatasetError::InlineValueTooLarge
            )
        ) {
            return Self::too_large();
        }
        if matches!(&error, AuthorityError::Corrupt) {
            return Self::integrity();
        }
        let status = super::super::status_for_authority(&error);
        let code = match status {
            StatusCode::BAD_REQUEST => "invalid_request",
            StatusCode::CONFLICT => "conflict",
            StatusCode::NOT_FOUND => "not_found",
            _ => "unavailable",
        };
        Self::new(status, code)
    }
}
