// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::AuthorityError;
use http_body_util::Full;
use hyper::{body::Bytes, Response, StatusCode};

pub(super) struct ApiError {
    status: StatusCode,
    code: &'static str,
    retryable: bool,
}
impl ApiError {
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
    fn new(status: StatusCode, code: &'static str) -> Self {
        Self {
            status,
            code,
            retryable: status == StatusCode::SERVICE_UNAVAILABLE,
        }
    }
    pub(super) fn response(&self, request_id: &str) -> Response<Full<Bytes>> {
        let body = serde_json::json!({"code":self.code,"message":self.code,"request_id":request_id,"retryable":self.retryable});
        Response::builder()
            .status(self.status)
            .header("content-type", "application/json")
            .header("x-request-id", request_id)
            .body(Full::new(Bytes::from(body.to_string())))
            .expect("static response")
    }
}
impl From<AuthorityError> for ApiError {
    fn from(error: AuthorityError) -> Self {
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
