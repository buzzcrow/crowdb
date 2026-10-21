// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Request/response metadata for workload accounting and CLI diagnostics.

use crate::error::{Error, Result};

/// Captured metadata for an outbound S3 HTTP request.
#[derive(Debug)]
pub struct S3HttpRequest {
    pub method: reqwest::Method,
    pub url: reqwest::Url,
    pub headers: reqwest::header::HeaderMap,
    /// `None` means no body was supplied; `Some(0)` is an explicit empty body.
    pub body_bytes: Option<usize>,
    /// Bounded copy of a textual request body for interactive display.
    pub body_preview: Option<Vec<u8>>,
    pub body_preview_truncated: bool,
}

/// One completed S3 HTTP request/response exchange.
#[derive(Debug)]
pub struct S3HttpExchange {
    pub request: S3HttpRequest,
    pub status: reqwest::StatusCode,
    pub response_headers: reqwest::header::HeaderMap,
    pub response_body: Vec<u8>,
}

impl S3HttpExchange {
    /// Convert the captured exchange to the compact result used by non-CLI
    /// callers, preserving the existing non-success error behavior.
    ///
    /// # Errors
    /// Returns an upstream error for a non-2xx HTTP response.
    pub fn into_result(self) -> Result<(u16, Vec<u8>)> {
        if !self.status.is_success() {
            return Err(Error::UpstreamRpc {
                node_id: "s3".into(),
                status: format!(
                    "HTTP {}: {}",
                    self.status.as_u16(),
                    String::from_utf8_lossy(&self.response_body)
                ),
            });
        }
        Ok((self.status.as_u16(), self.response_body))
    }
}
