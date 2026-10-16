// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{HttpClient, LimitedJson};
use crate::{Error, ServiceError};
use bytes::{Bytes, BytesMut};
use serde::Serialize;

impl HttpClient {
    /// Opens a bounded-window response without imposing a whole-body size cap.
    /// # Errors
    /// Returns admission, JSON, service, transport or version errors.
    pub async fn stream<T: Serialize + ?Sized>(
        &self,
        method: &str,
        path: &[&str],
        value: Option<&T>,
    ) -> Result<crate::ResponseStream, Error> {
        let deadline = tokio::time::Instant::now()
            .checked_add(self.config.stream_timeout)
            .ok_or_else(|| Error::Invalid("deadline outside clock range".into()))?;
        let reserve = self
            .config
            .max_response_bytes
            .checked_mul(5)
            .and_then(|n| n.checked_add(self.config.max_request_bytes))
            .ok_or(Error::Limit)?;
        let permit = self.admission.acquire(reserve)?;
        let mut body = LimitedJson {
            bytes: Vec::new(),
            limit: self.config.max_request_bytes,
        };
        if let Some(value) = value {
            serde_json::to_writer(&mut body, value).map_err(|error| {
                if error.is_io() {
                    Error::Limit
                } else {
                    Error::Codec(error)
                }
            })?;
        }
        let mut response = self
            .headers(
                method,
                path,
                Bytes::from(body.bytes),
                "application/json",
                (true, true),
                &[],
            )
            .await?;
        let status = response.status();
        if !status.is_success() {
            let mut body = BytesMut::new();
            while let Some(chunk) = response.chunk().await? {
                if body.len().saturating_add(chunk.len()) > 64 * 1024 {
                    return Err(Error::Limit);
                }
                body.extend_from_slice(&chunk);
            }
            let error = serde_json::from_slice(&body).unwrap_or(ServiceError {
                code: "invalid_error_response".into(),
                message: "invalid error response".into(),
                request_id: String::new(),
                retryable: false,
            });
            return Err(Error::Service {
                status: status.as_u16(),
                error,
            });
        }
        if response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            != Some("application/vnd.crowdb.dataset-batch.v1")
        {
            return Err(Error::Integrity);
        }
        Ok(crate::ResponseStream::new(
            response,
            permit,
            self.config.max_response_bytes,
            deadline,
        ))
    }
}
