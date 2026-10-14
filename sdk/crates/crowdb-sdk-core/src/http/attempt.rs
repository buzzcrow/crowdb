// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::HttpClient;
use crate::{Error, ServiceError};
use bytes::{Bytes, BytesMut};
use reqwest::Method;

impl HttpClient {
    pub(super) async fn headers(
        &self,
        method: &str,
        path: &[&str],
        body: Bytes,
        content_type: &str,
        mode: (bool, bool),
        query: &[(&str, &str)],
    ) -> Result<reqwest::Response, Error> {
        let (retry, streaming) = mode;
        let url = self.request_url(path, query)?;
        let method = Method::from_bytes(method.as_bytes()).map_err(|_| Error::Invalid("method".into()))?;
        let total = if streaming {
            self.config.stream_timeout
        } else {
            self.config.overall_timeout
        };
        let deadline = tokio::time::Instant::now()
            .checked_add(total)
            .ok_or_else(|| Error::Invalid("deadline outside clock range".into()))?;
        let attempts = if retry { self.config.max_attempts } else { 1 };
        for attempt in 0..attempts {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::Deadline);
            }
            let mut request = self
                .inner
                .request(method.clone(), url.clone())
                .header("content-type", content_type)
                .body(body.clone())
                .timeout(if streaming {
                    remaining
                } else {
                    self.config.request_timeout.min(remaining)
                });
            if let Some(token) = self
                .credentials
                .bearer()
                .map_err(|_| Error::Invalid("credential provider unavailable".into()))?
            {
                if token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()) {
                    return Err(Error::Invalid("bearer credential".into()));
                }
                request = request.bearer_auth(token);
            }
            let outcome = tokio::time::timeout_at(deadline, request.send())
                .await
                .map_err(|_| Error::Deadline)?;
            let error = match outcome {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(mut response) => {
                    let status = response.status().as_u16();
                    let mut bytes = BytesMut::new();
                    while let Some(chunk) = tokio::time::timeout_at(deadline, response.chunk())
                        .await
                        .map_err(|_| Error::Deadline)??
                    {
                        if bytes.len().saturating_add(chunk.len()) > 64 * 1024 {
                            return Err(Error::Limit);
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    let error = serde_json::from_slice(&bytes).unwrap_or(ServiceError {
                        code: "invalid_error_response".into(),
                        message: "invalid error response".into(),
                        request_id: String::new(),
                        retryable: false,
                    });
                    Error::Service { status, error }
                }
                Err(error) => Error::from(error),
            };
            let transient = match &error {
                Error::Transport(error) => error.retryable(),
                Error::Service { status, error } => error.retryable && matches!(status, 429 | 503 | 504),
                _ => false,
            };
            if !transient || attempt + 1 == attempts {
                return Err(error);
            }
            let factor = 1u32 << attempt.min(10);
            tokio::time::timeout_at(
                deadline,
                tokio::time::sleep(
                    self.config
                        .retry_backoff
                        .saturating_mul(factor)
                        .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
                ),
            )
            .await
            .map_err(|_| Error::Deadline)?;
        }
        Err(Error::Deadline)
    }
    fn request_url(&self, path: &[&str], query: &[(&str, &str)]) -> Result<reqwest::Url, Error> {
        let mut encoded = self.endpoint.as_str().len();
        for component in path
            .iter()
            .copied()
            .chain(query.iter().flat_map(|(key, value)| [*key, *value]))
        {
            encoded = component
                .len()
                .checked_mul(3)
                .and_then(|n| n.checked_add(1))
                .and_then(|n| encoded.checked_add(n))
                .ok_or(Error::Limit)?;
            if encoded > 16384 {
                return Err(Error::Limit);
            }
        }
        let mut url = self.endpoint.clone();
        if path.iter().any(|segment| matches!(*segment, "" | "." | "..")) {
            return Err(Error::Invalid("empty or dot path segment".into()));
        }
        url.path_segments_mut()
            .map_err(|()| Error::Invalid("base URL".into()))?
            .pop_if_empty()
            .extend(path);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }
        Ok(url)
    }
}
