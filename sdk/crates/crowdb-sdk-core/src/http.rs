// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use reqwest::{Client, Method, Url};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    admission::{Admission, Permit},
    Error, HttpConfig, ServiceError,
};

/// Clones share the HTTP pool and admission limits. No runtime is created here.
///
/// Returned bytes and decoded values transfer ownership to the caller. The
/// client bounds transport allocations; callers budget retained results.
#[derive(Clone)]
pub struct HttpClient {
    inner: Client,
    endpoint: Url,
    credentials: Arc<dyn crate::CredentialProvider>,
    config: HttpConfig,
    admission: Arc<Admission>,
}

impl HttpClient {
    /// # Errors
    /// Rejects invalid HTTP configuration, credentials or endpoint.
    pub fn new(endpoint: &str, bearer: Option<String>, timeout: Duration) -> Result<Self, Error> {
        Self::with_config(
            endpoint,
            bearer,
            HttpConfig {
                request_timeout: timeout,
                connect_timeout: timeout.min(Duration::from_secs(10)),
                ..HttpConfig::default()
            },
        )
    }

    /// # Errors
    /// Rejects invalid endpoints, credentials and inconsistent limits.
    pub fn with_config(endpoint: &str, bearer: Option<String>, config: HttpConfig) -> Result<Self, Error> {
        Self::with_provider(
            endpoint,
            Arc::new(crate::credentials::StaticCredential(bearer)),
            config,
        )
    }
    /// # Errors
    /// Rejects invalid configuration, TLS roots or credential providers.
    pub fn with_provider(
        endpoint: &str,
        credentials: Arc<dyn crate::CredentialProvider>,
        config: HttpConfig,
    ) -> Result<Self, Error> {
        let endpoint = Url::parse(endpoint).map_err(|_| Error::Invalid("endpoint URL".into()))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !config.valid()
        {
            return Err(Error::Invalid("endpoint or resource limits".into()));
        }
        if let Some(token) = &credentials.bearer()? {
            if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
                return Err(Error::Invalid("bearer credential".into()));
            }
        }
        let mut builder = Client::builder()
            .connect_timeout(config.connect_timeout)
            .pool_idle_timeout(config.pool_idle_timeout)
            .pool_max_idle_per_host(config.max_idle_connections_per_host)
            .redirect(reqwest::redirect::Policy::none());
        if let Some(pem) = &config.ca_pem {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(pem)
                    .map_err(|_| Error::Invalid("TLS root certificate".into()))?,
            );
        }
        let inner = builder.build()?;
        Ok(Self {
            inner,
            endpoint,
            credentials,
            admission: Arc::new(Admission::new(config.clone())),
            config,
        })
    }

    /// # Errors
    /// Returns byte capacity exhaustion before a transfer buffer is allocated.
    pub fn reserve_buffer(&self, size: usize) -> Result<crate::BufferPermit, Error> {
        if size > self.config.max_request_bytes {
            return Err(Error::Limit);
        }
        Ok(crate::BufferPermit {
            _permit: self.admission.acquire_bytes(size)?,
        })
    }
    /// # Errors
    /// Returns admission, transport, service, limit or decoding errors.
    pub async fn json<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        method: &str,
        path: &[&str],
        value: Option<&T>,
    ) -> Result<R, Error> {
        let (bytes, _permit) = self
            .serialized_request(method, path, value, (false, false))
            .await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Sends bounded JSON and returns a bounded, possibly binary, response.
    /// # Errors
    /// Returns admission, encoding, service, transport or limit errors.
    pub async fn json_request<T: Serialize + ?Sized>(
        &self,
        method: &str,
        path: &[&str],
        value: Option<&T>,
    ) -> Result<Bytes, Error> {
        let (bytes, _permit) = self
            .serialized_request(method, path, value, (false, false))
            .await?;
        Ok(bytes)
    }

    /// Executes an explicitly token-idempotent operation within one retry deadline.
    /// # Errors
    /// Returns deadline, admission, transport, service or decoding failures.
    pub async fn json_idempotent<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        method: &str,
        path: &[&str],
        value: Option<&T>,
    ) -> Result<R, Error> {
        let (bytes, _permit) = self
            .serialized_request(method, path, value, (true, false))
            .await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
    /// Uses the transfer deadline for integrity verification of large completed uploads.
    /// # Errors
    /// Returns deadline, admission, transport, service or decoding failures.
    pub async fn json_long_idempotent<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        method: &str,
        path: &[&str],
        value: Option<&T>,
    ) -> Result<R, Error> {
        let (bytes, _permit) = self.serialized_request(method, path, value, (true, true)).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
    /// # Errors
    /// Returns bounded query, admission, transport, service or decoding failures.
    pub async fn json_query<R: DeserializeOwned>(
        &self,
        method: &str,
        path: &[&str],
        query: &[(&str, &str)],
    ) -> Result<R, Error> {
        let _permit = self.admission.acquire(self.config.max_response_bytes)?;
        let mut response = self
            .headers(
                method,
                path,
                Bytes::new(),
                "application/json",
                (true, false),
                query,
            )
            .await?;
        let mut bytes = BytesMut::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len().saturating_add(chunk.len()) > self.config.max_response_bytes {
                return Err(Error::Limit);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
    async fn serialized_request<T: Serialize + ?Sized>(
        &self,
        method: &str,
        path: &[&str],
        value: Option<&T>,
        mode: (bool, bool),
    ) -> Result<(Bytes, Permit), Error> {
        let permit = self
            .admission
            .acquire(self.config.max_request_bytes + self.config.max_response_bytes)?;
        let mut body = LimitedJson {
            bytes: Vec::new(),
            limit: self.config.max_request_bytes,
        };
        if let Some(value) = value {
            if let Err(error) = serde_json::to_writer(&mut body, value) {
                return Err(if error.is_io() {
                    Error::Limit
                } else {
                    Error::Codec(error)
                });
            }
        }
        let bytes = self
            .send(method, path, Bytes::from(body.bytes), "application/json", mode)
            .await?;
        Ok((bytes, permit))
    }

    /// Executes one bounded request. Mutation retries are deliberately explicit.
    /// Dropping the future cancels this request and releases its admission.
    /// # Errors
    /// Returns admission, transport, service or byte-limit errors.
    pub async fn request(
        &self,
        method: &str,
        path: &[&str],
        body: Bytes,
        content_type: &str,
    ) -> Result<Bytes, Error> {
        if body.len() > self.config.max_request_bytes {
            return Err(Error::Limit);
        }
        let bytes = body
            .len()
            .checked_add(self.config.max_response_bytes)
            .ok_or(Error::Limit)?;
        let _permit = self.admission.acquire(bytes)?;
        self.send(method, path, body, content_type, (false, false)).await
    }

    /// # Errors
    /// Returns bounded replay, deadline, transport or service errors.
    pub async fn request_idempotent(
        &self,
        method: &str,
        path: &[&str],
        body: Bytes,
        content_type: &str,
    ) -> Result<Bytes, Error> {
        if body.len() > self.config.max_request_bytes {
            return Err(Error::Limit);
        }
        let _permit = self.admission.acquire(
            body.len()
                .checked_add(self.config.max_response_bytes)
                .ok_or(Error::Limit)?,
        )?;
        self.send(method, path, body, content_type, (true, false)).await
    }
    async fn send(
        &self,
        method: &str,
        path: &[&str],
        body: Bytes,
        content_type: &str,
        mode: (bool, bool),
    ) -> Result<Bytes, Error> {
        let mut response = self.headers(method, path, body, content_type, mode, &[]).await?;
        let status = response.status();
        let limit = if status.is_success() {
            self.config.max_response_bytes
        } else {
            self.config.max_response_bytes.min(64 * 1024)
        };
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(Error::Limit);
        }
        let mut bytes = BytesMut::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len().saturating_add(chunk.len()) > limit {
                return Err(Error::Limit);
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let error = serde_json::from_slice(&bytes).unwrap_or(ServiceError {
                code: "invalid_error_response".into(),
                message: "service error body was not valid JSON".into(),
                request_id: String::new(),
                retryable: false,
            });
            return Err(Error::Service {
                status: status.as_u16(),
                error,
            });
        }
        Ok(bytes.freeze())
    }
    async fn headers(
        &self,
        method: &str,
        path: &[&str],
        body: Bytes,
        content_type: &str,
        mode: (bool, bool),
        query: &[(&str, &str)],
    ) -> Result<reqwest::Response, Error> {
        let (retry, streaming) = mode;
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
        let method = Method::from_bytes(method.as_bytes()).map_err(|_| Error::Invalid("method".into()))?;
        let total = if streaming {
            self.config.stream_timeout
        } else {
            self.config.overall_timeout
        };
        let deadline = tokio::time::Instant::now() + total;
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
            if let Some(token) = self.credentials.bearer()? {
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
                tokio::time::sleep(self.config.retry_backoff.saturating_mul(factor)),
            )
            .await
            .map_err(|_| Error::Deadline)?;
        }
        Err(Error::Deadline)
    }

    /// Opens a bounded-window response without imposing a whole-body size cap.
    /// # Errors
    /// Returns admission, JSON, service, transport or version errors.
    pub async fn stream<T: Serialize + ?Sized>(
        &self,
        method: &str,
        path: &[&str],
        value: Option<&T>,
    ) -> Result<crate::ResponseStream, Error> {
        let deadline = tokio::time::Instant::now() + self.config.stream_timeout;
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

struct LimitedJson {
    bytes: Vec<u8>,
    limit: usize,
}
impl std::io::Write for LimitedJson {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.limit {
            return Err(std::io::Error::other("JSON request byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
