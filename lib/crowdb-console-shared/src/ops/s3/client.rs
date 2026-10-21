// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Selected S3 endpoint transport, independent of runtime/process lifecycle.

use super::{http_error, load, sign_request, S3HttpExchange, S3HttpRequest};
use crate::error::{Error, Result};
use std::{path::Path, time::Duration};

const BODY_PREVIEW_LIMIT: usize = 64 * 1024;

/// Credentials selected from the attached cluster, never formatted in diagnostics.
#[derive(Clone)]
pub struct S3Credentials {
    pub access: String,
    pub secret: String,
    pub region: String,
    pub session: Option<String>,
}

/// Reusable thin client for one local S3 endpoint.
#[derive(Clone)]
pub struct S3HttpClient {
    endpoint: String,
    client: reqwest::Client,
    credentials: Option<S3Credentials>,
}

impl S3HttpClient {
    /// Build a client with the standard per-request timeout.
    ///
    /// # Errors
    /// Returns an error when the HTTP client cannot be constructed.
    pub fn new(endpoint: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| http_error(&error))?;
        Ok(Self {
            endpoint,
            client,
            credentials: None,
        })
    }

    /// Build an authenticated client for a selected cluster's published S3 origin.
    ///
    /// # Errors
    /// Rejects malformed origins or empty credentials before issuing requests.
    pub fn signed(endpoint: String, credentials: S3Credentials) -> Result<Self> {
        let origins = crate::deployment::target::management_origins(&[endpoint])
            .map_err(|_| Error::Config("invalid selected S3 origin".into()))?;
        if [&credentials.access, &credentials.secret, &credentials.region]
            .iter()
            .any(|value| value.is_empty() || value.contains(['\n', '\r']))
        {
            return Err(Error::Config("invalid selected S3 credentials".into()));
        }
        let mut client = Self::new(origins[0].clone())?;
        client.credentials = Some(credentials);
        Ok(client)
    }

    /// Load the endpoint persisted for a mini-cluster.
    ///
    /// # Errors
    /// Returns an error when the cluster record or HTTP client is invalid.
    pub fn from_data_dir(data_dir: &Path) -> Result<Self> {
        let (_, record) = load(data_dir)?;
        Self::new(record.endpoint)
    }

    /// Send one S3 request, optionally with an inclusive byte range.
    ///
    /// # Errors
    /// Returns an error for an invalid range or transport/protocol/S3 failure.
    pub async fn request(
        &self,
        method: reqwest::Method,
        bucket: Option<&str>,
        object: Option<&str>,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
        range: Option<(u64, u64)>,
    ) -> Result<(u16, Vec<u8>)> {
        self.request_exchange(method, bucket, object, query, body, range)
            .await?
            .into_result()
    }

    /// Send one S3 request and retain HTTP metadata even for non-2xx status.
    ///
    /// # Errors
    /// Returns an error for an invalid range, request construction failure, or
    /// transport/protocol failure.
    pub async fn request_exchange(
        &self,
        method: reqwest::Method,
        bucket: Option<&str>,
        object: Option<&str>,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
        range: Option<(u64, u64)>,
    ) -> Result<S3HttpExchange> {
        self.request_exchange_with_headers(
            method,
            bucket,
            object,
            query,
            body,
            range,
            reqwest::header::HeaderMap::new(),
        )
        .await
    }

    /// Send one S3 request with caller-supplied headers and retain HTTP
    /// metadata even for a non-2xx status.
    ///
    /// # Errors
    /// Returns an error for an invalid range, request construction failure, or
    /// transport/protocol failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn request_exchange_with_headers(
        &self,
        method: reqwest::Method,
        bucket: Option<&str>,
        object: Option<&str>,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
        range: Option<(u64, u64)>,
        headers: reqwest::header::HeaderMap,
    ) -> Result<S3HttpExchange> {
        let url = request_url(&self.endpoint, bucket, object, query)?;
        let mut headers = headers;
        if let Some(credentials) = &self.credentials {
            sign_request(
                credentials,
                &method,
                &url,
                body.as_deref().unwrap_or_default(),
                &mut headers,
            )
            .map_err(Error::Config)?;
        }
        let mut request = self.client.request(method, url).headers(headers);
        if let Some((start, end)) = range {
            if start > end {
                return Err(Error::Validation {
                    field: "range".into(),
                    message: "range start must not exceed end".into(),
                });
            }
            request = request.header(reqwest::header::RANGE, format!("bytes={start}-{end}"));
        }
        let body_bytes = body.as_ref().map(Vec::len);
        let textual_body = request
            .try_clone()
            .and_then(|builder| builder.build().ok())
            .and_then(|request| request.headers().get(reqwest::header::CONTENT_TYPE).cloned())
            .and_then(|value| value.to_str().ok().map(str::to_ascii_lowercase))
            .is_some_and(|value| {
                value.starts_with("text/") || value.contains("json") || value.contains("xml")
            });
        let body_preview = body
            .as_ref()
            .filter(|_| textual_body)
            .map(|body| body.iter().copied().take(BODY_PREVIEW_LIMIT).collect::<Vec<_>>());
        let body_preview_truncated =
            body_bytes.is_some_and(|bytes| textual_body && bytes > BODY_PREVIEW_LIMIT);
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_LENGTH, body.len())
                .body(body);
        }
        let mut request = request.build().map_err(|error| http_error(&error))?;
        require_host(&mut request)?;
        let request_summary = S3HttpRequest {
            method: request.method().clone(),
            url: request.url().clone(),
            headers: request.headers().clone(),
            body_bytes,
            body_preview,
            body_preview_truncated,
        };
        let response = self
            .client
            .execute(request)
            .await
            .map_err(|error| http_error(&error))?;
        let status = response.status();
        let response_headers = response.headers().clone();
        let response_body = response
            .bytes()
            .await
            .map_err(|error| http_error(&error))?
            .to_vec();
        Ok(S3HttpExchange {
            request: request_summary,
            status,
            response_headers,
            response_body,
        })
    }
}

fn encode_path(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

fn request_url(
    endpoint: &str,
    bucket: Option<&str>,
    object: Option<&str>,
    query: &[(&str, String)],
) -> Result<reqwest::Url> {
    let mut url = endpoint.to_owned();
    if let Some(bucket) = bucket {
        url.push('/');
        url.push_str(&encode_path(bucket));
    }
    if let Some(object) = object {
        url.push('/');
        url.push_str(&object.split('/').map(encode_path).collect::<Vec<_>>().join("/"));
    }
    if !query.is_empty() {
        url.push('?');
        url.push_str(
            &form_urlencoded::Serializer::new(String::new())
                .extend_pairs(query.iter().map(|(k, v)| (*k, v.as_str())))
                .finish(),
        );
    }
    reqwest::Url::parse(&url).map_err(|_| Error::Config("invalid S3 request URL".into()))
}

fn require_host(request: &mut reqwest::Request) -> Result<()> {
    if !request.headers().contains_key(reqwest::header::HOST) {
        let host = request.url().host_str().ok_or_else(|| Error::Validation {
            field: "endpoint".into(),
            message: "S3 endpoint has no host".into(),
        })?;
        let host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host.to_string()
        };
        let authority = request
            .url()
            .port()
            .map_or(host.clone(), |port| format!("{host}:{port}"));
        let value =
            reqwest::header::HeaderValue::from_str(&authority).map_err(|error| Error::Validation {
                field: "endpoint".into(),
                message: format!("invalid S3 endpoint authority: {error}"),
            })?;
        request.headers_mut().insert(reqwest::header::HOST, value);
    }
    Ok(())
}
