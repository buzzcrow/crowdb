// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! Table-scoped `FileIO` through the configured Catalog origin and fresh writer grants.
use super::{credentials, signing, ApiError, BODY_LIMIT};
use crate::{
    error::{err_400, err_502},
    state::AppState,
};
use axum::{body::Body, extract::Request, response::Response};
use serde_json::Value;

fn header<'a>(request: &'a Request, name: &str) -> Result<&'a str, ApiError> {
    if request.headers().get_all(name).iter().count() != 1 {
        return Err(err_400("Expected one file scope header"));
    }
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() <= 8192)
        .ok_or_else(|| err_400("Invalid file scope header"))
}

fn credential_url(origin: &str, endpoint: &str) -> Result<reqwest::Url, ApiError> {
    let (path, _) = endpoint
        .split_once('?')
        .ok_or_else(|| err_400("Missing table identity"))?;
    let url = reqwest::Url::parse(&format!("{origin}{endpoint}"))
        .map_err(|_| err_400("Invalid credential endpoint"))?;
    let parts: Vec<_> = path.split('/').collect();
    if parts.len() != 7
        || parts[..3] != ["", "v1", "namespaces"]
        || parts[4] != "tables"
        || parts[6] != "credentials"
        || [parts[3], parts[5]].iter().any(|value| {
            value.is_empty() || *value == "." || *value == ".." || value.contains('#') || value.contains('\\')
        })
    {
        return Err(err_400("Invalid table credential scope"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for (name, value) in url.query_pairs() {
        if !seen.insert(name.to_string()) {
            return Err(err_400("Duplicate credential selector"));
        }
        let valid = match name.as_ref() {
            "table-id" | "storage-namespace-id" => {
                value.len() == 32
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    && value.bytes().any(|byte| byte != b'0')
            }
            "format-version" => matches!(value.as_ref(), "1" | "2" | "3"),
            _ => false,
        };
        if !valid {
            return Err(err_400("Invalid credential selector"));
        }
    }
    if !seen.contains("table-id") {
        return Err(err_400("Missing immutable table identity"));
    }
    if url.origin().ascii_serialization() != origin || url.path() != path || url.fragment().is_some() {
        return Err(err_400("Invalid credential endpoint"));
    }
    Ok(url)
}

async fn grant(
    client: &reqwest::Client,
    state: &AppState,
    target: &str,
    endpoint: &str,
) -> Result<Value, ApiError> {
    let token = credentials::writer(state).map_err(err_502)?;
    let mut response = client
        .get(credential_url(target, endpoint)?)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|_| err_502("Could not refresh table credentials"))?;
    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| err_502("Could not read table credentials"))?
    {
        if bytes.len() + chunk.len() > 65536 {
            return Err(err_502("Table credentials exceed 64 KiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err((
            status,
            axum::Json(crate::error::ErrorBody {
                error: format!(
                    "Table credential refresh failed: {}",
                    String::from_utf8_lossy(&bytes)
                ),
            }),
        ));
    }
    serde_json::from_slice(&bytes).map_err(|_| err_502("Invalid table credentials"))
}

fn file_query(request: &Request) -> Result<String, ApiError> {
    if request.headers().get_all("x-crowdb-file-query").iter().count() > 1 {
        return Err(err_400("Duplicate file query header"));
    }
    let query = request
        .headers()
        .get("x-crowdb-file-query")
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| err_400("Invalid file query"))?
        .unwrap_or("");
    if query.len() > 2048 {
        return Err(err_400("File query exceeds its limit"));
    }
    let mut names = std::collections::BTreeSet::new();
    let file_query = reqwest::Url::parse(&format!("http://file.invalid/?{query}"))
        .map_err(|_| err_400("Invalid file query"))?;
    for (name, _) in file_query.query_pairs() {
        if !matches!(
            name.as_ref(),
            "crowdb-native-upload"
                | "uploads"
                | "uploadId"
                | "partNumber"
                | "part-number-marker"
                | "max-parts"
        ) || !names.insert(name.to_string())
        {
            return Err(err_400("Unsupported file query"));
        }
    }
    Ok(query.to_owned())
}

fn file_location(location: &str) -> Result<reqwest::Url, ApiError> {
    let file = reqwest::Url::parse(location).map_err(|_| err_400("Invalid file location"))?;
    if file.scheme() != "s3"
        || file.host_str().is_none()
        || !file.username().is_empty()
        || file.password().is_some()
        || file.query().is_some()
        || file.fragment().is_some()
        || file.as_str() != location
    {
        return Err(err_400("Expected a canonical S3 file location"));
    }
    Ok(file)
}

fn file_credentials(grant: &Value, location: &str) -> Result<credentials::S3Credentials, ApiError> {
    let grants = grant["storage-credentials"]
        .as_array()
        .filter(|values| values.len() == 1)
        .ok_or_else(|| err_502("Invalid table credential grant"))?;
    let prefix = grants[0]["prefix"]
        .as_str()
        .ok_or_else(|| err_502("Missing table credential prefix"))?;
    if !location
        .strip_prefix(&format!("{}/", prefix.trim_end_matches('/')))
        .is_some_and(|tail| !tail.is_empty())
    {
        return Err(err_400("File is outside the authorized table"));
    }
    let config = &grants[0]["config"];
    let value = |name: &str| {
        config[name]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| err_502("Incomplete file credentials"))
    };
    Ok(credentials::S3Credentials {
        access: value("s3.access-key-id")?,
        secret: value("s3.secret-access-key")?,
        session: Some(value("s3.session-token")?),
        region: "us-east-1".into(),
    })
}

pub(super) async fn proxy(state: AppState, request: Request) -> Result<Response, ApiError> {
    if !matches!(
        request.method().as_str(),
        "GET" | "HEAD" | "PUT" | "POST" | "DELETE"
    ) {
        return Err(err_400("Unsupported file method"));
    }
    let target = super::origin(&state, "iceberg")?.ok_or_else(|| err_502("Catalog endpoint unavailable"))?;
    let endpoint = header(&request, "x-crowdb-file-credentials")?;
    let location = header(&request, "x-crowdb-file-location")?;
    let query = file_query(&request)?;
    let file = file_location(location)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|reason| err_502(reason.to_string()))?;
    let grant = grant(&client, &state, &target, endpoint).await?;
    let credentials = file_credentials(&grant, location)?;
    let mut url = reqwest::Url::parse(&format!("{target}/{}{}", file.host_str().unwrap(), file.path()))
        .map_err(|_| err_400("Invalid native file URL"))?;
    if !query.is_empty() {
        url.set_query(Some(&query));
    }
    let method = request.method().clone();
    let mut headers = super::request_headers(request.headers());
    let body = axum::body::to_bytes(request.into_body(), BODY_LIMIT)
        .await
        .map_err(|_| err_400("File part exceeds 16 MiB; use multipart upload"))?;
    signing::sign(&credentials, &method, &url, &body, &mut headers).map_err(err_502)?;
    let upstream = client
        .request(method, url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .map_err(|_| err_502("File transfer reply unavailable; retain the saved input before retrying"))?;
    let mut response = Response::builder().status(upstream.status());
    for name in ["content-type", "content-length", "etag", "content-range"] {
        if let Some(value) = upstream.headers().get(name) {
            response = response.header(name, value);
        }
    }
    response
        .body(Body::from_stream(upstream.bytes_stream()))
        .map_err(|reason| err_502(reason.to_string()))
}
