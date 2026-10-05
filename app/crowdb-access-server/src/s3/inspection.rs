// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Privileged, metadata-only object location endpoint on the S3 listener.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crowdb_access_iceberg::catalog::ManagementPrivilege;
use crowdb_access_iceberg::wire::BearerAuthenticator;
use crowdb_access_s3::bucket;
use crowdb_access_s3::inspection::{InspectionError, LocationInspector, MAX_REFERENCE_BYTES};
use crowdb_access_s3::metadata::{ChunkKvMetadataStore, MetadataKey, ObjectRecord, TenantId};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::json;

use super::{full_body, ResponseBody};

pub const OBJECT_LOCATIONS_PATH: &str = "/_crowdb/admin/object-locations";

pub struct ObjectInspector {
    authentication: BearerAuthenticator,
    metadata: Arc<ChunkKvMetadataStore>,
    tenant: TenantId,
    locations: LocationInspector,
}

impl ObjectInspector {
    /// # Errors
    /// Rejects an empty inspection continuation signing key.
    pub fn new(
        authentication: BearerAuthenticator,
        metadata: Arc<ChunkKvMetadataStore>,
        tenant: TenantId,
        continuation_key: Vec<u8>,
    ) -> Result<Self, InspectionError> {
        Ok(Self {
            authentication,
            metadata,
            tenant,
            locations: LocationInspector::new(continuation_key)?,
        })
    }

    /// Installs the management surface only when explicit management credentials exist.
    /// # Errors
    /// Rejects incomplete or invalid configured bearer credentials.
    pub fn from_environment(
        metadata: Arc<ChunkKvMetadataStore>,
        tenant: TenantId,
        continuation_key: Vec<u8>,
    ) -> Result<Option<Self>, super::BoxError> {
        let manager = match std::env::var("CROWDB_ICEBERG_MANAGE_TOKEN") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let authentication = BearerAuthenticator::new(
            &std::env::var("CROWDB_ICEBERG_READ_TOKEN")?,
            &std::env::var("CROWDB_ICEBERG_WRITE_TOKEN")?,
            &manager,
            &std::env::var("CROWDB_ICEBERG_CLEAR_TOKEN")?,
        )?;
        Self::new(authentication, metadata, tenant, continuation_key)
            .map(Some)
            .map_err(Into::into)
    }

    pub(super) async fn handle(&self, request: Request<Incoming>) -> Response<ResponseBody> {
        let result = tokio::time::timeout(Duration::from_secs(5), self.read(&request)).await;
        match result {
            Ok(Ok(body)) => response(StatusCode::OK, body),
            Ok(Err((status, error))) => response(status, json!({ "error": error }).to_string().into_bytes()),
            Err(_) => response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "Object metadata inspection timed out"})
                    .to_string()
                    .into_bytes(),
            ),
        }
    }

    async fn read(&self, request: &Request<Incoming>) -> Result<Vec<u8>, Failure> {
        let mut headers = request.headers().get_all(hyper::header::AUTHORIZATION).iter();
        let authorization = match (headers.next(), headers.next()) {
            (Some(value), None) => value.to_str().unwrap_or_default(),
            _ => "",
        };
        let principal = self.authentication.authenticate(authorization).ok_or_else(|| {
            failure(
                StatusCode::UNAUTHORIZED,
                "Management bearer authentication is required",
            )
        })?;
        if principal.management != ManagementPrivilege::Manage {
            return Err(failure(StatusCode::FORBIDDEN, "Management privilege is required"));
        }
        if request.method() != Method::GET {
            return Err(failure(
                StatusCode::METHOD_NOT_ALLOWED,
                "Object inspection requires GET",
            ));
        }
        if request.uri().to_string().len() > 16 * 1024 {
            return Err(failure(
                StatusCode::BAD_REQUEST,
                "Inspection query exceeds the request limit",
            ));
        }
        let query = query(request.uri().query().unwrap_or_default())?;
        let name = query
            .get("bucket")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| failure(StatusCode::BAD_REQUEST, "Bucket is required"))?;
        let key = query
            .get("key")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| failure(StatusCode::BAD_REQUEST, "Object key is required"))?;
        if name.len() > 1024 || key.len() > 1024 {
            return Err(failure(
                StatusCode::BAD_REQUEST,
                "Bucket or object key exceeds metadata bounds",
            ));
        }
        let limit = query
            .get("limit")
            .map_or(Ok(20), |value| value.parse::<usize>())
            .map_err(|_| failure(StatusCode::BAD_REQUEST, "Invalid inspection limit"))?;
        if !(1..=100).contains(&limit) {
            return Err(inspection_failure(InspectionError::Limit));
        }
        let bucket = bucket::head_bucket(&self.metadata, &self.tenant, name.as_bytes())
            .await
            .map_err(|error| failure(StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?
            .ok_or_else(|| failure(StatusCode::NOT_FOUND, "Bucket not found"))?;
        let metadata_key = MetadataKey::object(&self.tenant, bucket, key.as_bytes())
            .map_err(|error| failure(StatusCode::BAD_REQUEST, error.to_string()))?;
        let value = self
            .metadata
            .get(metadata_key)
            .await
            .map_err(|error| failure(StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?
            .ok_or_else(|| failure(StatusCode::NOT_FOUND, "Object not found"))?;
        if value.value.len() > MAX_REFERENCE_BYTES + 32 * 1024 {
            return Err(inspection_failure(InspectionError::ReferenceLimit));
        }
        let record =
            ObjectRecord::decode(&value.value).map_err(|_| inspection_failure(InspectionError::Reference))?;
        if record.bucket_id != bucket || record.key != key.as_bytes() {
            return Err(inspection_failure(InspectionError::Reference));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let page = self
            .locations
            .page(
                name,
                &record,
                value.revision,
                limit,
                query.get("cursor").map(String::as_str),
                now,
            )
            .map_err(inspection_failure)?;
        serde_json::to_vec(&page).map_err(|_| {
            failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Inspection serialization failed",
            )
        })
    }
}

type Failure = (StatusCode, String);
fn failure(status: StatusCode, message: impl Into<String>) -> Failure {
    (status, message.into())
}
fn inspection_failure(error: InspectionError) -> Failure {
    let status = match error {
        InspectionError::Limit | InspectionError::Cursor => StatusCode::BAD_REQUEST,
        InspectionError::Stale => StatusCode::CONFLICT,
        InspectionError::ReferenceLimit | InspectionError::ResponseLimit => StatusCode::PAYLOAD_TOO_LARGE,
        InspectionError::Reference => StatusCode::UNPROCESSABLE_ENTITY,
    };
    failure(status, error.to_string())
}
fn query(text: &str) -> Result<BTreeMap<String, String>, Failure> {
    let mut result = BTreeMap::new();
    for pair in text.split('&').filter(|pair| !pair.is_empty()) {
        let (name, value) = pair
            .split_once('=')
            .ok_or_else(|| failure(StatusCode::BAD_REQUEST, "Invalid query"))?;
        let decode = |value: &str| {
            percent_decode_str(&value.replace('+', " "))
                .decode_utf8()
                .map(std::borrow::Cow::into_owned)
                .map_err(|_| failure(StatusCode::BAD_REQUEST, "Query is not UTF-8"))
        };
        let name = decode(name)?;
        let value = decode(value)?;
        if !["bucket", "key", "limit", "cursor"].contains(&name.as_str())
            || result.insert(name, value).is_some()
        {
            return Err(failure(
                StatusCode::BAD_REQUEST,
                "Unknown or duplicate inspection parameter",
            ));
        }
    }
    Ok(result)
}
fn response(status: StatusCode, body: Vec<u8>) -> Response<ResponseBody> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(hyper::header::CACHE_CONTROL, "no-store")
        .body(full_body(body.into()))
        .expect("static object inspection response headers")
}

pub(super) fn dispatch(
    inspector: Option<Arc<ObjectInspector>>,
    request: Request<Incoming>,
) -> super::HandlerFuture {
    Box::pin(async move {
        Ok(match inspector {
            Some(inspector) => inspector.handle(request).await,
            None => response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "Management object inspection is not configured"})
                    .to_string()
                    .into_bytes(),
            ),
        })
    })
}
