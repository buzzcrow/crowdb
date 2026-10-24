// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Compatibility routes for the original bincode client.

use super::{
    cancelled_status, decode_snapshot_request, encode, response, status_for_authority, DatasetHttpService,
    MAX_REQUEST_BYTES,
};
use crowdb_access_dataset::{
    AuthorityError, DatasetManifestResponse, DatasetPublishRequest, DatasetPublishResponse,
    DatasetReadRequest, DatasetReclaimResponse, DatasetRetentionResponse, DatasetScanRequest,
    DatasetSnapshotRequest, DatasetSnapshotResponse, DatasetSnapshotsResponse, OperationId,
    ReadTransportError,
};
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    Method, Request, Response, StatusCode,
};

impl DatasetHttpService {
    pub(super) async fn handle_legacy(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        if !self.authorized(&request) {
            return response(StatusCode::UNAUTHORIZED, b"unauthorized".to_vec());
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/read" {
            return self.read(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/scan" {
            return self.scan(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/create" {
            return self.create().await;
        }
        if request.method() == Method::GET && request.uri().path() == "/v1/dataset/open" {
            return self.open().await;
        }
        if request.method() == Method::GET && request.uri().path() == "/v1/dataset/latest" {
            return self.latest().await;
        }
        if request.method() == Method::GET && request.uri().path() == "/v1/dataset/snapshots" {
            return self.snapshots().await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/publish" {
            return self.publish(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/prepare" {
            return self.publish(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/retain" {
            return self.retain(request, true).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/release" {
            return self.retain(request, false).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/snapshot" {
            return self.snapshot(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/manifest" {
            return self.manifest(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/stable" {
            return self.stable(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/reclaim" {
            return self.reclaim(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/reclaim/status" {
            return self.reclaim_status(request).await;
        }
        if request.method() == Method::POST && request.uri().path() == "/v1/dataset/cancel" {
            self.reads.cancel();
            return response(StatusCode::NO_CONTENT, Vec::new());
        }
        response(StatusCode::NOT_FOUND, b"not found".to_vec())
    }

    async fn open(&self) -> Response<Full<Bytes>> {
        match self.reads.authority().get_dataset(self.reads.identity()).await {
            Ok(record) => encode(StatusCode::OK, &record),
            Err(_) => response(StatusCode::NOT_FOUND, b"dataset not found".to_vec()),
        }
    }

    async fn create(&self) -> Response<Full<Bytes>> {
        match self
            .reads
            .authority()
            .create_dataset(self.reads.identity().clone())
            .await
        {
            Ok(id) => encode(StatusCode::OK, &id),
            Err(crowdb_access_dataset::AuthorityError::AlreadyExists) => {
                response(StatusCode::CONFLICT, b"dataset already exists".to_vec())
            }
            Err(_) => response(StatusCode::SERVICE_UNAVAILABLE, b"dataset unavailable".to_vec()),
        }
    }

    async fn latest(&self) -> Response<Full<Bytes>> {
        match self.reads.authority().latest(self.reads.identity()).await {
            Ok(snapshot) => encode(StatusCode::OK, &snapshot),
            Err(_) => response(StatusCode::SERVICE_UNAVAILABLE, b"dataset unavailable".to_vec()),
        }
    }

    async fn snapshots(&self) -> Response<Full<Bytes>> {
        match self.reads.authority().list_snapshots(self.reads.identity()).await {
            Ok(snapshots) => encode(StatusCode::OK, &DatasetSnapshotsResponse { snapshots }),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn publish(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let body = match tokio::time::timeout(self.request_timeout, request.into_body().collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return response(StatusCode::REQUEST_TIMEOUT, b"request timeout".to_vec()),
        };
        if body.len() > MAX_REQUEST_BYTES {
            return response(StatusCode::PAYLOAD_TOO_LARGE, b"request too large".to_vec());
        }
        let request: DatasetPublishRequest = match bincode::deserialize(&body) {
            Ok(request) => request,
            Err(_) => return response(StatusCode::BAD_REQUEST, b"invalid publication request".to_vec()),
        };
        if request.validate().is_err() {
            return response(StatusCode::BAD_REQUEST, b"invalid publication request".to_vec());
        }
        let operation = request.operation.unwrap_or_else(OperationId::random);
        match self
            .reads
            .authority()
            .publish_snapshot_with_operation(
                self.reads.identity(),
                request.parent,
                request.manifest,
                operation,
            )
            .await
        {
            Ok(snapshot) => encode(StatusCode::OK, &DatasetPublishResponse { snapshot }),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn retain(&self, request: Request<Incoming>, retain: bool) -> Response<Full<Bytes>> {
        let body = match tokio::time::timeout(self.request_timeout, request.into_body().collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return response(StatusCode::REQUEST_TIMEOUT, b"request timeout".to_vec()),
        };
        let request: DatasetSnapshotRequest = match bincode::deserialize(&body) {
            Ok(request) => request,
            Err(_) => return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec()),
        };
        let result = if retain {
            self.reads
                .authority()
                .retain_snapshot(self.reads.identity(), request.snapshot)
                .await
        } else {
            self.reads
                .authority()
                .release_snapshot(self.reads.identity(), request.snapshot)
                .await
        };
        match result {
            Ok(()) => encode(
                StatusCode::OK,
                &DatasetRetentionResponse {
                    snapshot: request.snapshot,
                    retained: retain,
                },
            ),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn snapshot(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .get_snapshot(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(snapshot) => encode(StatusCode::OK, &DatasetSnapshotResponse { snapshot }),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn manifest(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .read_manifest(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(manifest) => encode(StatusCode::OK, &DatasetManifestResponse { manifest }),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn stable(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .set_stable(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(()) => encode(
                StatusCode::OK,
                &DatasetRetentionResponse {
                    snapshot: request.snapshot,
                    retained: true,
                },
            ),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn reclaim(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .reclaim_snapshot_metadata(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(()) => encode(
                StatusCode::OK,
                &DatasetReclaimResponse {
                    snapshot: request.snapshot,
                    reclaimed: true,
                },
            ),
            Err(AuthorityError::ReclaimPending { .. }) => encode(
                StatusCode::OK,
                &DatasetReclaimResponse {
                    snapshot: request.snapshot,
                    reclaimed: false,
                },
            ),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    async fn reclaim_status(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Some(request) = decode_snapshot_request(request, self.request_timeout).await else {
            return response(StatusCode::BAD_REQUEST, b"invalid snapshot request".to_vec());
        };
        match self
            .reads
            .authority()
            .reclaim_progress(self.reads.identity(), request.snapshot)
            .await
        {
            Ok(progress) => encode(StatusCode::OK, &progress),
            Err(error) => response(status_for_authority(&error), error.to_string().into_bytes()),
        }
    }

    pub(super) fn authorized(&self, request: &Request<Incoming>) -> bool {
        let Some(expected) = &self.bearer else { return true };
        request
            .headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|value| value.as_bytes() == expected.as_bytes())
    }

    async fn read(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let body = match tokio::time::timeout(self.request_timeout, request.into_body().collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return response(StatusCode::REQUEST_TIMEOUT, b"request timeout".to_vec()),
        };
        if body.len() > MAX_REQUEST_BYTES {
            return response(StatusCode::PAYLOAD_TOO_LARGE, b"request too large".to_vec());
        }
        let request: DatasetReadRequest = match bincode::deserialize(&body) {
            Ok(request) => request,
            Err(_) => return response(StatusCode::BAD_REQUEST, b"invalid dataset request".to_vec()),
        };
        match self.reads.read(request).await {
            Ok(result) => encode(StatusCode::OK, &result),
            Err(ReadTransportError::WindowFull) => {
                response(StatusCode::TOO_MANY_REQUESTS, b"read window full".to_vec())
            }
            Err(ReadTransportError::Invalid(crowdb_access_dataset::DatasetError::ReadCancelled)) => {
                response(cancelled_status(), b"dataset read cancelled".to_vec())
            }
            Err(ReadTransportError::Invalid(_)) => {
                response(StatusCode::BAD_REQUEST, b"invalid dataset request".to_vec())
            }
            Err(ReadTransportError::Authority(error)) => {
                response(status_for_authority(&error), error.to_string().into_bytes())
            }
        }
    }

    async fn scan(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let body = match tokio::time::timeout(self.request_timeout, request.into_body().collect()).await {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return response(StatusCode::REQUEST_TIMEOUT, b"request timeout".to_vec()),
        };
        if body.len() > MAX_REQUEST_BYTES {
            return response(StatusCode::PAYLOAD_TOO_LARGE, b"request too large".to_vec());
        }
        let request: DatasetScanRequest = match bincode::deserialize(&body) {
            Ok(request) => request,
            Err(_) => return response(StatusCode::BAD_REQUEST, b"invalid scan request".to_vec()),
        };
        match self.reads.scan(request).await {
            Ok(result) => encode(StatusCode::OK, &result),
            Err(ReadTransportError::WindowFull) => {
                response(StatusCode::TOO_MANY_REQUESTS, b"read window full".to_vec())
            }
            Err(ReadTransportError::Invalid(crowdb_access_dataset::DatasetError::ReadCancelled)) => {
                response(cancelled_status(), b"dataset read cancelled".to_vec())
            }
            Err(ReadTransportError::Invalid(_)) => {
                response(StatusCode::BAD_REQUEST, b"invalid scan request".to_vec())
            }
            Err(ReadTransportError::Authority(error)) => {
                response(status_for_authority(&error), error.to_string().into_bytes())
            }
        }
    }
}
