// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{
    ControlClient, CreateUpload, ExactMatrix, FieldPreview, InspectionPreparation, MatrixRequest, OpaqueId,
    ReadBinding, ReadPlan, SampleId, SnapshotSummary, UploadPart, UploadStatus,
};
use crate::{BatchStream, Error};
use bytes::Bytes;

impl ControlClient {
    /// # Errors
    /// Returns capability, malformed identity, missing Snapshot or service errors.
    pub async fn contains(&self, snapshot: &OpaqueId, sample: &SampleId) -> Result<bool, Error> {
        #[derive(serde::Deserialize)]
        struct Contains {
            contains: bool,
        }
        self.require("bounded-inspection").await?;
        let value: Contains = self
            .inner
            .http
            .json_query(
                "GET",
                &self.path(&["snapshots", snapshot.as_str(), "contains"]),
                &[("sample", &sample.base64())],
            )
            .await?;
        Ok(value.contains)
    }
    /// # Errors
    /// Returns capability, unpublished Snapshot or bounded inspection errors.
    pub async fn summary(&self, snapshot: &OpaqueId) -> Result<SnapshotSummary, Error> {
        self.call::<(), _>(
            "bounded-inspection",
            "GET",
            &["snapshots", snapshot.as_str(), "summary"],
            None,
        )
        .await
    }
    /// # Errors
    /// Returns unsupported legacy metadata, capability or service errors.
    pub async fn prepare_inspection(&self, snapshot: &OpaqueId) -> Result<InspectionPreparation, Error> {
        self.call::<(), _>(
            "inspection-preparation",
            "POST",
            &["snapshots", snapshot.as_str(), "inspection"],
            None,
        )
        .await
    }
    /// # Errors
    /// Returns capability, missing preparation or service errors.
    pub async fn inspection_status(&self, snapshot: &OpaqueId) -> Result<InspectionPreparation, Error> {
        self.call::<(), _>(
            "bounded-inspection",
            "GET",
            &["snapshots", snapshot.as_str(), "inspection"],
            None,
        )
        .await
    }
    /// # Errors
    /// Rejects invalid bounds or returns capability, scope or service errors.
    pub async fn matrix(&self, snapshot: &OpaqueId, request: &MatrixRequest) -> Result<ExactMatrix, Error> {
        if request.sample_ids.is_empty()
            || request.sample_ids.len() > 20
            || request.fields.is_empty()
            || request.fields.len() > 12
        {
            return Err(Error::Limit);
        }
        self.call(
            "bounded-inspection",
            "POST",
            &["snapshots", snapshot.as_str(), "matrix"],
            Some(request),
        )
        .await
    }
    /// Preview does not create a read plan or acknowledge consumption.
    /// # Errors
    /// Rejects bounds, unavailable payload readers or service failures.
    pub async fn preview(
        &self,
        snapshot: &OpaqueId,
        sample: &SampleId,
        field: &str,
        max_bytes: u32,
    ) -> Result<FieldPreview, Error> {
        if !(1..=65536).contains(&max_bytes) {
            return Err(Error::Limit);
        }
        self.require("bounded-inspection").await?;
        self.inner
            .http
            .json_query(
                "GET",
                &self.path(&["snapshots", snapshot.as_str(), "preview"]),
                &[
                    ("sample", &sample.base64()),
                    ("field", field),
                    ("max_bytes", &max_bytes.to_string()),
                ],
            )
            .await
    }
    /// # Errors
    /// Returns capability, input, upload conflict or service errors.
    pub async fn create_upload(&self, request: &CreateUpload) -> Result<UploadStatus, Error> {
        self.call("uploads", "POST", &["uploads"], Some(request))
            .await
            .map_err(|error| error.with_identity(request.id.as_str()))
    }
    /// # Errors
    /// Returns capability, scope, missing upload or service errors.
    pub async fn upload_status(&self, id: &OpaqueId) -> Result<UploadStatus, Error> {
        self.call::<(), _>("uploads", "GET", &["uploads", id.as_str()], None)
            .await
    }
    /// # Errors
    /// Returns capability, ownership conflict or service errors.
    pub async fn abort_upload(&self, id: &OpaqueId) -> Result<UploadStatus, Error> {
        self.call::<(), _>("uploads", "DELETE", &["uploads", id.as_str()], None)
            .await
    }
    /// # Errors
    /// Returns capability, incomplete parts, integrity or service errors.
    pub async fn complete_upload(&self, id: &OpaqueId) -> Result<UploadStatus, Error> {
        self.require("uploads").await?;
        self.inner
            .http
            .json_long_idempotent::<(), _>("POST", &self.path(&["uploads", id.as_str(), "complete"]), None)
            .await
            .map_err(|error| error.with_identity(id.as_str()))
    }
    /// # Errors
    /// Returns capability, part bounds, integrity or service errors.
    pub async fn upload_part(&self, id: &OpaqueId, index: u32, data: Bytes) -> Result<UploadPart, Error> {
        if data.is_empty() || data.len() > 4 * 1024 * 1024 {
            return Err(Error::Limit);
        }
        self.require("uploads").await?;
        let bytes = self
            .inner
            .http
            .request_idempotent(
                "PUT",
                &self.path(&["uploads", id.as_str(), "parts", &index.to_string()]),
                data,
                "application/octet-stream",
            )
            .await
            .map_err(|error| error.with_identity(id.as_str()))?;
        Ok(serde_json::from_slice(&bytes)?)
    }
    /// Pins immutable versions into a client-owned continuation, without read state.
    /// # Errors
    /// Returns capability, query, admission or service failures.
    pub async fn bind_read(&self, request: &ReadPlan) -> Result<ReadBinding, Error> {
        self.call("stateless-reads", "POST", &["reads", "bind"], Some(request))
            .await
    }
    /// Dropping cancels only this HTTP stream. Retrying a cursor replays its batch.
    /// # Errors
    /// Returns invalid continuations, deletion admission, bounded work or service failures.
    pub async fn next_batch(&self, cursor: &str) -> Result<BatchStream, Error> {
        self.require("stateless-reads").await?;
        Ok(BatchStream::new_v2(
            self.inner
                .http
                .stream(
                    "POST",
                    &self.path(&["reads", "next"]),
                    Some(&serde_json::json!({"cursor":cursor})),
                )
                .await?,
        ))
    }
}
