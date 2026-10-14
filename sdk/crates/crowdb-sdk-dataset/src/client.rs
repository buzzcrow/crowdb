// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    BatchRequest, DatasetInfo, Error, Manifest, Publication, Snapshot, SnapshotRequest, Upload, UploadRequest,
};
use crowdb_sdk_core::{HttpClient, HttpConfig};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Clone)]
pub struct DatasetClient {
    pub(crate) http: HttpClient,
    namespace: String,
    name: String,
}

impl DatasetClient {
    /// # Errors
    /// Rejects invalid HTTP configuration and empty Dataset names.
    pub fn new(endpoint: &str, name: &str, bearer: Option<String>) -> Result<Self, Error> {
        Self::with_config(endpoint, name, bearer, HttpConfig::default())
    }

    /// # Errors
    /// Rejects invalid names, endpoint, credentials or resource budgets.
    pub fn with_config(
        endpoint: &str,
        name: &str,
        bearer: Option<String>,
        config: HttpConfig,
    ) -> Result<Self, Error> {
        if name.is_empty()
            || name.len() > 255
            || matches!(name, "." | "..")
            || name.bytes().any(|byte| byte == 0 || byte == 0x1f)
        {
            return Err(Error::Invalid("Dataset name".into()));
        }
        Ok(Self {
            http: HttpClient::with_config(endpoint, bearer, config)?,
            namespace: "dataset-ns".into(),
            name: name.into(),
        })
    }

    pub(crate) fn path<'a>(&'a self, suffix: &[&'a str]) -> Vec<&'a str> {
        let mut path = vec![
            "api",
            "dataset",
            "v1",
            "namespaces",
            &self.namespace,
            "datasets",
            &self.name,
        ];
        path.extend_from_slice(suffix);
        path
    }

    pub(crate) async fn call<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        method: &str,
        path: &[&str],
        value: Option<&T>,
    ) -> Result<R, Error> {
        self.http.json(method, &self.path(path), value).await
    }

    /// # Errors
    /// Returns a duplicate Dataset conflict or service/transport error.
    pub async fn create(&self) -> Result<DatasetInfo, Error> {
        self.call("PUT", &[], Some(&())).await
    }
    /// # Errors
    /// Returns not-found or service/transport errors.
    pub async fn open(&self) -> Result<DatasetInfo, Error> {
        self.call::<(), _>("GET", &[], None).await
    }
    /// # Errors
    /// Returns invalid input, conflict or service/transport errors.
    pub async fn prepare(&self, request: &Publication) -> Result<Snapshot, Error> {
        self.call("POST", &["snapshots", "prepare"], Some(request)).await
    }
    /// # Errors
    /// Returns invalid input, conflict or an ambiguous transport outcome.
    pub async fn publish(&self, request: &Publication) -> Result<Snapshot, Error> {
        self.call("POST", &["snapshots", "publish"], Some(request)).await
    }
    /// # Errors
    /// Returns not-found or service/transport errors.
    pub async fn operation(&self, token: &str) -> Result<Snapshot, Error> {
        self.call::<(), _>("GET", &["operations", token], None).await
    }
    /// # Errors
    /// Returns unpublished/not-found, service or transport errors.
    pub async fn snapshot(&self, id: &str) -> Result<Snapshot, Error> {
        self.call::<(), _>("GET", &["snapshots", id], None).await
    }
    /// # Errors
    /// Returns not-found, metadata-limit, service or transport errors.
    pub async fn manifest(&self, id: &str) -> Result<Manifest, Error> {
        self.call::<(), _>("GET", &["snapshots", id, "manifest"], None)
            .await
    }
    /// # Errors
    /// Returns not-found, service or transport errors.
    pub async fn retain(&self, id: &str) -> Result<Snapshot, Error> {
        self.call("POST", &["snapshots", id, "retain"], Some(&())).await
    }
    /// # Errors
    /// Returns not-found, service or transport errors.
    pub async fn release(&self, id: &str) -> Result<Snapshot, Error> {
        self.call("POST", &["snapshots", id, "release"], Some(&())).await
    }
    /// # Errors
    /// Rejects an unpublished Snapshot or unavailable service.
    pub async fn set_stable(&self, id: &str) -> Result<DatasetInfo, Error> {
        self.call(
            "PUT",
            &["names", "stable"],
            Some(&SnapshotRequest { snapshot: id.into() }),
        )
        .await
    }
    /// # Errors
    /// Returns invalid input, conflict or service/transport errors.
    pub async fn create_upload(&self, request: &UploadRequest) -> Result<Upload, Error> {
        self.call("POST", &["uploads"], Some(request)).await
    }
    /// # Errors
    /// Returns not-found or service/transport errors.
    pub async fn upload_status(&self, id: &str) -> Result<Upload, Error> {
        self.call::<(), _>("GET", &["uploads", id], None).await
    }
    /// # Errors
    /// Returns incomplete, integrity or service/transport errors.
    pub async fn complete_upload(&self, id: &str) -> Result<Upload, Error> {
        self.call("POST", &["uploads", id, "complete"], Some(&())).await
    }
    /// # Errors
    /// Returns ownership conflict or service/transport errors.
    pub async fn abort_upload(&self, id: &str) -> Result<Upload, Error> {
        self.call("DELETE", &["uploads", id], Some(&())).await
    }
    /// Returns a verified batch bounded by the client's response limit.
    /// # Errors
    /// Returns invalid selection, not-found or service/transport errors.
    pub async fn get_batch(&self, request: &BatchRequest) -> Result<crate::wire::VerifiedBatch, Error> {
        let bytes = self
            .http
            .json_request("POST", &self.path(&["reads"]), Some(request))
            .await?;
        crate::wire::decode_batch(bytes)
    }
}
