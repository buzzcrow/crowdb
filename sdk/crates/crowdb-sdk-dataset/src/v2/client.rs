// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    required_option, CommitReceipt, Head, OpaqueId, RetainedSnapshot, SnapshotLifecycle, SnapshotReclaim,
};
use crate::{DatasetClient, DatasetInfo, Error};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Versioned control client sharing the original HTTP pool and resource limits.
/// Capabilities are checked before requests; mutations never downgrade to v1.
#[derive(Clone)]
pub struct ControlClient {
    pub(crate) inner: DatasetClient,
}
impl DatasetClient {
    #[must_use]
    pub fn control(&self) -> ControlClient {
        ControlClient { inner: self.clone() }
    }
}
impl ControlClient {
    pub(crate) fn path<'a>(&'a self, suffix: &[&'a str]) -> Vec<&'a str> {
        let mut path = self.inner.path(suffix);
        path[2] = "v2";
        path
    }
    pub(crate) async fn require(&self, capability: &str) -> Result<(), Error> {
        self.inner.capabilities().await?.require(&[capability])
    }
    pub(crate) async fn call<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        capability: &str,
        method: &str,
        suffix: &[&str],
        body: Option<&T>,
    ) -> Result<R, Error> {
        self.require(capability).await?;
        self.inner
            .http
            .json_idempotent(method, &self.path(suffix), body)
            .await
    }
    /// # Errors
    /// Returns capability, admission, transport or service errors.
    pub async fn head(&self) -> Result<Head, Error> {
        self.call::<(), _>("head-inspection", "GET", &["head"], None)
            .await
    }
    /// Dataset creation is not replayed automatically; a duplicate name conflicts.
    /// # Errors
    /// Returns capability, duplicate-name, admission, transport or service errors.
    pub async fn create(&self) -> Result<DatasetInfo, Error> {
        self.require("dataset-management").await?;
        self.inner.http.json::<(), _>("PUT", &self.path(&[]), None).await
    }
    /// # Errors
    /// Returns capability, admission, transport or service errors.
    pub async fn open(&self) -> Result<DatasetInfo, Error> {
        self.call::<(), _>("dataset-management", "GET", &[], None).await
    }
    /// # Errors
    /// Returns capability, unpublished target, transport or service errors.
    pub async fn set_stable(&self, snapshot: &OpaqueId) -> Result<DatasetInfo, Error> {
        self.call(
            "dataset-management",
            "PUT",
            &["names", "stable"],
            Some(&serde_json::json!({"snapshot":snapshot})),
        )
        .await
    }
    /// # Errors
    /// Returns capability, missing Snapshot, transport or service errors.
    pub async fn retain(&self, snapshot: &OpaqueId) -> Result<RetainedSnapshot, Error> {
        self.call::<(), _>(
            "dataset-management",
            "POST",
            &["snapshots", snapshot.as_str(), "retain"],
            None,
        )
        .await
    }
    /// # Errors
    /// Returns capability, missing Snapshot, transport or service errors.
    pub async fn release(&self, snapshot: &OpaqueId) -> Result<RetainedSnapshot, Error> {
        self.call::<(), _>(
            "dataset-management",
            "POST",
            &["snapshots", snapshot.as_str(), "release"],
            None,
        )
        .await
    }
    /// One bounded retirement step. Physical deletion may remain deferred.
    /// # Errors
    /// Returns capability, protected Snapshot, transport or service errors.
    pub async fn reclaim(&self, snapshot: &OpaqueId) -> Result<SnapshotReclaim, Error> {
        self.call::<(), _>(
            "snapshot-reclaim",
            "POST",
            &["snapshots", snapshot.as_str(), "reclaim"],
            None,
        )
        .await
    }
    /// Observes retirement and protection without advancing reclamation.
    /// # Errors
    /// Returns capability, missing Snapshot, transport or service errors.
    pub async fn lifecycle(&self, snapshot: &OpaqueId) -> Result<SnapshotLifecycle, Error> {
        self.call::<(), _>(
            "bounded-inspection",
            "GET",
            &["snapshots", snapshot.as_str(), "reclaim"],
            None,
        )
        .await
    }
    /// Resolves an unknown publication outcome without issuing a new attempt.
    /// # Errors
    /// Returns capability, transport, invalid response or service errors.
    pub async fn commit_status(&self, attempt: &OpaqueId) -> Result<Option<CommitReceipt>, Error> {
        #[derive(Deserialize)]
        struct Status {
            #[serde(deserialize_with = "required_option")]
            receipt: Option<CommitReceipt>,
        }
        let status: Status = self
            .call::<(), _>("dataset-management", "GET", &["commits", attempt.as_str()], None)
            .await?;
        Ok(status.receipt)
    }
}
