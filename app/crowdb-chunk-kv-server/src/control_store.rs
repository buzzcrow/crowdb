// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Revision-checked group-0 persistence for chunk KV control records.

use std::sync::Arc;

use async_trait::async_trait;
use crowdb_kv_client::{CrowdbKvClient, Error as KvClientError, GetOutcome, ReadMode};
use crowdb_protocol::chunk_kv::{
    CatalogHead, CatalogPage, DomainMonitorDescriptor, EnsureDomainMonitorOutcome, ServingGrant,
};
use crowdb_protocol::key::{
    ChunkKvCatalogHeadKey, ChunkKvCatalogPageKey, DomainMonitorKey, ServingGrantKey, TextKey,
};
use thiserror::Error;

use crate::{CatalogError, CatalogStore, HeadWriteOutcome, MonitorDescriptorStore, MonitorError};

const GROUP0_STORE: u64 = 0;
const GROUP0_GROUP: u64 = 0;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub revision: u64,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum Group0KvError {
    #[error("group-0 compare-and-set failed at revision {current_revision}")]
    CasFailed { current_revision: u64 },
    #[error("group-0 write outcome is unknown")]
    OutcomeUnknown,
    #[error("group-0 operation failed: {0}")]
    Unavailable(String),
}

#[async_trait]
pub trait Group0Kv: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>, Group0KvError>;
    async fn put_cas(&self, key: &[u8], value: &[u8], expected_revision: u64) -> Result<(), Group0KvError>;
}

#[async_trait]
impl Group0Kv for CrowdbKvClient {
    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>, Group0KvError> {
        match self
            .get(GROUP0_STORE, GROUP0_GROUP, key, ReadMode::Linearizable, None)
            .await
            .map_err(map_client_error)?
        {
            GetOutcome::Found { value, revision } => Ok(Some(VersionedValue {
                value: value.to_vec(),
                revision,
            })),
            GetOutcome::NotFound => Ok(None),
        }
    }

    async fn put_cas(&self, key: &[u8], value: &[u8], expected_revision: u64) -> Result<(), Group0KvError> {
        CrowdbKvClient::put_cas(self, GROUP0_STORE, GROUP0_GROUP, key, value, expected_revision)
            .await
            .map(|_| ())
            .map_err(map_client_error)
    }
}

fn map_client_error(error: KvClientError) -> Group0KvError {
    match error {
        KvClientError::CasFailed { current_revision } => Group0KvError::CasFailed { current_revision },
        KvClientError::OutcomeUnknown => Group0KvError::OutcomeUnknown,
        other => Group0KvError::Unavailable(other.to_string()),
    }
}

pub struct Group0ControlStore {
    kv: Arc<dyn Group0Kv>,
}

impl Group0ControlStore {
    #[must_use]
    pub fn new(kv: Arc<dyn Group0Kv>) -> Self {
        Self { kv }
    }

    #[must_use]
    pub fn from_client(kv: Arc<CrowdbKvClient>) -> Self {
        Self { kv }
    }

    /// Loads and validates the latest serving grant for one instance.
    ///
    /// # Errors
    ///
    /// Returns an unavailable error for transport, decoding, or invalid grant
    /// data. Absence is returned separately so callers remain fenced.
    pub async fn load_serving_grant(&self, instance_id: u64) -> Result<Option<ServingGrant>, Group0KvError> {
        let path = ServingGrantKey { instance_id }.to_path();
        let grant = self
            .read::<ServingGrant>(&path)
            .await
            .map_err(Group0KvError::Unavailable)?
            .map(|(grant, _)| grant);
        if grant.as_ref().is_some_and(|grant| grant.validate().is_err()) {
            return Err(Group0KvError::Unavailable(format!(
                "invalid serving grant {path}"
            )));
        }
        Ok(grant)
    }

    async fn read<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<Option<(T, u64)>, String> {
        let stored = self
            .kv
            .get(path.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        stored
            .map(|stored| {
                serde_json::from_slice(&stored.value)
                    .map(|value| (value, stored.revision))
                    .map_err(|error| format!("invalid control record {path}: {error}"))
            })
            .transpose()
    }

    async fn reconcile_immutable<T>(&self, path: &str, intended: &T) -> Result<(), CatalogError>
    where
        T: serde::de::DeserializeOwned + PartialEq,
    {
        match self.read::<T>(path).await.map_err(CatalogError::Unavailable)? {
            Some((current, _)) if current == *intended => Ok(()),
            Some(_) => Err(CatalogError::GenerationConflict),
            None => Err(CatalogError::Unavailable(
                "immutable page write was not visible after reconciliation".into(),
            )),
        }
    }
}

#[async_trait]
impl CatalogStore for Group0ControlStore {
    async fn put_page(&self, page: CatalogPage) -> Result<(), CatalogError> {
        page.validate()?;
        let path = ChunkKvCatalogPageKey {
            generation: page.generation,
            page_index: page.page_index,
        }
        .to_path();
        if let Some((current, _)) = self
            .read::<CatalogPage>(&path)
            .await
            .map_err(CatalogError::Unavailable)?
        {
            return if current == page {
                Ok(())
            } else {
                Err(CatalogError::GenerationConflict)
            };
        }
        let encoded =
            serde_json::to_vec(&page).map_err(|error| CatalogError::Unavailable(error.to_string()))?;
        match self.kv.put_cas(path.as_bytes(), &encoded, 0).await {
            Ok(()) => Ok(()),
            Err(Group0KvError::CasFailed { .. } | Group0KvError::OutcomeUnknown) => {
                self.reconcile_immutable(&path, &page).await
            }
            Err(error) => Err(CatalogError::Unavailable(error.to_string())),
        }
    }

    async fn get_page(&self, generation: u64, page_index: u64) -> Result<Option<CatalogPage>, CatalogError> {
        let path = ChunkKvCatalogPageKey {
            generation,
            page_index,
        }
        .to_path();
        self.read(&path)
            .await
            .map(|value| value.map(|(page, _)| page))
            .map_err(CatalogError::Unavailable)
    }

    async fn put_head(&self, head: CatalogHead) -> Result<HeadWriteOutcome, CatalogError> {
        let path = ChunkKvCatalogHeadKey.to_path();
        let current = self
            .read::<CatalogHead>(&path)
            .await
            .map_err(CatalogError::Unavailable)?;
        if current.as_ref().is_some_and(|(stored, _)| stored == &head) {
            return Ok(HeadWriteOutcome::Committed);
        }
        let expected_revision = current.map_or(0, |(_, revision)| revision);
        let encoded =
            serde_json::to_vec(&head).map_err(|error| CatalogError::Unavailable(error.to_string()))?;
        match self
            .kv
            .put_cas(path.as_bytes(), &encoded, expected_revision)
            .await
        {
            Ok(()) => Ok(HeadWriteOutcome::Committed),
            Err(Group0KvError::CasFailed { .. }) => {
                let committed = self
                    .read::<CatalogHead>(&path)
                    .await
                    .map_err(CatalogError::Unavailable)?
                    .is_some_and(|(stored, _)| stored == head);
                Ok(if committed {
                    HeadWriteOutcome::Committed
                } else {
                    HeadWriteOutcome::DefinitelyNotCommitted
                })
            }
            Err(Group0KvError::OutcomeUnknown) => Ok(HeadWriteOutcome::Ambiguous),
            Err(error) => Err(CatalogError::Unavailable(error.to_string())),
        }
    }

    async fn get_head(&self) -> Result<Option<CatalogHead>, CatalogError> {
        let path = ChunkKvCatalogHeadKey.to_path();
        self.read(&path)
            .await
            .map(|value| value.map(|(head, _)| head))
            .map_err(CatalogError::Unavailable)
    }
}

#[async_trait]
impl MonitorDescriptorStore for Group0ControlStore {
    async fn ensure(
        &self,
        descriptor: &DomainMonitorDescriptor,
    ) -> Result<EnsureDomainMonitorOutcome, MonitorError> {
        let path = DomainMonitorKey {
            domain: descriptor.domain.clone(),
        }
        .to_path();
        match self
            .read::<DomainMonitorDescriptor>(&path)
            .await
            .map_err(MonitorError::Store)?
        {
            Some((current, _)) if current == *descriptor => {
                return Ok(EnsureDomainMonitorOutcome::AlreadyExists);
            }
            Some(_) => return Ok(EnsureDomainMonitorOutcome::DescriptorConflict),
            None => {}
        }
        let encoded =
            serde_json::to_vec(descriptor).map_err(|error| MonitorError::Store(error.to_string()))?;
        match self.kv.put_cas(path.as_bytes(), &encoded, 0).await {
            Ok(()) => Ok(EnsureDomainMonitorOutcome::Created),
            Err(Group0KvError::CasFailed { .. } | Group0KvError::OutcomeUnknown) => {
                match self
                    .read::<DomainMonitorDescriptor>(&path)
                    .await
                    .map_err(MonitorError::Store)?
                {
                    Some((current, _)) if current == *descriptor => {
                        Ok(EnsureDomainMonitorOutcome::AlreadyExists)
                    }
                    Some(_) => Ok(EnsureDomainMonitorOutcome::DescriptorConflict),
                    None => Err(MonitorError::Store(
                        "monitor descriptor write was not visible after reconciliation".into(),
                    )),
                }
            }
            Err(error) => Err(MonitorError::Store(error.to_string())),
        }
    }
}
