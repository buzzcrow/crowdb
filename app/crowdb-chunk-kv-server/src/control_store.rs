// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Revision-checked group-0 persistence for chunk KV control records.

use std::sync::Arc;

use async_trait::async_trait;
use crowdb_kv_client::{CrowdbKvClient, Error as KvClientError, GetOutcome, ReadMode};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, DomainMonitorDescriptor, EnsureDomainMonitorOutcome,
    ServingGrant, SplitTransition, TransferTransition,
};
use crowdb_protocol::key::{
    ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, ChunkKvSplitKey, ChunkKvTransferKey,
    DomainMonitorKey, ServingGrantKey, TextKey,
};
use thiserror::Error;

use crate::{
    ChunkKvRangeCatalogError, ChunkKvRangeCatalogStore, HeadWriteOutcome, MonitorDescriptorStore,
    MonitorError,
};

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
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, Group0KvError>;
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

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, VersionedValue)>, Group0KvError> {
        let mut values = Vec::new();
        let mut start_after = Vec::new();
        let mut cutoff = None;
        loop {
            let page = match cutoff {
                Some(cutoff) => {
                    self.scan_bounded_at(
                        GROUP0_STORE,
                        GROUP0_GROUP,
                        prefix,
                        &start_after,
                        &[],
                        0,
                        false,
                        None,
                        cutoff,
                    )
                    .await
                }
                None => {
                    self.scan_bounded(
                        GROUP0_STORE,
                        GROUP0_GROUP,
                        prefix,
                        &start_after,
                        &[],
                        0,
                        false,
                        None,
                    )
                    .await
                }
            }
            .map_err(map_client_error)?;
            if page.items.len() != page.commit_slots.len() {
                return Err(Group0KvError::Unavailable(
                    "group-0 scan returned mismatched item revisions".into(),
                ));
            }
            cutoff = Some(page.scan_cutoff);
            for ((key, value), revision) in page.items.iter().zip(page.commit_slots) {
                values.push((
                    key.to_vec(),
                    VersionedValue {
                        value: value.to_vec(),
                        revision,
                    },
                ));
            }
            if !page.truncated || page.items.is_empty() {
                break;
            }
            start_after = page.items.last().expect("nonempty page").0.to_vec();
        }
        Ok(values)
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

    /// Loads and validates one durable ownership-transfer transition.
    ///
    /// # Errors
    ///
    /// Returns a typed control-plane error for unavailable, malformed, or
    /// internally inconsistent persisted state.
    pub async fn load_transfer_transition(
        &self,
        transition_id: crowdb_protocol::chunk_kv::Id128,
    ) -> Result<Option<(TransferTransition, u64)>, MonitorError> {
        let path = ChunkKvTransferKey { transition_id }.to_path();
        let stored = self
            .read::<TransferTransition>(&path)
            .await
            .map_err(MonitorError::Store)?;
        if let Some((transition, revision)) = stored {
            transition
                .validate()
                .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
            if transition.transition_id != transition_id {
                return Err(MonitorError::PlanFailed(
                    "transfer key and record identity differ".into(),
                ));
            }
            return Ok(Some((transition, revision)));
        }
        Ok(None)
    }

    /// Lists one fixed-snapshot view of every durable transfer transition.
    ///
    /// # Errors
    ///
    /// Returns an error for scan failure, malformed keys or values, or an
    /// identity mismatch. Callers must abandon the whole observation on error.
    pub async fn list_transfer_transitions(&self) -> Result<Vec<(TransferTransition, u64)>, MonitorError> {
        let prefix = <ChunkKvTransferKey as TextKey>::prefix_all();
        let records = self
            .kv
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|error| MonitorError::Store(error.to_string()))?;
        records
            .into_iter()
            .map(|(key, stored)| {
                let path =
                    std::str::from_utf8(&key).map_err(|error| MonitorError::Store(error.to_string()))?;
                let typed = ChunkKvTransferKey::from_path(path)
                    .map_err(|error| MonitorError::Store(error.to_string()))?;
                let transition: TransferTransition = serde_json::from_slice(&stored.value)
                    .map_err(|error| MonitorError::Store(error.to_string()))?;
                transition
                    .validate()
                    .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
                if typed.transition_id != transition.transition_id {
                    return Err(MonitorError::PlanFailed(
                        "transfer key and record identity differ".into(),
                    ));
                }
                Ok((transition, stored.revision))
            })
            .collect()
    }

    /// Persists exactly one validated transfer state with revision fencing.
    /// Exact repeats are idempotent; stale or conflicting writers fail closed.
    ///
    /// # Errors
    ///
    /// Returns a planning error for invalid/conflicting state or a storage
    /// error when the write cannot be reconciled.
    pub async fn persist_transfer_transition(
        &self,
        transition: &TransferTransition,
        expected_revision: u64,
    ) -> Result<u64, MonitorError> {
        transition
            .validate()
            .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
        let path = ChunkKvTransferKey {
            transition_id: transition.transition_id,
        }
        .to_path();
        if let Some((current, revision)) = self.load_transfer_transition(transition.transition_id).await? {
            if current == *transition {
                return Ok(revision);
            }
            if revision != expected_revision {
                return Err(MonitorError::PlanFailed(
                    "transfer transition revision conflict".into(),
                ));
            }
        } else if expected_revision != 0 {
            return Err(MonitorError::PlanFailed(
                "transfer transition revision conflict".into(),
            ));
        }
        let encoded =
            serde_json::to_vec(transition).map_err(|error| MonitorError::Store(error.to_string()))?;
        match self
            .kv
            .put_cas(path.as_bytes(), &encoded, expected_revision)
            .await
        {
            Ok(()) | Err(Group0KvError::CasFailed { .. } | Group0KvError::OutcomeUnknown) => {
                match self.load_transfer_transition(transition.transition_id).await? {
                    Some((current, revision)) if current == *transition => Ok(revision),
                    Some(_) => Err(MonitorError::PlanFailed(
                        "transfer transition write conflicted".into(),
                    )),
                    None => Err(MonitorError::Store(
                        "transfer transition write was not visible after reconciliation".into(),
                    )),
                }
            }
            Err(error) => Err(MonitorError::Store(error.to_string())),
        }
    }

    /// Loads and validates one durable split transition.
    ///
    /// # Errors
    ///
    /// Returns a typed control-plane error for unavailable, malformed, or
    /// internally inconsistent persisted state.
    pub async fn load_split_transition(
        &self,
        transition_id: crowdb_protocol::chunk_kv::Id128,
    ) -> Result<Option<(SplitTransition, u64)>, MonitorError> {
        let path = ChunkKvSplitKey { transition_id }.to_path();
        let stored = self
            .read::<SplitTransition>(&path)
            .await
            .map_err(MonitorError::Store)?;
        if let Some((transition, revision)) = stored {
            transition
                .validate()
                .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
            if transition.transition_id != transition_id {
                return Err(MonitorError::PlanFailed(
                    "split key and record identity differ".into(),
                ));
            }
            return Ok(Some((transition, revision)));
        }
        Ok(None)
    }

    /// Lists one fixed-snapshot view of every durable split transition.
    ///
    /// # Errors
    ///
    /// Returns an error for scan failure, malformed keys or values, or an
    /// identity mismatch. Callers must abandon the whole observation on error.
    pub async fn list_split_transitions(&self) -> Result<Vec<(SplitTransition, u64)>, MonitorError> {
        let prefix = <ChunkKvSplitKey as TextKey>::prefix_all();
        let records = self
            .kv
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|error| MonitorError::Store(error.to_string()))?;
        records
            .into_iter()
            .map(|(key, stored)| {
                let path =
                    std::str::from_utf8(&key).map_err(|error| MonitorError::Store(error.to_string()))?;
                let typed = ChunkKvSplitKey::from_path(path)
                    .map_err(|error| MonitorError::Store(error.to_string()))?;
                let transition: SplitTransition = serde_json::from_slice(&stored.value)
                    .map_err(|error| MonitorError::Store(error.to_string()))?;
                transition
                    .validate()
                    .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
                if typed.transition_id != transition.transition_id {
                    return Err(MonitorError::PlanFailed(
                        "split key and record identity differ".into(),
                    ));
                }
                Ok((transition, stored.revision))
            })
            .collect()
    }

    /// Persists one validated split state with revision fencing and ambiguous
    /// write reconciliation.
    ///
    /// # Errors
    ///
    /// Returns a planning error for invalid/conflicting state or a storage
    /// error when the write cannot be reconciled.
    pub async fn persist_split_transition(
        &self,
        transition: &SplitTransition,
        expected_revision: u64,
    ) -> Result<u64, MonitorError> {
        transition
            .validate()
            .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
        let path = ChunkKvSplitKey {
            transition_id: transition.transition_id,
        }
        .to_path();
        if let Some((current, revision)) = self.load_split_transition(transition.transition_id).await? {
            if current == *transition {
                return Ok(revision);
            }
            if revision != expected_revision {
                return Err(MonitorError::PlanFailed(
                    "split transition revision conflict".into(),
                ));
            }
        } else if expected_revision != 0 {
            return Err(MonitorError::PlanFailed(
                "split transition revision conflict".into(),
            ));
        }
        let encoded =
            serde_json::to_vec(transition).map_err(|error| MonitorError::Store(error.to_string()))?;
        match self
            .kv
            .put_cas(path.as_bytes(), &encoded, expected_revision)
            .await
        {
            Ok(()) | Err(Group0KvError::CasFailed { .. } | Group0KvError::OutcomeUnknown) => {
                match self.load_split_transition(transition.transition_id).await? {
                    Some((current, revision)) if current == *transition => Ok(revision),
                    Some(_) => Err(MonitorError::PlanFailed(
                        "split transition write conflicted".into(),
                    )),
                    None => Err(MonitorError::Store(
                        "split transition write was not visible after reconciliation".into(),
                    )),
                }
            }
            Err(error) => Err(MonitorError::Store(error.to_string())),
        }
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

    async fn reconcile_immutable<T>(&self, path: &str, intended: &T) -> Result<(), ChunkKvRangeCatalogError>
    where
        T: serde::de::DeserializeOwned + PartialEq,
    {
        match self
            .read::<T>(path)
            .await
            .map_err(ChunkKvRangeCatalogError::Unavailable)?
        {
            Some((current, _)) if current == *intended => Ok(()),
            Some(_) => Err(ChunkKvRangeCatalogError::GenerationConflict),
            None => Err(ChunkKvRangeCatalogError::Unavailable(
                "immutable page write was not visible after reconciliation".into(),
            )),
        }
    }
}

#[async_trait]
impl ChunkKvRangeCatalogStore for Group0ControlStore {
    async fn put_page(&self, page: ChunkKvRangeCatalogPage) -> Result<(), ChunkKvRangeCatalogError> {
        page.validate()?;
        let path = ChunkKvRangeCatalogPageKey {
            generation: page.generation,
            page_index: page.page_index,
        }
        .to_path();
        if let Some((current, _)) = self
            .read::<ChunkKvRangeCatalogPage>(&path)
            .await
            .map_err(ChunkKvRangeCatalogError::Unavailable)?
        {
            return if current == page {
                Ok(())
            } else {
                Err(ChunkKvRangeCatalogError::GenerationConflict)
            };
        }
        let encoded = serde_json::to_vec(&page)
            .map_err(|error| ChunkKvRangeCatalogError::Unavailable(error.to_string()))?;
        match self.kv.put_cas(path.as_bytes(), &encoded, 0).await {
            Ok(()) => Ok(()),
            Err(Group0KvError::CasFailed { .. } | Group0KvError::OutcomeUnknown) => {
                self.reconcile_immutable(&path, &page).await
            }
            Err(error) => Err(ChunkKvRangeCatalogError::Unavailable(error.to_string())),
        }
    }

    async fn get_page(
        &self,
        generation: u64,
        page_index: u64,
    ) -> Result<Option<ChunkKvRangeCatalogPage>, ChunkKvRangeCatalogError> {
        let path = ChunkKvRangeCatalogPageKey {
            generation,
            page_index,
        }
        .to_path();
        self.read(&path)
            .await
            .map(|value| value.map(|(page, _)| page))
            .map_err(ChunkKvRangeCatalogError::Unavailable)
    }

    async fn put_head(
        &self,
        head: ChunkKvRangeCatalogHead,
    ) -> Result<HeadWriteOutcome, ChunkKvRangeCatalogError> {
        let path = ChunkKvRangeCatalogHeadKey.to_path();
        let current = self
            .read::<ChunkKvRangeCatalogHead>(&path)
            .await
            .map_err(ChunkKvRangeCatalogError::Unavailable)?;
        if current.as_ref().is_some_and(|(stored, _)| stored == &head) {
            return Ok(HeadWriteOutcome::Committed);
        }
        let expected_revision = current.map_or(0, |(_, revision)| revision);
        let encoded = serde_json::to_vec(&head)
            .map_err(|error| ChunkKvRangeCatalogError::Unavailable(error.to_string()))?;
        match self
            .kv
            .put_cas(path.as_bytes(), &encoded, expected_revision)
            .await
        {
            Ok(()) => Ok(HeadWriteOutcome::Committed),
            Err(Group0KvError::CasFailed { .. }) => {
                let committed = self
                    .read::<ChunkKvRangeCatalogHead>(&path)
                    .await
                    .map_err(ChunkKvRangeCatalogError::Unavailable)?
                    .is_some_and(|(stored, _)| stored == head);
                Ok(if committed {
                    HeadWriteOutcome::Committed
                } else {
                    HeadWriteOutcome::DefinitelyNotCommitted
                })
            }
            Err(Group0KvError::OutcomeUnknown) => Ok(HeadWriteOutcome::Ambiguous),
            Err(error) => Err(ChunkKvRangeCatalogError::Unavailable(error.to_string())),
        }
    }

    async fn get_head(&self) -> Result<Option<ChunkKvRangeCatalogHead>, ChunkKvRangeCatalogError> {
        let path = ChunkKvRangeCatalogHeadKey.to_path();
        self.read(&path)
            .await
            .map(|value| value.map(|(head, _)| head))
            .map_err(ChunkKvRangeCatalogError::Unavailable)
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
