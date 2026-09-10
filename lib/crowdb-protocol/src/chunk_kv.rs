// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable catalog, monitor, and serving-grant types for chunk-backed KV.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::chunk_stream::StreamName;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Id128 {
    pub high: u64,
    pub low: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && self.end.as_deref().map_or(true, |end| key < end)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogPartitionState {
    #[default]
    Prepared,
    Serving,
    SplitPreparing,
    SplitFenced,
    Transferring,
    Retired,
    Faulted,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerDescriptor {
    pub instance_id: u64,
    pub rpc_endpoint: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionArtifact {
    pub tree_manifest: u64,
    pub stream_name: StreamName,
    pub applied_seq: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub partition_id: Id128,
    pub range: KeyRange,
    pub owner: OwnerDescriptor,
    pub owner_epoch: u64,
    pub state: CatalogPartitionState,
    pub artifact: PartitionArtifact,
    pub transition_id: Option<Id128>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPage {
    pub generation: u64,
    pub page_index: u64,
    pub entries: Vec<CatalogEntry>,
    pub checksum: [u8; 32],
}

impl CatalogPage {
    /// Computes and installs the canonical checksum for this page.
    ///
    /// # Errors
    ///
    /// Returns an encoding error if the page cannot be serialized.
    pub fn seal(&mut self) -> Result<(), ChunkKvProtocolError> {
        self.checksum = page_checksum(self)?;
        Ok(())
    }

    /// Validates identity, ordering, and checksum before publication.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error for malformed page content.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if self.generation == 0 || self.entries.is_empty() || self.checksum != page_checksum(self)? {
            return Err(ChunkKvProtocolError::InvalidCatalogPage);
        }
        for entry in &self.entries {
            validate_entry(entry)?;
        }
        if self
            .entries
            .windows(2)
            .any(|pair| pair[0].range.start >= pair[1].range.start)
        {
            return Err(ChunkKvProtocolError::InvalidCatalogPage);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPageRef {
    pub page_generation: u64,
    pub page_index: u64,
    pub first_key: Vec<u8>,
    pub page_checksum: [u8; 32],
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogHead {
    pub generation: u64,
    pub previous_generation: Option<u64>,
    pub pages: Vec<CatalogPageRef>,
    pub checksum: [u8; 32],
}

impl CatalogHead {
    /// Computes and installs the canonical checksum for this head.
    ///
    /// # Errors
    ///
    /// Returns an encoding error if the head cannot be serialized.
    pub fn seal(&mut self) -> Result<(), ChunkKvProtocolError> {
        self.checksum = head_checksum(self)?;
        Ok(())
    }

    /// Validates the head and every referenced page as one complete keyspace.
    ///
    /// # Errors
    ///
    /// Returns an error for bad checksums, identities, ordering, holes,
    /// overlaps, or incomplete binary-keyspace coverage.
    pub fn validate_pages(&self, pages: &[CatalogPage]) -> Result<(), ChunkKvProtocolError> {
        if self.generation == 0
            || self.pages.is_empty()
            || self.checksum != head_checksum(self)?
            || pages.len() != self.pages.len()
            || self
                .previous_generation
                .is_some_and(|previous| previous >= self.generation)
        {
            return Err(ChunkKvProtocolError::InvalidCatalogHead);
        }
        let mut entries = Vec::new();
        for (reference, page) in self.pages.iter().zip(pages) {
            page.validate()?;
            if page.generation != reference.page_generation
                || page.generation > self.generation
                || page.page_index != reference.page_index
                || page.checksum != reference.page_checksum
                || page.entries.first().map(|entry| &entry.range.start) != Some(&reference.first_key)
            {
                return Err(ChunkKvProtocolError::InvalidCatalogHead);
            }
            entries.extend(page.entries.iter());
        }
        validate_complete_entries(&entries)
    }

    /// Validates a monotonic successor and prevents ownership epoch regression.
    ///
    /// # Errors
    ///
    /// Returns an error if either generation is invalid, the generation does
    /// not advance, or a retained partition's ownership epoch decreases.
    pub fn validate_successor(
        &self,
        pages: &[CatalogPage],
        previous: &CatalogHead,
        previous_pages: &[CatalogPage],
    ) -> Result<(), ChunkKvProtocolError> {
        previous.validate_pages(previous_pages)?;
        self.validate_pages(pages)?;
        if self.generation <= previous.generation {
            return Err(ChunkKvProtocolError::CatalogRegression);
        }
        let prior_epochs: HashMap<Id128, u64> = previous_pages
            .iter()
            .flat_map(|page| page.entries.iter())
            .map(|entry| (entry.partition_id, entry.owner_epoch))
            .collect();
        if pages.iter().flat_map(|page| page.entries.iter()).any(|entry| {
            prior_epochs
                .get(&entry.partition_id)
                .is_some_and(|previous_epoch| entry.owner_epoch < *previous_epoch)
        }) {
            return Err(ChunkKvProtocolError::CatalogRegression);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainFailurePolicy {
    #[default]
    AutomaticSharedStorage,
    OperatorOnly,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainMonitorDescriptor {
    pub domain: String,
    pub service_registry_name: String,
    pub driver_version: u32,
    pub capability_version: u32,
    pub heartbeat_interval_ms: u64,
    pub suspect_after_ms: u64,
    pub dead_after_ms: u64,
    pub lease_duration_ms: u64,
    pub max_clock_skew_ms: u64,
    pub self_fence_margin_ms: u64,
    pub failure_policy: DomainFailurePolicy,
    pub balance_policy: String,
}

impl DomainMonitorDescriptor {
    /// Validates ordered liveness and conservative lease timing bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for missing identity/version or unsafe timing.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if !valid_name(&self.domain)
            || !valid_name(&self.service_registry_name)
            || self.driver_version == 0
            || self.capability_version == 0
            || self.balance_policy.is_empty()
            || self.heartbeat_interval_ms == 0
            || self.suspect_after_ms < self.heartbeat_interval_ms
            || self.dead_after_ms < self.suspect_after_ms
            || self
                .max_clock_skew_ms
                .checked_add(self.self_fence_margin_ms)
                .map_or(true, |margin| self.lease_duration_ms <= margin)
            || self
                .dead_after_ms
                .checked_add(self.self_fence_margin_ms)
                .map_or(true, |deadline| deadline > self.lease_duration_ms)
        {
            return Err(ChunkKvProtocolError::InvalidMonitorDescriptor);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnsureDomainMonitorRequest {
    pub descriptor: DomainMonitorDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnsureDomainMonitorOutcome {
    Created,
    AlreadyExists,
    DescriptorConflict,
    UnsupportedMonitorDomain,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceHealth {
    #[default]
    Healthy,
    Suspect,
    Dead,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedPartition {
    pub partition_id: Id128,
    pub owner_epoch: u64,
    pub recovering: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkKvInstanceObservation {
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub last_heartbeat_ms: u64,
    pub capacity_bytes: u64,
    pub durable_bytes: u64,
    pub request_rate: u64,
    pub hosted: Vec<HostedPartition>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ServingAssignment {
    pub partition_id: Id128,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServingGrant {
    pub instance_id: u64,
    pub lease_sequence: u64,
    pub catalog_generation: u64,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub assignments: Vec<ServingAssignment>,
    pub assignment_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientRequestId {
    pub client_instance_id: Id128,
    pub client_sequence: u64,
}

impl ClientRequestId {
    /// Validates the stable logical request identity used across retries.
    ///
    /// # Errors
    ///
    /// Returns an error when either identity component is zero.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if self.client_instance_id == Id128::default() || self.client_sequence == 0 {
            return Err(ChunkKvProtocolError::InvalidRpcRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcJournalPosition {
    pub stream_name: Id128,
    pub offset: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestRouting {
    pub request_id: ClientRequestId,
    pub map_revision: u64,
    pub partition_id: Id128,
    pub owner_epoch: u64,
    pub min_journal_position: Option<RpcJournalPosition>,
    pub deadline_ms: Option<u64>,
}

impl RequestRouting {
    /// Validates identity and routing fields before request admission.
    ///
    /// # Errors
    ///
    /// Returns an error when a stable identity or authority field is absent.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        self.request_id.validate()?;
        if self.map_revision == 0 || self.partition_id == Id128::default() || self.owner_epoch == 0 {
            return Err(ChunkKvProtocolError::InvalidRpcRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PointOperation {
    Get {
        key: Vec<u8>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    PutIfAbsent {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    CompareExchange {
        key: Vec<u8>,
        condition: RpcCompareCondition,
        value: Vec<u8>,
    },
    ConditionalDelete {
        key: Vec<u8>,
        condition: RpcCompareCondition,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcCompareCondition {
    Revision(u64),
    Value(Vec<u8>),
}

impl PointOperation {
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get { key }
            | Self::Put { key, .. }
            | Self::Delete { key }
            | Self::PutIfAbsent { key, .. }
            | Self::CompareExchange { key, .. }
            | Self::ConditionalDelete { key, .. } => key,
        }
    }

    #[must_use]
    pub fn is_mutation(&self) -> bool {
        !matches!(self, Self::Get { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointRequest {
    pub routing: RequestRouting,
    pub operation: PointOperation,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeekKind {
    #[default]
    Ceiling,
    Higher,
    Floor,
    Lower,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeekRequest {
    pub routing: RequestRouting,
    pub key: Vec<u8>,
    pub kind: SeekKind,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScanDirection {
    #[default]
    Forward,
    Reverse,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanContinuation {
    pub direction: ScanDirection,
    pub last_key: Vec<u8>,
    pub partition_id: Id128,
    pub owner_epoch: u64,
    pub map_revision: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanRequest {
    pub routing: RequestRouting,
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub direction: ScanDirection,
    pub limit: u32,
    pub continuation: Option<ScanContinuation>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationResult {
    Value(Option<RpcValue>),
    Mutation {
        applied: bool,
        revision: Option<u64>,
        observed: Option<RpcValue>,
    },
    Scan {
        items: Vec<RpcValue>,
        continuation: Option<ScanContinuation>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkKvRpcErrorCode {
    Overloaded,
    WriteStalled,
    Recovering,
    LeaseExpired,
    RequestExpired,
    RequestConflict,
    NotMyRange,
    RefreshRequired,
    InvalidRequest,
    Internal,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerHint {
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcFailure {
    pub code: ChunkKvRpcErrorCode,
    pub message: String,
    pub retry_after_ms: Option<u64>,
    pub latest_map_revision: Option<u64>,
    pub owner_hint: Option<OwnerHint>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkKvResponse {
    pub map_revision: u64,
    pub journal_position: Option<RpcJournalPosition>,
    pub result: Result<OperationResult, RpcFailure>,
}

impl ServingGrant {
    /// Sorts assignments and installs their canonical digest.
    pub fn seal(&mut self) {
        self.assignments.sort_unstable();
        self.assignment_digest = assignment_digest(&self.assignments);
    }

    /// Validates lease identity, deadline, sorted uniqueness, and digest.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or ambiguous authority.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if self.instance_id == 0
            || self.lease_sequence == 0
            || self.catalog_generation == 0
            || self.expires_at_ms <= self.issued_at_ms
            || self.assignments.is_empty()
            || self.assignments.windows(2).any(|pair| pair[0] >= pair[1])
            || self
                .assignments
                .iter()
                .any(|assignment| assignment.owner_epoch == 0)
            || self.assignment_digest != assignment_digest(&self.assignments)
        {
            return Err(ChunkKvProtocolError::InvalidServingGrant);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChunkKvProtocolError {
    #[error("catalog entry is invalid")]
    InvalidCatalogEntry,
    #[error("catalog page is invalid")]
    InvalidCatalogPage,
    #[error("catalog head is invalid")]
    InvalidCatalogHead,
    #[error("catalog does not cover the complete binary keyspace")]
    IncompleteKeyspace,
    #[error("catalog generation or ownership epoch regressed")]
    CatalogRegression,
    #[error("domain monitor descriptor is invalid")]
    InvalidMonitorDescriptor,
    #[error("serving grant is invalid")]
    InvalidServingGrant,
    #[error("chunk KV RPC request is invalid")]
    InvalidRpcRequest,
    #[error("protocol record encoding failed")]
    Encoding,
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn validate_entry(entry: &CatalogEntry) -> Result<(), ChunkKvProtocolError> {
    if entry.partition_id == Id128::default()
        || entry.owner.instance_id == 0
        || entry.owner.rpc_endpoint.is_empty()
        || entry.owner_epoch == 0
        || entry.artifact.stream_name == StreamName::default()
        || entry
            .range
            .end
            .as_ref()
            .is_some_and(|end| entry.range.start >= *end)
    {
        return Err(ChunkKvProtocolError::InvalidCatalogEntry);
    }
    Ok(())
}

fn validate_complete_entries(entries: &[&CatalogEntry]) -> Result<(), ChunkKvProtocolError> {
    if entries
        .first()
        .map_or(true, |entry| !entry.range.start.is_empty())
        || entries.last().map_or(true, |entry| entry.range.end.is_some())
    {
        return Err(ChunkKvProtocolError::IncompleteKeyspace);
    }
    for pair in entries.windows(2) {
        if pair[0].range.end.as_ref() != Some(&pair[1].range.start) {
            return Err(ChunkKvProtocolError::IncompleteKeyspace);
        }
    }
    let mut identities = HashSet::with_capacity(entries.len());
    if entries.iter().any(|entry| !identities.insert(entry.partition_id)) {
        return Err(ChunkKvProtocolError::InvalidCatalogEntry);
    }
    Ok(())
}

fn page_checksum(page: &CatalogPage) -> Result<[u8; 32], ChunkKvProtocolError> {
    hash_encoded(&(page.generation, page.page_index, &page.entries))
}

fn head_checksum(head: &CatalogHead) -> Result<[u8; 32], ChunkKvProtocolError> {
    hash_encoded(&(head.generation, head.previous_generation, &head.pages))
}

#[must_use]
pub fn assignment_digest(assignments: &[ServingAssignment]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for assignment in assignments {
        digest.update(assignment.partition_id.high.to_le_bytes());
        digest.update(assignment.partition_id.low.to_le_bytes());
        digest.update(assignment.owner_epoch.to_le_bytes());
    }
    digest.finalize().into()
}

fn hash_encoded<T: Serialize>(value: &T) -> Result<[u8; 32], ChunkKvProtocolError> {
    let encoded = bincode::serialize(value).map_err(|_| ChunkKvProtocolError::Encoding)?;
    Ok(Sha256::digest(encoded).into())
}
