// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Idempotent registration of persisted group-0 domain monitors.

use std::sync::Arc;

use crowdb_protocol::chunk_kv::{
    DomainMonitorDescriptor, EnsureDomainMonitorOutcome, EnsureDomainMonitorRequest,
};
use crowdb_protocol::key::{DomainMonitorKey, TextKey};

use crate::{CrowdbKvClient, Error, GetOutcome, ReadMode, Result};

const GROUP_ZERO: u64 = 0;

#[derive(Clone)]
pub struct DomainMonitorClient {
    kv: Arc<CrowdbKvClient>,
}

impl DomainMonitorClient {
    #[must_use]
    pub fn from_shared(kv: Arc<CrowdbKvClient>) -> Self {
        Self { kv }
    }

    /// Persist a monitor descriptor exactly once, reconciling a create race by
    /// rereading the winner.
    ///
    /// # Errors
    ///
    /// Returns a validation, serialization, transport, or ambiguous-CAS error.
    pub async fn ensure(&self, request: &EnsureDomainMonitorRequest) -> Result<EnsureDomainMonitorOutcome> {
        request
            .descriptor
            .validate()
            .map_err(|error| Error::Server(error.to_string()))?;
        let path = DomainMonitorKey {
            domain: request.descriptor.domain.clone(),
        }
        .to_path();
        let value = serde_json::to_vec(&request.descriptor).map_err(|error| Error::SysdataDecode {
            key: path.clone(),
            reason: error.to_string(),
        })?;
        match self.read(&path).await? {
            Some(existing) if existing == request.descriptor => Ok(EnsureDomainMonitorOutcome::AlreadyExists),
            Some(_) => Ok(EnsureDomainMonitorOutcome::DescriptorConflict),
            None => match self
                .kv
                .put_cas(GROUP_ZERO, GROUP_ZERO, path.as_bytes(), &value, 0)
                .await
            {
                Ok(_) => Ok(EnsureDomainMonitorOutcome::Created),
                Err(Error::CasFailed { .. }) => match self.read(&path).await? {
                    Some(existing) if existing == request.descriptor => {
                        Ok(EnsureDomainMonitorOutcome::AlreadyExists)
                    }
                    Some(_) => Ok(EnsureDomainMonitorOutcome::DescriptorConflict),
                    None => Err(Error::OutcomeUnknown),
                },
                Err(error) => Err(error),
            },
        }
    }

    async fn read(&self, path: &str) -> Result<Option<DomainMonitorDescriptor>> {
        match self
            .kv
            .get(
                GROUP_ZERO,
                GROUP_ZERO,
                path.as_bytes(),
                ReadMode::Linearizable,
                None,
            )
            .await?
        {
            GetOutcome::Found { value, .. } => {
                serde_json::from_slice(&value)
                    .map(Some)
                    .map_err(|error| Error::SysdataDecode {
                        key: path.to_string(),
                        reason: error.to_string(),
                    })
            }
            GetOutcome::NotFound => Ok(None),
        }
    }
}
