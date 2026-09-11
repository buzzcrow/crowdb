// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use crowdb_protocol::chunk_kv::{
    DomainMonitorDescriptor, EnsureDomainMonitorOutcome, EnsureDomainMonitorRequest,
};
use thiserror::Error;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum MonitorError {
    #[error("monitor descriptor is invalid")]
    InvalidDescriptor,
    #[error("monitor domain or driver version is unsupported")]
    UnsupportedDomain,
    #[error("monitor descriptor conflicts with persisted policy")]
    DescriptorConflict,
    #[error("monitor control-plane read failed: {0}")]
    ReadFailed(String),
    #[error("monitor planning failed: {0}")]
    PlanFailed(String),
    #[error("monitor publication failed: {0}")]
    PublishFailed(String),
    #[error("monitor descriptor storage failed: {0}")]
    Store(String),
}

#[async_trait]
pub trait MonitorDescriptorStore: Send + Sync {
    /// Atomically creates the descriptor, accepts an exact duplicate, or
    /// reports a conflict without replacing the existing descriptor.
    async fn ensure(
        &self,
        descriptor: &DomainMonitorDescriptor,
    ) -> Result<EnsureDomainMonitorOutcome, MonitorError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupportedMonitor {
    pub domain: String,
    pub driver_version: u32,
    pub max_capability_version: u32,
}

pub struct DomainMonitorRegistry {
    store: Arc<dyn MonitorDescriptorStore>,
    supported: Vec<SupportedMonitor>,
}

impl DomainMonitorRegistry {
    #[must_use]
    pub fn new(store: Arc<dyn MonitorDescriptorStore>, supported: Vec<SupportedMonitor>) -> Self {
        Self { store, supported }
    }

    /// Ensures one durable, compiled monitor policy without silent upgrades.
    ///
    /// # Errors
    ///
    /// Returns an error when validation or descriptor storage fails. Unsupported
    /// and conflicting requests are closed protocol outcomes, not storage errors.
    pub async fn ensure(
        &self,
        request: &EnsureDomainMonitorRequest,
    ) -> Result<EnsureDomainMonitorOutcome, MonitorError> {
        request
            .descriptor
            .validate()
            .map_err(|_| MonitorError::InvalidDescriptor)?;
        let supported = self.supported.iter().any(|supported| {
            supported.domain == request.descriptor.domain
                && supported.driver_version == request.descriptor.driver_version
                && supported.max_capability_version >= request.descriptor.capability_version
        });
        if !supported {
            return Ok(EnsureDomainMonitorOutcome::UnsupportedMonitorDomain);
        }
        self.store.ensure(&request.descriptor).await
    }
}

#[async_trait]
pub trait DomainMonitorDriver: Send + Sync {
    type Snapshot: Send;
    type Plan: Send;

    /// Reads every registry, catalog, binding, and transition input needed by
    /// this tick. Any error aborts the tick before planning or publication.
    async fn observe(&self) -> Result<Self::Snapshot, MonitorError>;
    /// Derives a deterministic plan from one complete observation.
    ///
    /// # Errors
    ///
    /// Returns a planning error without publishing partial state.
    fn plan(&self, snapshot: Self::Snapshot) -> Result<Self::Plan, MonitorError>;
    /// Publishes the completed plan under the caller's leader fence.
    ///
    /// # Errors
    ///
    /// Returns a publication error for reconciliation by a later tick.
    async fn publish(&self, plan: Self::Plan) -> Result<(), MonitorError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MonitorTick {
    Standby,
    Published,
}

/// A monitor task prepared on every replica and publication-gated by leadership.
pub struct PreparedMonitor<D: DomainMonitorDriver> {
    driver: D,
}

impl<D: DomainMonitorDriver> PreparedMonitor<D> {
    #[must_use]
    pub fn new(driver: D) -> Self {
        Self { driver }
    }

    /// Executes one staged tick only while its caller holds the group-0 leader fence.
    ///
    /// # Errors
    ///
    /// Returns the original observation, planning, or publication failure. In
    /// particular, an observation failure can never manufacture an empty input.
    pub async fn tick(&self, is_group0_leader: bool) -> Result<MonitorTick, MonitorError> {
        if !is_group0_leader {
            return Ok(MonitorTick::Standby);
        }
        let snapshot = self.driver.observe().await?;
        let plan = self.driver.plan(snapshot)?;
        self.driver.publish(plan).await?;
        Ok(MonitorTick::Published)
    }
}
