// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Keep UUID words out of browser JSON numbers.

use super::Failure;
use crowdb_protocol::{
    mgmt::node::{
        setup::SetupMedium,
        storage::{SuppliedPreparation, SuppliedPreparationPhase},
    },
    DiskIdExt,
};
use serde::Serialize;

#[derive(Serialize)]
pub(crate) struct CandidateOutcome {
    pub operation_id: String,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub disk_id: String,
    pub medium: SetupMedium,
    pub phase: SuppliedPreparationPhase,
    pub verified_bytes: u64,
    pub error: Option<String>,
}
#[derive(Serialize)]
pub(crate) struct Page {
    pub items: Vec<CandidateOutcome>,
    pub next: Option<String>,
}
impl CandidateOutcome {
    pub(super) fn from_state(state: &SuppliedPreparation) -> Result<Self, Failure> {
        state.request.validate().map_err(crate::error::err_502)?;
        let record = &state.request.record;
        let physical = record
            .physical_backing
            .as_ref()
            .ok_or_else(|| crate::error::err_502("Supplied backing receipt missing"))?;
        Ok(Self {
            operation_id: record.operation_id.clone(),
            node_id: record.node_id,
            disk_group_id: record.disk_group_id,
            disk_id: record.disk_id.to_display_string(),
            medium: SetupMedium {
                path: record.geometry.device_path.clone(),
                backend: record.backend,
                backing_identity: record.backing_identity.clone(),
                resource_identity: Some(physical.resource_identity.clone()),
                capacity_bytes: physical.capacity_bytes,
                media_type: record.media_type,
                initialize: state.request.initialize,
            },
            phase: state.phase,
            verified_bytes: state.verified_bytes,
            error: state.error.clone(),
        })
    }
}
