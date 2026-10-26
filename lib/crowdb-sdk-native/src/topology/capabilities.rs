// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::{TopologyFailureKind, TopologySource};
use crate::{Error, Result};
use crowdb_rpc_ffi::RdmaWireLayout;
use serde_json::Value;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportGroup {
    Metadata,
    Aggregate,
    Data,
}

#[derive(Clone, Debug)]
pub struct RdmaCapabilities {
    pub ready: bool,
    pub layout: RdmaWireLayout,
    pub data_slot_capacity: u32,
    pub control_slot_capacity: u32,
    pub max_control_message_bytes: u32,
    /// Configured SQ bound; each provider connection reports its actual bound separately.
    pub configured_sq_depth: u32,
    pub configured_rq_depth: u32,
    pub registered_memory_budget_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct TransportCapabilities {
    pub group: TransportGroup,
    pub observed_at: Instant,
    pub tcp_fallback_allowed: bool,
    pub rdma: Option<RdmaCapabilities>,
}

pub(super) fn observe(metadata: &[u8], data: Option<&[u8]>) -> Result<Vec<TransportCapabilities>> {
    let metadata = parse(metadata, TopologySource::MetadataTransport)?;
    let mut result = Vec::with_capacity(3);
    for (name, group) in [
        ("metadata", TransportGroup::Metadata),
        ("aggregate", TransportGroup::Aggregate),
    ] {
        result.push(native(
            &metadata[name]["transport"],
            group,
            TopologySource::MetadataTransport,
        )?);
    }
    if let Some(data) = data {
        let data = parse(data, TopologySource::DataTransport)?;
        result.push(native(
            &data,
            TransportGroup::Data,
            TopologySource::DataTransport,
        )?);
    }
    Ok(result)
}

fn parse(bytes: &[u8], origin: TopologySource) -> Result<Value> {
    let value: Value = serde_json::from_slice(bytes).map_err(|error| invalid(origin, &error.to_string()))?;
    if value["schema_version"].as_u64() != Some(1) {
        return Err(invalid(origin, "unsupported transport status schema"));
    }
    Ok(value)
}

fn native(value: &Value, group: TransportGroup, origin: TopologySource) -> Result<TransportCapabilities> {
    if value["schema_version"].as_u64() != Some(1) {
        return Err(invalid(origin, "unsupported native status schema"));
    }
    let tcp_fallback_allowed = value["allow_tcp_fallback"]
        .as_bool()
        .ok_or_else(|| invalid(origin, "missing TCP policy"))?;
    let rdma = if value["rdma"].is_null() {
        None
    } else {
        let rdma = &value["rdma"];
        let layout = match rdma["wire_layout"].as_str() {
            Some("control_data") => RdmaWireLayout::ControlData,
            Some("contiguous") => RdmaWireLayout::Contiguous,
            _ => return Err(invalid(origin, "unknown RDMA layout")),
        };
        Some(RdmaCapabilities {
            ready: rdma["ready"]
                .as_bool()
                .ok_or_else(|| invalid(origin, "missing RDMA readiness"))?,
            layout,
            data_slot_capacity: number(rdma, "data_slot_capacity", origin)?,
            control_slot_capacity: number(rdma, "control_slot_capacity", origin)?,
            max_control_message_bytes: number(rdma, "max_control_message_bytes", origin)?,
            configured_sq_depth: number(rdma, "sq_depth", origin)?,
            configured_rq_depth: number(rdma, "rq_depth", origin)?,
            registered_memory_budget_bytes: rdma["registered_memory_budget_bytes"]
                .as_u64()
                .ok_or_else(|| invalid(origin, "missing registration budget"))?,
        })
    };
    Ok(TransportCapabilities {
        group,
        observed_at: Instant::now(),
        tcp_fallback_allowed,
        rdma,
    })
}

fn number(value: &Value, field: &str, origin: TopologySource) -> Result<u32> {
    value[field]
        .as_u64()
        .and_then(|number| u32::try_from(number).ok())
        .ok_or_else(|| invalid(origin, field))
}

fn invalid(origin: TopologySource, detail: &str) -> Error {
    Error::Topology {
        origin,
        kind: TopologyFailureKind::Invalid,
        detail: detail.into(),
    }
}
