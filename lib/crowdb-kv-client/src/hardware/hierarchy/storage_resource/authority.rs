// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Project only the immutable physical-host fields from authoritative node admission.

use crate::{CrowdbKvClient, Error, GetOutcome, ReadMode, Result};
use crowdb_protocol::disk_storage::DiskStorageRecord;
use serde::Deserialize;
use std::collections::BTreeMap;

pub(super) type Hosts = BTreeMap<u64, String>;

#[derive(Deserialize)]
struct Registry {
    nodes: Vec<AdmissionNode>,
}
#[derive(Deserialize)]
struct AdmissionNode {
    node_id: u64,
    rack_id: u64,
    physical_host_id: String,
    confirmed: bool,
    #[serde(default)]
    cancelled: bool,
}

pub(super) async fn hosts(kv: &CrowdbKvClient, record: &DiskStorageRecord) -> Result<Hosts> {
    let GetOutcome::Found { value, .. } = kv
        .get(0, 0, b"/deployment/nodes", ReadMode::Linearizable, None)
        .await?
    else {
        return Err(Error::Server(
            "physical resource claim is waiting for confirmed node admission".into(),
        ));
    };
    if value.len() > 4 * 1024 * 1024 {
        return Err(Error::Server("physical node authority exceeds bound".into()));
    }
    let registry: Registry =
        serde_json::from_slice(&value).map_err(|error| Error::Server(error.to_string()))?;
    if registry.nodes.len() > 4096 {
        return Err(Error::Server("physical node authority exceeds bound".into()));
    }
    let backing = record
        .physical_backing
        .as_ref()
        .ok_or_else(|| Error::Server("physical backing missing".into()))?;
    let mut hosts = Hosts::new();
    let mut authorized = false;
    for node in registry.nodes {
        if node.physical_host_id.is_empty()
            || node.physical_host_id.len() > 256
            || node.physical_host_id.contains('\0')
            || hosts
                .insert(node.node_id, node.physical_host_id.clone())
                .is_some()
        {
            return Err(Error::Server(
                "physical node authority is invalid or ambiguous".into(),
            ));
        }
        if node.node_id == record.node_id {
            authorized = node.rack_id == record.rack_id
                && node.confirmed
                && !node.cancelled
                && node.physical_host_id == backing.physical_host_id;
        }
    }
    if !authorized {
        return Err(Error::Server(
            "physical resource host differs from confirmed node admission".into(),
        ));
    }
    Ok(hosts)
}
