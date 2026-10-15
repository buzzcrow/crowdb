// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Private immutable supplied-medium journals; no allocation or membership here.

use super::DiskInput;
use crate::node::durable;
use crowdb_protocol::mgmt::node::storage::{
    PrepareSuppliedDisk, SuppliedPreparation, SuppliedPreparationPhase,
};
use std::{
    io,
    path::{Path, PathBuf},
};

pub(crate) fn path(root: &Path, id: &str) -> io::Result<PathBuf> {
    let parsed = uuid::Uuid::parse_str(id).map_err(io::Error::other)?;
    if parsed.is_nil() || parsed.to_string() != id {
        return Err(io::Error::other(
            "supplied operation must be a canonical non-nil UUID",
        ));
    }
    Ok(root.join(format!("disk-supplied-{id}.json")))
}

pub(crate) fn lookup(root: &Path, id: &str) -> io::Result<Option<SuppliedPreparation>> {
    if !path(root, id)?.try_exists()? {
        return Ok(None);
    }
    status(root, id).map(Some)
}

pub(crate) fn list(
    root: &Path,
    after: Option<&str>,
    limit: u32,
) -> io::Result<crowdb_protocol::mgmt::node::storage::SuppliedPreparationPage> {
    use crowdb_protocol::mgmt::node::storage::SuppliedPreparationPage;
    if limit == 0 || limit > 16 {
        return Err(io::Error::other("supplied operation page limit must be 1..16"));
    }
    if let Some(after) = after {
        path(root, after)?;
    }
    let ids = operations(root)?;
    let mut page = SuppliedPreparationPage {
        items: Vec::new(),
        next: None,
    };
    let mut bytes = 0;
    for id in ids
        .iter()
        .filter(|id| after.map_or(true, |after| id.as_str() > after))
    {
        let item = status(root, id)?;
        let size = serde_json::to_vec(&item)?.len();
        if size > 48 * 1024 {
            return Err(io::Error::other("supplied operation record exceeds reply bound"));
        }
        if page.items.len() >= limit as usize || bytes + size > 56 * 1024 {
            page.next = page
                .items
                .last()
                .map(|item| item.request.record.operation_id.clone());
            break;
        }
        bytes += size;
        page.items.push(item);
    }
    Ok(page)
}

pub(crate) fn status(root: &Path, id: &str) -> io::Result<SuppliedPreparation> {
    let state: SuppliedPreparation = durable::read(&path(root, id)?)?
        .ok_or_else(|| io::Error::other("supplied operation does not exist"))?;
    state.request.validate().map_err(io::Error::other)?;
    if state.version != 1 || state.request.record.operation_id != id {
        return Err(io::Error::other("supplied operation identity or version changed"));
    }
    Ok(state)
}

pub(crate) fn save(root: &Path, state: &SuppliedPreparation) -> io::Result<()> {
    state.request.validate().map_err(io::Error::other)?;
    if state.version != 1 || state.error.as_ref().is_some_and(|value| value.len() > 512) {
        return Err(io::Error::other("invalid supplied operation state"));
    }
    durable::write(&path(root, &state.request.record.operation_id)?, state)
}

pub(crate) fn inputs() -> io::Result<Vec<DiskInput>> {
    serde_json::from_str(&std::env::var("CROWDB_DISK_INPUTS").unwrap_or_else(|_| "[]".into()))
        .map_err(io::Error::other)
}

pub(crate) fn accept(root: &Path, request: PrepareSuppliedDisk) -> io::Result<SuppliedPreparation> {
    accept_with_inputs(root, request, &inputs()?)
}

pub(crate) fn accept_with_inputs(
    root: &Path,
    request: PrepareSuppliedDisk,
    supplied: &[DiskInput],
) -> io::Result<SuppliedPreparation> {
    request.validate().map_err(io::Error::other)?;
    let target = path(root, &request.record.operation_id)?;
    if target.exists() {
        let mut current = status(root, &request.record.operation_id)?;
        if current.request != request {
            return Err(io::Error::other("supplied operation inputs changed"));
        }
        if current.phase == SuppliedPreparationPhase::Failed {
            current.phase = SuppliedPreparationPhase::Accepted;
            current.error = None;
            current.verified_bytes = 0;
            save(root, &current)?;
        }
        return Ok(current);
    }
    super::blank::observe(root, supplied, &request.record)?;
    let state = SuppliedPreparation {
        version: 1,
        request,
        phase: SuppliedPreparationPhase::Accepted,
        verified_bytes: 0,
        error: None,
    };
    save(root, &state)?;
    Ok(state)
}

pub(crate) fn operations(root: &Path) -> io::Result<Vec<String>> {
    let mut ids = Vec::new();
    for (index, entry) in std::fs::read_dir(root)?.enumerate() {
        if index >= 8192 {
            return Err(io::Error::other("node operation discovery exceeds bound"));
        }
        let name = entry?.file_name();
        let Some(id) = name
            .to_str()
            .and_then(|name| name.strip_prefix("disk-supplied-"))
            .and_then(|name| name.strip_suffix(".json"))
        else {
            continue;
        };
        path(root, id)?;
        ids.push(id.to_owned());
        if ids.len() > 4096 {
            return Err(io::Error::other("supplied operation inventory exceeds bound"));
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

#[cfg(feature = "test-util")]
pub struct SuppliedOperationsForTests;

#[cfg(feature = "test-util")]
impl SuppliedOperationsForTests {
    /// # Errors
    /// Propagates production journal, request and exact input validation.
    pub fn accept_for_tests(
        root: &Path,
        request: PrepareSuppliedDisk,
        inputs: &[DiskInput],
    ) -> io::Result<SuppliedPreparation> {
        accept_with_inputs(root, request, inputs)
    }
    /// # Errors
    /// Propagates production cursor, bound and durable-record validation.
    pub fn list_for_tests(
        root: &Path,
        after: Option<&str>,
        limit: u32,
    ) -> io::Result<crowdb_protocol::mgmt::node::storage::SuppliedPreparationPage> {
        list(root, after, limit)
    }
    /// # Errors
    /// Missing journals return None; other read/validation errors remain errors.
    pub fn lookup_for_tests(root: &Path, id: &str) -> io::Result<Option<SuppliedPreparation>> {
        lookup(root, id)
    }
}
