// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded pages of durable file outcomes, including failures before file creation.

use std::{fs, io, path::Path};

use crowdb_protocol::mgmt::node::storage::FilePreparationPage;

pub(crate) fn list(root: &Path, after: Option<&str>, limit: u32) -> io::Result<FilePreparationPage> {
    if limit == 0 || limit > 32 {
        return Err(io::Error::other("file operation page limit must be 1..32"));
    }
    if let Some(after) = after {
        canonical_id(after)?;
    }
    let limit = usize::try_from(limit).map_err(io::Error::other)?;
    let mut ids = Vec::new();
    for (index, entry) in fs::read_dir(root)?.enumerate() {
        if index >= 8192 {
            return Err(io::Error::other("node root exceeds operation discovery bound"));
        }
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(id) = name
            .strip_prefix("disk-file-")
            .and_then(|name| name.strip_suffix(".json"))
        else {
            continue;
        };
        canonical_id(id)?;
        if after.map_or(true, |after| id > after) {
            ids.push(id.to_owned());
        }
        if ids.len() > 4096 {
            return Err(io::Error::other("file operation inventory exceeds bound"));
        }
    }
    ids.sort_unstable();
    let more = ids.len() > limit;
    ids.truncate(limit);
    let next = if more { ids.last().cloned() } else { None };
    let items = ids
        .into_iter()
        .map(|id| super::file_preparation::status(root, &id))
        .collect::<io::Result<_>>()?;
    Ok(FilePreparationPage { items, next })
}

fn canonical_id(id: &str) -> io::Result<()> {
    if uuid::Uuid::parse_str(id).map_err(io::Error::other)?.to_string() != id {
        return Err(io::Error::other("file operation cursor must be a canonical UUID"));
    }
    Ok(())
}

#[cfg(feature = "test-util")]
/// # Errors
/// Propagates production cursor validation and durable-record read failures.
pub fn file_operations_for_tests(
    root: &Path,
    after: Option<&str>,
    limit: u32,
) -> io::Result<FilePreparationPage> {
    list(root, after, limit)
}
