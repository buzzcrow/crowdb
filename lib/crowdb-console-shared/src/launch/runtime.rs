// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Process identity belongs to the runtime directory, never the launch registry.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::config::web::LaunchRecord;
use crate::error::{Error, Result};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub start_ticks: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeRecord {
    version: u32,
    host: String,
    identity: ProcessIdentity,
}

pub(super) fn path(root: &Path, launch: &LaunchRecord) -> PathBuf {
    root.join(format!("{}-{}.json", launch.node_id, launch.service_id))
}

pub(super) fn load(root: &Path, launch: &LaunchRecord) -> Result<Option<ProcessIdentity>> {
    let body = match std::fs::read(path(root, launch)) {
        Ok(body) => body,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let record: RuntimeRecord =
        serde_json::from_slice(&body).map_err(|error| Error::Config(error.to_string()))?;
    if record.version != 1 || record.host != launch.host || record.identity.pid == 0 {
        return Err(Error::Config(
            "launch runtime identity does not match its host".into(),
        ));
    }
    Ok(Some(record.identity))
}

pub(super) fn save(root: &Path, launch: &LaunchRecord, identity: ProcessIdentity) -> Result<()> {
    use std::io::Write;
    std::fs::create_dir_all(root)?;
    let path = path(root, launch);
    let temp = path.with_extension(format!(
        "{}.{}.tmp",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        let body = serde_json::to_vec(&RuntimeRecord {
            version: 1,
            host: launch.host.clone(),
            identity,
        })
        .map_err(|error| Error::Config(error.to_string()))?;
        file.write_all(&body)?;
        file.sync_all()?;
        std::fs::rename(&temp, path)?;
        std::fs::File::open(root)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}

pub(super) fn parse_identity(pid: u32, stat: &str) -> Result<Option<ProcessIdentity>> {
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .ok_or_else(|| Error::Config("invalid process stat".into()))?
        .1
        .split_whitespace()
        .collect();
    if fields.first() == Some(&"Z") {
        return Ok(None);
    }
    let start_ticks = fields
        .get(19)
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| Error::Config("missing process start time".into()))?;
    Ok(Some(ProcessIdentity { pid, start_ticks }))
}

pub(super) fn local_identity(pid: u32) -> Result<Option<ProcessIdentity>> {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => parse_identity(pid, &stat),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
