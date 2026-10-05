// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Local S3 mini-cluster process inputs and runtime identity, without topology.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::config::{ConsoleConfig, LocalLaunchSpec, ServerEntry, ServiceType};
use crate::error::{Error, Result};

const FILE: &str = "s3-local-state.toml";
const VERSION: u32 = 1;
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalState {
    version: u32,
    group0_seeds: Vec<String>,
    #[serde(rename = "service")]
    services: Vec<ServerEntry>,
    #[serde(default)]
    local_launches: BTreeMap<String, LocalLaunchSpec>,
}

pub(super) fn path(data_dir: &Path) -> PathBuf {
    data_dir.join(FILE)
}

pub(super) fn load(data_dir: &Path) -> Result<(ConsoleConfig, Vec<String>)> {
    let body = fs::read_to_string(path(data_dir))?;
    let state: LocalState = toml::from_str(&body).map_err(|error| Error::Config(error.to_string()))?;
    state.validate()?;
    Ok((
        ConsoleConfig {
            servers: state.services,
            local_launches: state.local_launches,
            ..ConsoleConfig::default()
        },
        state.group0_seeds,
    ))
}

pub(super) fn save(data_dir: &Path, config: &ConsoleConfig) -> Result<()> {
    let group0_seeds: Vec<_> = config
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::PaxosKv)
        .map(|server| server.url.clone())
        .collect();
    let state = LocalState {
        version: VERSION,
        group0_seeds,
        services: config.servers.clone(),
        local_launches: config.local_launches.clone(),
    };
    state.validate()?;
    let body = toml::to_string_pretty(&state).map_err(|error| Error::Config(error.to_string()))?;
    let destination = path(data_dir);
    let temporary = destination.with_extension(format!(
        "tmp.{}.{}",
        std::process::id(),
        NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, &destination)?;
        fs::File::open(data_dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

impl LocalState {
    fn validate(&self) -> Result<()> {
        let expected_seeds: Vec<_> = self
            .services
            .iter()
            .filter(|service| service.service_type == ServiceType::PaxosKv)
            .map(|service| service.url.clone())
            .collect();
        if self.version != VERSION
            || expected_seeds.is_empty()
            || expected_seeds != self.group0_seeds
            || self.group0_seeds.iter().any(String::is_empty)
        {
            return Err(Error::Config(
                "S3 local state version or Group 0 seeds are invalid".into(),
            ));
        }
        let mut identities = HashSet::new();
        if self
            .services
            .iter()
            .any(|service| !identities.insert(&service.id))
            || self
                .local_launches
                .values()
                .any(|launch| launch.env.contains_key("CROWDB_S3_MASTER_KEY"))
        {
            return Err(Error::Config(
                "S3 local state has duplicate services or inline secrets".into(),
            ));
        }
        Ok(())
    }
}
