// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::error::{Error, Result};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

pub(super) const FILE: &str = "container-cluster.json";
pub(super) const INITIALIZING: &str = "s3-mini-cluster.initializing.json";
pub(super) const LABEL: &str = "org.crowdb.cluster.id";
pub(super) const ROOT_LABEL: &str = "org.crowdb.cluster.root";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub version: u32,
    pub id: String,
    pub root: PathBuf,
    pub image: String,
    pub name: String,
    pub network: String,
    pub phase: String,
    pub node_cpus: f64,
    pub memory_mib: u64,
    pub addresses: Vec<String>,
}

impl State {
    pub fn node_name(&self, index: usize) -> String {
        if index == 0 {
            self.name.clone()
        } else {
            format!("{}-node{}", self.name, index + 1)
        }
    }

    pub fn data(&self, index: usize) -> PathBuf {
        self.root.join(format!("nodes/node{index}/data"))
    }

    pub fn web(&self) -> Result<String> {
        self.addresses
            .first()
            .map(|address| format!("http://{address}:9090"))
            .ok_or_else(|| Error::Config("container cluster has no inspected endpoint".into()))
    }

    pub fn load(root: &Path) -> Result<Self> {
        let state: Self = serde_json::from_slice(&fs::read(root.join(FILE))?)
            .map_err(|error| Error::Config(error.to_string()))?;
        state.validate(root)?;
        Ok(state)
    }

    pub fn validate(&self, root: &Path) -> Result<()> {
        if self.version != 1
            || self.id.len() != 32
            || !self.id.bytes().all(|b| b.is_ascii_hexdigit())
            || self.root != fs::canonicalize(root)?
            || self.root.parent().is_none()
            || !self.image.starts_with("sha256:")
            || self.image.len() != 71
            || !self.image[7..].bytes().all(|b| b.is_ascii_hexdigit())
            || self.network != format!("{}-network", self.name)
            || !self.node_cpus.is_finite()
            || self.node_cpus <= 0.0
            || self.memory_mib < 512
            || (!self.addresses.is_empty()
                && (self.addresses.len() != 3
                    || self
                        .addresses
                        .iter()
                        .any(|ip| ip.parse::<std::net::Ipv4Addr>().is_err())))
        {
            return Err(Error::Config(
                "container cluster identity, ownership or resources are invalid".into(),
            ));
        }
        for index in 0..3 {
            for path in [
                self.root.join("nodes"),
                self.root.join(format!("nodes/node{index}")),
                self.data(index),
                self.data(index).join("disk.img"),
            ] {
                if fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
                    return Err(Error::Config(
                        "cluster data paths cannot be symbolic links".into(),
                    ));
                }
            }
        }
        validate_name(&self.name)
    }

    pub fn save(&self) -> Result<()> {
        self.validate(&self.root)?;
        atomic(
            &self.root.join(FILE),
            &self.id,
            &serde_json::to_vec_pretty(self).map_err(|e| Error::Config(e.to_string()))?,
        )?;
        if self.phase != "ready" && self.phase != "stopped" {
            atomic(
                &self.root.join(INITIALIZING),
                &self.id,
                &serde_json::to_vec_pretty(self).map_err(|e| Error::Config(e.to_string()))?,
            )?;
        }
        Ok(())
    }

    pub fn client_record(&self) -> Result<()> {
        let web = self.web()?;
        let record = crowdb_console_shared::ops::s3::MiniClusterRecord {
            version: 1,
            protected_test: true,
            endpoint: format!("{web}/api/access/s3"),
            web_endpoint: web,
            web_pid: None,
            tenant: "default".into(),
            storage_profile: crowdb_console_shared::ops::s3::StorageProfile::Persistent,
        };
        atomic(
            &self.root.join("s3-mini-cluster.json"),
            &self.id,
            &serde_json::to_vec_pretty(&record).map_err(|e| Error::Config(e.to_string()))?,
        )?;
        let origins: Vec<_> = self
            .addresses
            .iter()
            .map(|ip| format!("http://{ip}:10000"))
            .collect();
        let mut body = format!(
            "version = 1\ngroup0_seeds = {}\n",
            serde_json::to_string(&origins).map_err(|e| Error::Config(e.to_string()))?
        );
        for (index, origin) in origins.iter().enumerate() {
            writeln!(body, "\n[[service]]\nid = \"kv-{}\"\nurl = \"{origin}\"\nnode_id = {}\nservice_type = \"paxos-kv\"\nauto_start = false", index + 1, index + 1)
                .map_err(|error| Error::Config(error.to_string()))?;
        }
        atomic(&self.root.join("s3-local-state.toml"), &self.id, body.as_bytes())
    }
}

pub(super) fn random_id() -> String {
    let mut bytes = [0; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub(super) fn validate_name(name: &str) -> Result<()> {
    validate_member_name(name)?;
    if name.len() > 48 {
        return Err(Error::Validation {
            field: "container".into(),
            message: "cluster name must contain at most 48 characters".into(),
        });
    }
    Ok(())
}

pub(super) fn validate_member_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    {
        return Err(Error::Validation { field: "container".into(), message: "member container name must start with an alphanumeric character and contain at most 64 alphanumeric, dot, dash or underscore characters".into() });
    }
    Ok(())
}

pub(super) fn atomic(path: &Path, owner: &str, bytes: &[u8]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let parent = path
        .parent()
        .ok_or_else(|| Error::Config("cluster state parent absent".into()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::Config("cluster state filename is invalid".into()))?;
    let temporary = parent.join(format!("{name}.{owner}.{}.tmp", random_id()));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        fs::File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(Error::Io)
}

pub(super) fn remove_temporary(state: &State) -> Result<()> {
    for entry in fs::read_dir(&state.root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let owned = [FILE, INITIALIZING, "s3-mini-cluster.json", "s3-local-state.toml"]
            .iter()
            .any(|file| {
                name.strip_prefix(&format!("{file}.{}.", state.id))
                    .and_then(|nonce| nonce.strip_suffix(".tmp"))
                    .is_some_and(|nonce| nonce.len() == 32 && nonce.bytes().all(|b| b.is_ascii_hexdigit()))
            });
        if owned && entry.file_type()?.is_file() {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}
