// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Host-owned Docker launch inputs, shared with independent Pixi shortcuts.

use crowdb_console_shared::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DockerIdentity {
    pub daemon: String,
    pub context: String,
    pub endpoint: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Launch {
    pub version: u8,
    pub cluster: String,
    pub owner: String,
    pub runtime: DockerIdentity,
    pub nodes: u8,
    pub port_base: u16,
    pub image: String,
    pub image_id: String,
    pub cpus: f64,
    pub memory_mib: u64,
}

pub(super) fn directory() -> PathBuf {
    crowdb_protocol::port::namespace::runtime_root().join("persistent/oci-clusters")
}

pub(super) fn name_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 48
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

pub(super) fn path(name: &str, directory: &Path) -> Result<PathBuf> {
    if !name_valid(name) {
        return Err(invalid("invalid cluster name"));
    }
    check_path(directory)?;
    Ok(directory.join(format!("{name}.json")))
}

pub(super) fn check_path(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(invalid("cluster state path must be absolute"));
    }
    for parent in path.ancestors() {
        match fs::symlink_metadata(parent) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid("cluster state paths cannot contain symlinks"))
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub(super) fn read_json(path: &Path) -> Result<Option<serde_json::Value>> {
    check_path(path)?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 || metadata.len() > 1024 * 1024 {
        return Err(invalid("cluster receipt must be a bounded private regular file"));
    }
    let mut bytes = Vec::new();
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024 {
        return Err(invalid("cluster receipt exceeds its bound"));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| invalid("invalid cluster receipt JSON"))
}

pub(super) fn seal_json(path: &Path, value: &serde_json::Value) -> Result<()> {
    check_path(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("cluster receipt has no directory"))?;
    fs::DirBuilder::new().recursive(true).mode(0o700).create(parent)?;
    let temporary = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(value).map_err(|_| invalid("invalid cluster receipt"))?)?;
        file.sync_all()?;
        match fs::hard_link(&temporary, path) {
            Ok(()) => File::open(parent)?.sync_all()?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        if read_json(path)?.as_ref() != Some(value) {
            return Err(invalid("cluster receipt conflicts with accepted inputs"));
        }
        Ok(())
    })();
    let cleanup = fs::remove_file(temporary);
    result?;
    cleanup?;
    Ok(())
}

impl Launch {
    pub fn load(name: &str, directory: &Path) -> Result<Option<Self>> {
        let Some(value) = read_json(&path(name, directory)?)? else {
            return Ok(None);
        };
        let launch: Self =
            serde_json::from_value(value).map_err(|_| invalid("invalid Docker launch receipt fields"))?;
        launch.validate()?;
        if launch.cluster != name {
            return Err(invalid("cluster name differs from its receipt"));
        }
        Ok(Some(launch))
    }
    pub fn seal(&self, directory: &Path) -> Result<()> {
        self.validate()?;
        seal_json(
            &path(&self.cluster, directory)?,
            &serde_json::to_value(self).map_err(|_| invalid("invalid Docker launch receipt"))?,
        )
    }
    pub fn validate(&self) -> Result<()> {
        let owner =
            uuid::Uuid::parse_str(&self.owner).map_err(|_| invalid("invalid cluster ownership token"))?;
        if self.version != 1
            || !name_valid(&self.cluster)
            || owner.is_nil()
            || owner.to_string() != self.owner
            || !matches!(self.nodes, 1 | 3)
            || !(1..=u16::MAX
                - (crowdb_protocol::ACCESS_SERVER_DATASET_HTTP_BASE - crowdb_protocol::WEB_BASE)
                - 2)
                .contains(&self.port_base)
            || !self.cpus.is_finite()
            || self.cpus <= 0.0
            || self.cpus > f64::from(u32::MAX)
            || self.memory_mib < 512
            || self.memory_mib.checked_mul(1024 * 1024).is_none()
            || self.image.is_empty()
            || self.image_id.strip_prefix("sha256:").map_or(true, |digest| {
                digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            || self.runtime.daemon.is_empty()
            || self.runtime.context.is_empty()
            || !self.runtime.endpoint.starts_with("unix://")
        {
            return Err(invalid(
                "invalid local Docker launch identity, image or resources",
            ));
        }
        Ok(())
    }
    pub fn origins(&self) -> Vec<String> {
        (0..self.nodes)
            .map(|index| format!("http://127.0.0.1:{}", self.port_base + u16::from(index)))
            .collect()
    }
    pub fn resource_name(&self, kind: &str, index: u8) -> String {
        let base = format!("crowdb-{}", self.cluster);
        if kind == "network" {
            base
        } else {
            format!("{base}-{index}{}", if kind == "volume" { "-data" } else { "" })
        }
    }
}

pub(super) fn invalid(message: &str) -> Error {
    Error::Config(message.into())
}
