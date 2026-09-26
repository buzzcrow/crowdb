use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const VERSION: u32 = 1;
const MAX_BYTES: u64 = 64 * 1024;

#[derive(Debug, Error)]
pub enum StatusError {
    #[error("monitor status storage failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("monitor status cannot be decoded: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("monitor status is invalid")]
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorPhase {
    Initializing,
    Ready,
    Restarting,
    Draining,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceStatus {
    pub pid: Option<u32>,
    pub generation: u64,
    pub healthy: bool,
    pub restart_attempts: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorStatus {
    pub version: u32,
    pub deployment_id: Uuid,
    pub monitor_pid: u32,
    pub revision: u64,
    pub updated_at_ms: u64,
    pub phase: MonitorPhase,
    pub services: BTreeMap<String, ServiceStatus>,
}

impl MonitorStatus {
    #[must_use]
    pub fn new(deployment_id: Uuid, phase: MonitorPhase) -> Self {
        Self {
            version: VERSION,
            deployment_id,
            monitor_pid: std::process::id(),
            revision: 0,
            updated_at_ms: 0,
            phase,
            services: BTreeMap::new(),
        }
    }
}

pub struct StatusStore {
    path: PathBuf,
}

impl StatusStore {
    /// # Errors
    /// Rejects missing, symlinked, or non-directory runtime roots.
    pub fn new(run_root: &Path) -> Result<Self, StatusError> {
        if !fs::symlink_metadata(run_root)?.file_type().is_dir() {
            return Err(StatusError::Invalid);
        }
        let directory = run_root.join("status");
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if !metadata.file_type().is_dir() => return Err(StatusError::Invalid),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(&directory)?,
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            path: directory.join("monitor.json"),
        })
    }

    /// # Errors
    /// Rejects missing or symlinked status directories without creating them.
    pub fn open(run_root: &Path) -> Result<Self, StatusError> {
        let directory = run_root.join("status");
        if !fs::symlink_metadata(&directory)?.file_type().is_dir() {
            return Err(StatusError::Invalid);
        }
        Ok(Self {
            path: directory.join("monitor.json"),
        })
    }

    /// # Errors
    /// Returns failed durable writes or an invalid deployment identity.
    pub fn publish(&self, status: &mut MonitorStatus) -> Result<(), StatusError> {
        if status.deployment_id.is_nil() || status.monitor_pid == 0 {
            return Err(StatusError::Invalid);
        }
        let mut updated = status.clone();
        updated.revision = updated.revision.checked_add(1).ok_or(StatusError::Invalid)?;
        updated.updated_at_ms = now_ms()?;
        let bytes = serde_json::to_vec(&updated)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(StatusError::Invalid);
        }
        atomic_write(&self.path, &bytes)?;
        *status = updated;
        Ok(())
    }

    /// # Errors
    /// Rejects missing, corrupt, stale, or incompatible status snapshots.
    pub fn read(&self, max_age: Duration) -> Result<MonitorStatus, StatusError> {
        let metadata = fs::symlink_metadata(&self.path)?;
        if !metadata.file_type().is_file() || metadata.len() > MAX_BYTES {
            return Err(StatusError::Invalid);
        }
        let status: MonitorStatus = serde_json::from_slice(&fs::read(&self.path)?)?;
        let age = now_ms()?
            .checked_sub(status.updated_at_ms)
            .ok_or(StatusError::Invalid)?;
        if status.version != VERSION
            || status.deployment_id.is_nil()
            || status.monitor_pid == 0
            || status.revision == 0
            || age > max_age.as_millis().try_into().unwrap_or(u64::MAX)
        {
            return Err(StatusError::Invalid);
        }
        Ok(status)
    }

    /// # Errors
    /// Rejects stale or non-ready status or any unhealthy child.
    pub fn readiness(&self, max_age: Duration) -> Result<(), StatusError> {
        let status = self.read(max_age)?;
        if status.phase != MonitorPhase::Ready
            || status.services.is_empty()
            || status
                .services
                .values()
                .any(|service| service.pid.is_none() || !service.healthy)
        {
            return Err(StatusError::Invalid);
        }
        Ok(())
    }
}

fn now_ms() -> Result<u64, StatusError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StatusError::Invalid)?
        .as_millis()
        .try_into()
        .map_err(|_| StatusError::Invalid)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StatusError> {
    let directory = path.parent().ok_or(StatusError::Invalid)?;
    let temporary = directory.join(format!(".monitor-{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(StatusError::Io)
}
