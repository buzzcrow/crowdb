use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use thiserror::Error;

use crate::{
    BootstrapSession, DeploymentProfile, DiskProfile, ManifestError, ManifestState, MonitorEvent,
    MonitorEventKind, MonitorLog, MonitorLogError,
};

#[derive(Debug, Error)]
pub enum DiskBootstrapError {
    #[error("disk bootstrap I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("bootstrap manifest failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("monitor lifecycle log failed: {0}")]
    MonitorLog(#[from] MonitorLogError),
    #[error("disk bootstrap state is invalid: {0}")]
    Invalid(&'static str),
}

#[must_use]
pub fn disk_step_names(profile: &DeploymentProfile) -> Vec<String> {
    let mut names = profile
        .disks
        .iter()
        .map(|disk| format!("disk-file-{}", disk.disk_id))
        .collect::<Vec<_>>();
    names.sort();
    names
}

/// # Errors
/// Rejects missing or changed files on Ready restart, and never truncates an existing disk.
pub async fn ensure_disk_files(
    session: &mut BootstrapSession,
    profile: &DeploymentProfile,
    events: &mut MonitorLog,
) -> Result<(), DiskBootstrapError> {
    let result = ensure_disk_files_inner(session, profile, events).await;
    if result.is_err() {
        events
            .record(&MonitorEvent {
                kind: MonitorEventKind::BootstrapFailed,
                service: session.manifest().next_step().or(Some("disk-files")),
                pid: None,
                attempt: None,
            })
            .await?;
    }
    result
}

async fn ensure_disk_files_inner(
    session: &mut BootstrapSession,
    profile: &DeploymentProfile,
    events: &mut MonitorLog,
) -> Result<(), DiskBootstrapError> {
    profile
        .validate()
        .map_err(|_| DiskBootstrapError::Invalid("deployment profile is invalid"))?;
    let disk_root = profile.paths.data_root.join("disks");
    match fs::symlink_metadata(&disk_root) {
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return Err(DiskBootstrapError::Invalid("disk root is not a directory"));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if session.manifest().state() == ManifestState::Ready {
                return Err(DiskBootstrapError::Invalid("ready disk root is missing"));
            }
            fs::create_dir(&disk_root)?;
            File::open(&profile.paths.data_root)?.sync_all()?;
        }
        Err(error) => return Err(error.into()),
    }
    let mut disks = profile.disks.iter().collect::<Vec<_>>();
    disks.sort_by(|left, right| left.disk_id.cmp(&right.disk_id));
    for disk in disks {
        let step = format!("disk-file-{}", disk.disk_id);
        let complete = session
            .manifest()
            .step_complete(&step)
            .ok_or(DiskBootstrapError::Invalid("disk step is absent from manifest"))?;
        if !complete {
            events
                .record(&MonitorEvent {
                    kind: MonitorEventKind::BootstrapStepStarted,
                    service: Some(&step),
                    pid: None,
                    attempt: None,
                })
                .await?;
        }
        ensure_one_disk(&disk_root, disk, complete, session.manifest().state())?;
        if !complete {
            session.complete_step(&step)?;
            events
                .record(&MonitorEvent {
                    kind: MonitorEventKind::BootstrapStepCompleted,
                    service: Some(&step),
                    pid: None,
                    attempt: None,
                })
                .await?;
        }
    }
    Ok(())
}

fn ensure_one_disk(
    disk_root: &Path,
    disk: &DiskProfile,
    complete: bool,
    state: ManifestState,
) -> Result<(), DiskBootstrapError> {
    match fs::symlink_metadata(&disk.path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() || metadata.len() != disk.capacity_bytes {
                return Err(DiskBootstrapError::Invalid(
                    "disk file type or capacity conflicts",
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if complete || state == ManifestState::Ready {
                return Err(DiskBootstrapError::Invalid("completed disk file is missing"));
            }
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&disk.path)?;
            file.set_len(disk.capacity_bytes)?;
            file.sync_all()?;
            File::open(disk_root)?.sync_all()?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
