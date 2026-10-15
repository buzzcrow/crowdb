// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only media discovery; Group 0 remains the admission authority.

pub(crate) mod file_operations;
pub(crate) mod file_preparation;
pub(crate) mod retained;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Seek, SeekFrom};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_protocol::mgmt::node::storage::{
    BackingFilesystem, DiskBackend, DiskInput, DiskInventory, DiskObservation,
};

const MAX_DISKS: usize = 4096;

/// Inspect direct children without creating disks or granting membership.
///
/// # Errors
/// Rejects an invalid root, malformed supplied paths, and unbounded inventory.
pub fn discover_disks(root: &Path, supplied: &[DiskInput]) -> io::Result<DiskInventory> {
    let disk_root = root.join("disks");
    let mut inputs = BTreeMap::new();
    for input in supplied {
        let path = Path::new(&input.path);
        if path.parent() != Some(disk_root.as_path()) || inputs.insert(path.to_path_buf(), input).is_some() {
            return Err(io::Error::other("invalid or duplicate supplied disk path"));
        }
    }
    let mut paths = inputs.keys().cloned().collect::<BTreeSet<_>>();
    match fs::symlink_metadata(&disk_root) {
        Ok(metadata) if metadata.is_dir() => {
            for entry in fs::read_dir(&disk_root)? {
                paths.insert(entry?.path());
                if paths.len() > MAX_DISKS {
                    return Err(io::Error::other("disk inventory exceeds limit"));
                }
            }
        }
        Ok(_) => return Err(io::Error::other("disk root is not a directory")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    if paths.len() > MAX_DISKS {
        return Err(io::Error::other("disk inventory exceeds limit"));
    }
    let mut identities = BTreeMap::<String, usize>::new();
    let mut filesystems = BTreeMap::new();
    let mut disks: Vec<_> = paths
        .into_iter()
        .map(|path| {
            let expected = inputs.get(&path);
            let mut observation = observe(&path, expected.is_some(), &mut filesystems);
            if let Some(input) = expected {
                if observation.error.is_none() && observation.backend != Some(input.backend) {
                    observation.error = Some("disk type differs from supplied mapping".into());
                }
                if observation.error.is_none()
                    && (input.backing_identity.is_none()
                        || observation.backing_identity != input.backing_identity)
                {
                    observation.error =
                        Some("disk identity differs from supplied mapping or is unverified".into());
                }
            }
            if let Some(identity) = &observation.backing_identity {
                *identities.entry(identity.clone()).or_default() += 1;
            }
            observation
        })
        .collect();
    for disk in &mut disks {
        if disk
            .backing_identity
            .as_ref()
            .is_some_and(|identity| identities[identity] > 1)
        {
            disk.error = Some("duplicate underlying disk resource".into());
        }
    }
    let inventory = DiskInventory {
        observed_at_ms: u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX),
        disks,
        filesystems: filesystems.into_values().collect(),
    };
    // Leave room for the control response envelope within its 64 KiB limit.
    if serde_json::to_vec(&inventory).map_err(io::Error::other)?.len() > 60 * 1024 {
        return Err(io::Error::other("disk inventory exceeds control response limit"));
    }
    Ok(inventory)
}

fn observe(
    path: &Path,
    supplied: bool,
    filesystems: &mut BTreeMap<String, BackingFilesystem>,
) -> DiskObservation {
    let mut result = DiskObservation {
        path: path.to_string_lossy().into_owned(),
        backend: None,
        capacity_bytes: None,
        allocated_bytes: None,
        backing_identity: None,
        supplied,
        error: None,
        backing_filesystem: None,
    };
    if let Err(error) = inspect(path, &mut result, filesystems) {
        result.error = Some(error.to_string());
    }
    result
}

fn inspect(
    path: &Path,
    result: &mut DiskObservation,
    filesystems: &mut BTreeMap<String, BackingFilesystem>,
) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_file() {
        result.backend = Some(DiskBackend::File);
        result.capacity_bytes = Some(metadata.len());
        result.allocated_bytes = Some(metadata.blocks().saturating_mul(512));
        result.backing_identity = Some(format!("file:{}:{}", metadata.dev(), metadata.ino()));
        if metadata.len() == 0 {
            return Err(io::Error::other("disk file is empty"));
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(i32::try_from(rustix::fs::OFlags::NOFOLLOW.bits()).map_err(io::Error::other)?)
            .open(path)?;
        let opened = file.metadata()?;
        if !opened.is_file() || opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
            return Err(io::Error::other("disk changed during observation"));
        }
        let identity = format!("filesystem:{}", opened.dev());
        result.backing_filesystem = Some(identity.clone());
        filesystems
            .entry(identity.clone())
            .or_insert_with(|| filesystem(&file, identity));
    } else if metadata.file_type().is_block_device() {
        result.backend = Some(DiskBackend::BlockDevice);
        result.backing_identity = Some(format!("device:{}", metadata.rdev()));
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(i32::try_from(rustix::fs::OFlags::NOFOLLOW.bits()).map_err(io::Error::other)?)
            .open(path)?;
        let opened = file.metadata()?;
        if !opened.file_type().is_block_device() || opened.rdev() != metadata.rdev() {
            return Err(io::Error::other("disk changed during observation"));
        }
        result.capacity_bytes = Some(file.seek(SeekFrom::End(0))?);
    } else {
        return Err(io::Error::other(
            "expected a regular file or block device, not a symlink or directory",
        ));
    }
    Ok(())
}

fn filesystem(file: &fs::File, identity: String) -> BackingFilesystem {
    let mut result = BackingFilesystem {
        identity,
        capacity_bytes: None,
        available_bytes: None,
        error: None,
    };
    match rustix::fs::fstatvfs(file) {
        Ok(stat) => {
            result.capacity_bytes = stat.f_blocks.checked_mul(stat.f_frsize);
            result.available_bytes = stat.f_bavail.checked_mul(stat.f_frsize);
            if result.capacity_bytes.is_none() || result.available_bytes.is_none() {
                result.error = Some("backing filesystem size exceeds observation bounds".into());
            }
        }
        Err(error) => result.error = Some(error.to_string()),
    }
    result
}
