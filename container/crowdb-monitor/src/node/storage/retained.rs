// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read existing bytes through an exact-identity descriptor, without initializing.

use super::{discover_disks, DiskBackend, DiskInput};
use crowdb_protocol::{disk_storage::DiskStorageRecord, mgmt::node::storage::RetainedMedium, DiskIdExt};
use std::{
    fs::OpenOptions,
    io::{self, Seek, SeekFrom},
    os::unix::fs::{FileExt, FileTypeExt, MetadataExt, OpenOptionsExt},
    path::Path,
};

pub(crate) fn check(
    root: &Path,
    supplied: &[DiskInput],
    record: &DiskStorageRecord,
) -> io::Result<RetainedMedium> {
    record.validate().map_err(io::Error::other)?;
    let path = Path::new(&record.geometry.device_path);
    if path.parent() != Some(root.join("disks").as_path()) {
        return Err(io::Error::other(
            "retained medium is outside the node disk directory",
        ));
    }
    let inventory = discover_disks(root, supplied)?;
    let observed = inventory
        .disks
        .iter()
        .find(|disk| disk.path == record.geometry.device_path)
        .ok_or_else(|| io::Error::other("retained medium is missing from discovery"))?;
    let capacity = record
        .physical_capacity_bytes()
        .ok_or_else(|| io::Error::other("retained capacity overflows"))?;
    if observed.error.is_some()
        || observed.backend != Some(record.backend)
        || observed.backing_identity.as_ref() != Some(&record.backing_identity)
        || observed.capacity_bytes != Some(capacity)
        || record
            .physical_backing
            .as_ref()
            .is_some_and(|backing| observed.resource_identity.as_ref() != Some(&backing.resource_identity))
    {
        return Err(io::Error::other(format!(
            "retained medium observation conflicts: {}",
            observed
                .error
                .as_deref()
                .unwrap_or("identity, type or capacity changed")
        )));
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(i32::try_from(rustix::fs::OFlags::NOFOLLOW.bits()).map_err(io::Error::other)?)
        .open(path)?;
    let opened = file.metadata()?;
    let (identity, size) = match record.backend {
        DiskBackend::File if opened.is_file() => {
            (format!("file:{}:{}", opened.dev(), opened.ino()), opened.len())
        }
        DiskBackend::BlockDevice if opened.file_type().is_block_device() => {
            (format!("device:{}", opened.rdev()), file.seek(SeekFrom::End(0))?)
        }
        _ => return Err(io::Error::other("opened retained medium type changed")),
    };
    if identity != record.backing_identity || size != capacity || size < 4096 {
        return Err(io::Error::other(
            "opened retained medium identity or geometry changed",
        ));
    }
    let mut buffer = [0_u8; 4096];
    file.read_exact_at(&mut buffer, 0)?;
    file.read_exact_at(&mut buffer, size - 4096)?;
    Ok(RetainedMedium {
        version: 1,
        disk_id: record.disk_id.to_display_string(),
        backend: record.backend,
        backing_identity: identity,
        capacity_bytes: capacity,
        observed_at_ms: inventory.observed_at_ms,
    })
}

#[cfg(feature = "test-util")]
/// # Errors
/// Rejects unavailable, changed, aliased or unreadable retained media.
pub fn check_retained_medium_for_tests(
    root: &Path,
    supplied: &[DiskInput],
    record: &DiskStorageRecord,
) -> io::Result<RetainedMedium> {
    check(root, supplied, record)
}
