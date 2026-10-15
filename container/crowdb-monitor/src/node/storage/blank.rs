// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Full read-only blank verification while retaining the exact medium descriptor.

use super::{discover_disks, DiskBackend, DiskInput};
use crowdb_protocol::disk_storage::DiskStorageRecord;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Seek, SeekFrom},
    os::unix::fs::{FileExt, FileTypeExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

pub(crate) struct VerifiedMedium {
    file: File,
    path: PathBuf,
    backend: DiskBackend,
    identity: String,
    capacity: u64,
}

impl VerifiedMedium {
    /// Keep this object alive until Init publication; raw devices stay claimed.
    pub(crate) fn unchanged(&mut self) -> io::Result<()> {
        let opened = self.file.metadata()?;
        let current = fs::symlink_metadata(&self.path)?;
        let opened_identity = identity(&opened, self.backend)?;
        let path_identity = identity(&current, self.backend)?;
        let size = match self.backend {
            DiskBackend::File => opened.len(),
            DiskBackend::BlockDevice => self.file.seek(SeekFrom::End(0))?,
        };
        if opened_identity != self.identity || path_identity != self.identity || size != self.capacity {
            return Err(io::Error::other("medium changed during blank verification"));
        }
        Ok(())
    }
}

pub(crate) fn verify(
    root: &Path,
    supplied: &[DiskInput],
    record: &DiskStorageRecord,
    mut progress: impl FnMut(u64) -> io::Result<()>,
) -> io::Result<VerifiedMedium> {
    observe(root, supplied, record)?;
    let backing = record
        .physical_backing
        .as_ref()
        .ok_or_else(|| io::Error::other("physical lineage missing"))?;
    let flags = rustix::fs::OFlags::NOFOLLOW
        | if record.backend == DiskBackend::BlockDevice {
            rustix::fs::OFlags::EXCL
        } else {
            rustix::fs::OFlags::empty()
        };
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(i32::try_from(flags.bits()).map_err(io::Error::other)?)
        .open(&record.geometry.device_path)?;
    let mut medium = VerifiedMedium {
        file,
        path: PathBuf::from(&record.geometry.device_path),
        backend: record.backend,
        identity: record.backing_identity.clone(),
        capacity: backing.capacity_bytes,
    };
    medium.unchanged()?;
    let logical = record.geometry.capacity_units * u64::from(record.geometry.unit_size_bytes);
    let mut buffer = vec![0; 1024 * 1024];
    let mut offset = 0;
    progress(0)?;
    while offset < logical {
        let length =
            usize::try_from((logical - offset).min(buffer.len() as u64)).map_err(io::Error::other)?;
        medium.file.read_exact_at(&mut buffer[..length], offset)?;
        if let Some(index) = buffer[..length].iter().position(|byte| *byte != 0) {
            return Err(io::Error::other(format!(
                "medium is not blank at byte {}; wipe/import is not supported",
                offset + index as u64
            )));
        }
        offset += length as u64;
        progress(offset)?;
    }
    medium.unchanged()?;
    Ok(medium)
}

pub(crate) fn observe(root: &Path, supplied: &[DiskInput], record: &DiskStorageRecord) -> io::Result<()> {
    record.validate().map_err(io::Error::other)?;
    let backing = record
        .physical_backing
        .as_ref()
        .ok_or_else(|| io::Error::other("blank verification requires verified physical lineage"))?;
    if record.removed || record.geometry.status != crowdb_protocol::common::HwStatus::Init as i32 {
        return Err(io::Error::other("blank verification requires fresh Init intent"));
    }
    let observed = discover_disks(root, supplied)?
        .disks
        .into_iter()
        .find(|disk| disk.path == record.geometry.device_path)
        .ok_or_else(|| io::Error::other("selected medium is no longer discovered"))?;
    if !observed.supplied
        || observed.error.is_some()
        || observed.backend != Some(record.backend)
        || observed.backing_identity.as_ref() != Some(&record.backing_identity)
        || observed.resource_identity.as_ref() != Some(&backing.resource_identity)
        || observed.capacity_bytes != Some(backing.capacity_bytes)
    {
        return Err(io::Error::other(
            "selected medium identity, lineage or capacity changed",
        ));
    }
    Ok(())
}

fn identity(metadata: &fs::Metadata, backend: DiskBackend) -> io::Result<String> {
    match backend {
        DiskBackend::File if metadata.is_file() => Ok(format!("file:{}:{}", metadata.dev(), metadata.ino())),
        DiskBackend::BlockDevice if metadata.file_type().is_block_device() => {
            Ok(format!("device:{}", metadata.rdev()))
        }
        _ => Err(io::Error::other("selected medium type changed")),
    }
}

#[cfg(feature = "test-util")]
/// # Errors
/// Propagates exact identity, full usable-byte verification and cancellation errors.
pub fn verify_blank_medium_for_tests(
    root: &Path,
    supplied: &[DiskInput],
    record: &DiskStorageRecord,
    progress: impl FnMut(u64) -> io::Result<()>,
) -> io::Result<()> {
    verify(root, supplied, record, progress)?.unchanged()
}
