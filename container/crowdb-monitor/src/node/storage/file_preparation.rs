// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable ownership of a generated file, before Group-0 admission.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use crowdb_protocol::mgmt::node::storage::{FilePreparation, FilePreparationPhase, PrepareDiskFile};

use super::super::durable;

fn operation_id(value: &str) -> io::Result<uuid::Uuid> {
    let id = uuid::Uuid::parse_str(value).map_err(io::Error::other)?;
    if id.to_string() != value {
        return Err(io::Error::other("file operation ID must be a canonical UUID"));
    }
    Ok(id)
}

fn state_path(root: &Path, id: &str) -> io::Result<PathBuf> {
    Ok(root.join(format!("disk-file-{}.json", operation_id(id)?)))
}

pub(crate) fn status(root: &Path, id: &str) -> io::Result<FilePreparation> {
    let state: FilePreparation = durable::read(&state_path(root, id)?)?
        .ok_or_else(|| io::Error::other("file preparation does not exist"))?;
    let publication = matches!(
        state.phase,
        FilePreparationPhase::Publishing
            | FilePreparationPhase::WaitingForDiskIO
            | FilePreparationPhase::Registered
            | FilePreparationPhase::Unknown
    );
    if state.version != 1
        || state.request.operation_id != id
        || Path::new(&state.path) != disk_path(root, id)?
        || (state.preparation_complete && state.backing_identity.is_none())
        || (publication && (state.request.admission.is_none() || !state.preparation_complete))
        || (matches!(
            state.phase,
            FilePreparationPhase::Prepared
                | FilePreparationPhase::Publishing
                | FilePreparationPhase::WaitingForDiskIO
                | FilePreparationPhase::Registered
        ) && !state.preparation_complete)
    {
        return Err(io::Error::other("invalid file preparation record"));
    }
    validate(&state.request)?;
    Ok(state)
}

fn disk_path(root: &Path, id: &str) -> io::Result<PathBuf> {
    Ok(root
        .join("disks")
        .join(format!("managed-{}.img", operation_id(id)?)))
}

fn validate(request: &PrepareDiskFile) -> io::Result<()> {
    operation_id(&request.operation_id)?;
    if request.cluster_id.is_empty()
        || request.capacity_bytes == 0
        || request.capacity_bytes > i64::MAX as u64
        || request.capacity_bytes % 4096 != 0
    {
        return Err(io::Error::other("invalid file preparation identity or capacity"));
    }
    if let Some(admission) = &request.admission {
        if admission.unit_size_bytes < 4096
            || !admission.unit_size_bytes.is_power_of_two()
            || admission.zone_size_bytes == 0
            || admission.zone_size_bytes % u64::from(admission.unit_size_bytes) != 0
            || request.capacity_bytes % u64::from(admission.unit_size_bytes) != 0
            || request.capacity_bytes < admission.zone_size_bytes
            || request.capacity_bytes / admission.zone_size_bytes > u64::from(u32::MAX)
            || admission
                .media_type
                .is_some_and(|media| crowdb_protocol::diskdb::rpc::DiskType::try_from(media).is_err())
        {
            return Err(io::Error::other("invalid file admission geometry"));
        }
    }
    Ok(())
}

pub(crate) fn accept(root: &Path, request: &PrepareDiskFile) -> io::Result<FilePreparation> {
    validate(request)?;
    let path = state_path(root, &request.operation_id)?;
    if path.try_exists()? {
        let current = status(root, &request.operation_id)?;
        if current.request != *request {
            return Err(io::Error::other(
                "file preparation inputs differ from accepted intent",
            ));
        }
        return Ok(current);
    }
    let state = FilePreparation {
        version: 1,
        request: request.clone(),
        path: disk_path(root, &request.operation_id)?
            .to_string_lossy()
            .into_owned(),
        phase: FilePreparationPhase::Accepted,
        backing_identity: None,
        preparation_complete: false,
        error: None,
    };
    durable::write(&path, &state)?;
    Ok(state)
}

pub(crate) fn execute(root: &Path, id: &str) -> io::Result<FilePreparation> {
    let mut state = status(root, id)?;
    let result = prepare(root, &mut state);
    match result {
        Ok(()) => {
            state.preparation_complete = true;
            state.phase = FilePreparationPhase::Prepared;
            state.error = None;
        }
        Err(error) => {
            state.phase = FilePreparationPhase::Failed;
            state.error = Some(error.to_string());
        }
    }
    durable::write(&state_path(root, id)?, &state)?;
    Ok(state)
}

pub(crate) fn pending(root: &Path, id: &str) -> io::Result<FilePreparation> {
    let mut state = status(root, id)?;
    state.phase = FilePreparationPhase::Accepted;
    state.error = None;
    durable::write(&state_path(root, id)?, &state)?;
    Ok(state)
}

pub(crate) fn outcome(
    root: &Path,
    id: &str,
    phase: FilePreparationPhase,
    error: Option<String>,
) -> io::Result<()> {
    let mut state = status(root, id)?;
    state.phase = phase;
    state.error = error;
    durable::write(&state_path(root, id)?, &state)
}

fn directory(root: &Path) -> io::Result<File> {
    let path = root.join("disks");
    match fs::create_dir(&path) {
        Ok(()) => File::open(root)?.sync_all()?,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let flags = rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::DIRECTORY;
    OpenOptions::new()
        .read(true)
        .custom_flags(i32::try_from(flags.bits()).map_err(io::Error::other)?)
        .open(path)
}

fn identity(metadata: &fs::Metadata) -> String {
    format!("file:{}:{}", metadata.dev(), metadata.ino())
}

fn prepare(root: &Path, state: &mut FilePreparation) -> io::Result<()> {
    let directory = directory(root)?;
    let flags = rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(i32::try_from(flags.bits()).map_err(io::Error::other)?);
    if state.backing_identity.is_none() {
        check_space(&directory, &state.request, 0)?;
        // Existing bytes without our durable identity are never adopted, even
        // after a crash between create_new and ownership publication.
        options.create_new(true);
    }
    let file = options.open(&state.path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(io::Error::other(
            "managed disk must be a regular file with one link",
        ));
    }
    let actual = identity(&metadata);
    if let Some(expected) = &state.backing_identity {
        if *expected != actual {
            return Err(io::Error::other("managed disk backing identity changed"));
        }
    } else {
        file.sync_all()?;
        directory.sync_all()?;
        state.backing_identity = Some(actual);
        state.phase = FilePreparationPhase::Creating;
        durable::write(&state_path(root, &state.request.operation_id)?, state)?;
    }
    if state.preparation_complete {
        return check_size(&file, state.request.capacity_bytes);
    }
    if metadata.len() > state.request.capacity_bytes {
        return Err(io::Error::other("managed disk exceeds requested capacity"));
    }
    check_space(&directory, &state.request, metadata.blocks().saturating_mul(512))?;
    if state.request.sparse {
        file.set_len(state.request.capacity_bytes)?;
    } else {
        rustix::fs::fallocate(
            &file,
            rustix::fs::FallocateFlags::empty(),
            0,
            state.request.capacity_bytes,
        )?;
    }
    file.sync_all()?;
    check_size(&file, state.request.capacity_bytes)
}

fn check_size(file: &File, capacity: u64) -> io::Result<()> {
    if file.metadata()?.len() != capacity {
        return Err(io::Error::other("managed disk capacity changed"));
    }
    Ok(())
}

fn check_space(directory: &File, request: &PrepareDiskFile, allocated: u64) -> io::Result<()> {
    let stat = rustix::fs::fstatvfs(directory)?;
    let available = stat
        .f_bavail
        .checked_mul(stat.f_frsize)
        .filter(|_| stat.f_frsize != 0)
        .ok_or_else(|| io::Error::other("backing filesystem available space is unknown"))?;
    let additional = if request.sparse {
        0
    } else {
        request.capacity_bytes.saturating_sub(allocated)
    };
    let required = additional
        .checked_add(request.reserve_bytes)
        .ok_or_else(|| io::Error::other("requested file space exceeds bounds"))?;
    if available < required {
        return Err(io::Error::other(
            "insufficient backing space including host reserve",
        ));
    }
    Ok(())
}

#[cfg(feature = "test-util")]
/// # Errors
/// Propagates production intent validation and durable write failures.
pub fn accept_file_for_tests(root: &Path, request: &PrepareDiskFile) -> io::Result<FilePreparation> {
    accept(root, request)
}

#[cfg(feature = "test-util")]
/// # Errors
/// Propagates production state read/write failures; file errors remain in status.
pub fn prepare_file_for_tests(root: &Path, id: &str) -> io::Result<FilePreparation> {
    execute(root, id)
}
