// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Private retention for file-based Linux core dumps.

use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub struct CrashRetention {
    root: PathBuf,
}

impl CrashRetention {
    /// Opens a private core directory and removes all but its newest core.
    ///
    /// # Errors
    /// Rejects symlinked roots and inaccessible directories or entries.
    pub fn open(root: PathBuf) -> io::Result<Self> {
        match fs::DirBuilder::new().mode(0o700).create(&root) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        if !fs::symlink_metadata(&root)?.file_type().is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "core root is not a directory",
            ));
        }
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let retention = Self { root };
        retention.prune()?;
        Ok(retention)
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Keeps the newest regular `core` or `core.*` file only.
    ///
    /// # Errors
    /// Returns a directory or removal error without exposing dump contents.
    pub fn prune(&self) -> io::Result<()> {
        let mut cores = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !core_name(&entry.file_name()) || !entry.file_type()?.is_file() {
                continue;
            }
            let modified = entry.metadata()?.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            cores.push((modified, entry.file_name(), entry.path()));
        }
        cores.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));
        for (_, _, path) in cores.into_iter().skip(1) {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

fn core_name(name: &OsStr) -> bool {
    name == "core" || name.as_encoded_bytes().starts_with(b"core.")
}
