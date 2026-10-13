// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use thiserror::Error;
use uuid::Uuid;

const IDENTITY_FILE: &str = "node-identity";
const MAX_BYTES: u64 = 64;

#[derive(Debug, Error)]
pub enum NodeIdentityError {
    #[error("node identity storage failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("node identity is invalid: {0}")]
    Invalid(&'static str),
}

/// Discovery identity, independent of cluster numeric node IDs and rack placement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeIdentity(Uuid);

impl NodeIdentity {
    #[must_use]
    pub fn uuid(self) -> Uuid {
        self.0
    }

    /// # Errors
    /// Rejects non-directory roots and corrupt or symlinked identity files.
    /// Concurrent initialization publishes one complete, durable identity.
    pub fn load_or_create(root: &Path) -> Result<Self, NodeIdentityError> {
        if !root.is_absolute() || !fs::symlink_metadata(root)?.file_type().is_dir() {
            return Err(NodeIdentityError::Invalid("root must be an absolute directory"));
        }
        let path = root.join(IDENTITY_FILE);
        match fs::symlink_metadata(&path) {
            Ok(_) => return Self::load(&path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let identity = Self(Uuid::new_v4());
        let temporary = root.join(format!(".node-identity-{}.tmp", identity.0));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            writeln!(file, "{}", identity.0)?;
            file.sync_all()?;
            // Publish without overwriting a concurrent initializer's identity.
            match fs::hard_link(&temporary, &path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(NodeIdentityError::Io(error)),
            }
            File::open(root)?.sync_all()?;
            Self::load(&path)
        })();
        let cleanup = fs::remove_file(&temporary);
        match result {
            Ok(identity) => {
                cleanup?;
                Ok(identity)
            }
            Err(error) => Err(error),
        }
    }

    fn load(path: &Path) -> Result<Self, NodeIdentityError> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.file_type().is_file()
            || metadata.permissions().mode() & 0o777 != 0o600
            || metadata.len() > MAX_BYTES
        {
            return Err(NodeIdentityError::Invalid(
                "expected a bounded mode-0600 regular file",
            ));
        }
        let mut text = String::new();
        File::open(path)?.take(MAX_BYTES + 1).read_to_string(&mut text)?;
        let identity =
            Uuid::parse_str(text.trim()).map_err(|_| NodeIdentityError::Invalid("expected a UUID"))?;
        if identity.get_version_num() != 4 {
            return Err(NodeIdentityError::Invalid("expected a generated UUIDv4"));
        }
        Ok(Self(identity))
    }
}
