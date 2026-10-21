// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable publication; recovery never learns replacement keys from discovery.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

use super::{invalid, MemberTrust};
use crate::error::Result;

const MAX_BYTES: u64 = 1024 * 1024;

impl MemberTrust {
    /// # Errors
    /// Rejects missing, corrupt, exposed, oversized and symlinked trust records.
    pub fn load(path: &Path) -> Result<Self> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o777 != 0o600 || metadata.len() > MAX_BYTES
        {
            return Err(invalid("invalid durable member trust file"));
        }
        let mut bytes = Vec::new();
        File::open(path)?.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("member trust too large"));
        }
        let trust: Self = serde_json::from_slice(&bytes).map_err(|_| invalid("corrupt member trust"))?;
        trust.validate()?;
        Ok(trust)
    }

    /// Seal authenticated inputs once; concurrent callers must agree exactly.
    ///
    /// # Errors
    /// Rejects changed pins, invalid files and publication failures.
    pub fn seal(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let mut canonical = self.clone();
        canonical.members.sort_by_key(|member| member.node_id);
        let bytes = serde_json::to_vec(&canonical).map_err(|_| invalid("invalid member trust"))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(invalid("member trust too large"));
        }
        let parent = path
            .parent()
            .ok_or_else(|| invalid("member trust has no parent"))?;
        let temporary = parent.join(format!(".member-trust-{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            match fs::hard_link(&temporary, path) {
                Ok(()) => File::open(parent)?.sync_all()?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            if Self::load(path)? != canonical {
                return Err(invalid("member trust differs from accepted pins"));
            }
            Ok(())
        })();
        let cleanup = fs::remove_file(temporary);
        result?;
        cleanup?;
        Ok(())
    }
}
