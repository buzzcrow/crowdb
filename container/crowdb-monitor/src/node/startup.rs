// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Startup policy is durable and never inferred from discovered node count.

use std::{io, path::Path};

use serde::{Deserialize, Serialize};

use super::durable;

mod policy;
pub use policy::{accept_node_policy, read_node_policy};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Mode {
    Standard,
    Single,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Startup {
    version: u32,
    mode: Mode,
}

pub(crate) fn validate(root: &Path, mode: Mode) -> io::Result<()> {
    if !crate::layout::is_clean_absolute(root) || !std::fs::symlink_metadata(root)?.file_type().is_dir() {
        return Err(io::Error::other("startup root must be an absolute directory"));
    }
    if root.join("node-policy.json").try_exists()? {
        return Err(io::Error::other(
            "accepted node policy cannot use the obsolete startup mode",
        ));
    }
    if let Some(current) = durable::read::<Startup>(&root.join("startup-mode.json"))? {
        if current.version != 1 || current.mode != mode {
            return Err(io::Error::other("startup mode differs from durable node policy"));
        }
    }
    // Legacy single profiles are identified by their bootstrap manifest, not
    // by one-node membership. The profile loader validates its contents.
    if mode == Mode::Standard && root.join("bootstrap/manifest.json").try_exists()? {
        return Err(io::Error::other(
            "existing single-profile bootstrap requires single mode",
        ));
    }
    if mode == Mode::Standard
        && root.join("node-binding.json").try_exists()?
        && !root.join("accepted-node.json").try_exists()?
    {
        return Err(io::Error::other("bound node has no accepted standard bootstrap"));
    }
    if mode == Mode::Single
        && [
            "accepted-node.json",
            "initialization.json",
            "prepared-bootstrap.json",
        ]
        .iter()
        .try_fold(false, |found, name| {
            root.join(name).try_exists().map(|exists| found || exists)
        })?
    {
        return Err(io::Error::other(
            "existing standard bootstrap cannot switch to single mode",
        ));
    }
    Ok(())
}

#[cfg(feature = "test-util")]
pub(crate) fn accept(root: &Path, mode: Mode) -> io::Result<()> {
    validate(root, mode)?;
    let path = root.join("startup-mode.json");
    if !path.try_exists()? {
        let temporary = root.join(format!(".startup-mode-{}.json", uuid::Uuid::new_v4()));
        durable::write(&temporary, &Startup { version: 1, mode })?;
        let published = std::fs::hard_link(&temporary, &path);
        std::fs::remove_file(&temporary)?;
        match published {
            Ok(()) => std::fs::File::open(root)?.sync_all()?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    validate(root, mode)
}

/// # Errors
/// Propagates durable mode validation and write failures.
#[cfg(feature = "test-util")]
pub fn accept_startup_mode_for_tests(root: &Path, single: bool) -> io::Result<()> {
    accept(root, if single { Mode::Single } else { Mode::Standard })
}
