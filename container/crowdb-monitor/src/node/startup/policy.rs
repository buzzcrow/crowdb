// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Publish one complete policy before creating identity or provisioning storage.

use std::{fs, io, path::Path};

use crowdb_protocol::mgmt::node::{
    deployment::{NodeDeploymentOverrides, NodeDeploymentPolicy},
    setup::SetupStorage,
};

use crate::node::durable;

const POLICY: &str = "node-policy.json";

/// Read accepted inputs without deriving replacement defaults.
///
/// # Errors
/// Rejects corrupt, unsupported or non-regular policy records.
pub fn read_node_policy(root: &Path) -> io::Result<Option<NodeDeploymentPolicy>> {
    let policy = durable::read::<NodeDeploymentPolicy>(&root.join(POLICY))?;
    if let Some(policy) = &policy {
        policy.validate().map_err(io::Error::other)?;
    }
    Ok(policy)
}

/// Seal inputs atomically; concurrent publishers cannot overwrite the winner.
///
/// # Errors
/// Rejects conflicting accepted inputs and unrecognized populated roots.
pub fn accept_node_policy(
    root: &Path,
    file: &NodeDeploymentOverrides,
    explicit: &NodeDeploymentOverrides,
) -> io::Result<NodeDeploymentPolicy> {
    if !crate::layout::is_clean_absolute(root) || !fs::symlink_metadata(root)?.is_dir() {
        return Err(io::Error::other("node policy root must be an absolute directory"));
    }
    let accepted = read_node_policy(root)?;
    let selected = explicit
        .resolve(file, accepted.as_ref())
        .map_err(io::Error::other)?;
    if accepted.is_some() {
        validate_retained_identity(root)?;
        return Ok(selected);
    }
    validate_fresh_root(root, &selected)?;
    let temporary = root.join(format!(".node-policy-{}.json", uuid::Uuid::new_v4()));
    durable::write(&temporary, &selected)?;
    let outcome = fs::hard_link(&temporary, root.join(POLICY));
    fs::remove_file(&temporary)?;
    match outcome {
        Ok(()) => fs::File::open(root)?.sync_all()?,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let winner = read_node_policy(root)?.ok_or_else(|| io::Error::other("accepted node policy missing"))?;
    if winner != selected {
        return Err(io::Error::other(
            "concurrent startup accepted another node policy",
        ));
    }
    Ok(winner)
}

fn validate_retained_identity(root: &Path) -> io::Result<()> {
    if root.join("node-identity").try_exists()? {
        return Ok(());
    }
    for name in [
        "accepted-node.json",
        "node-binding.json",
        "initialization.json",
        "node-setup.json",
    ] {
        if root.join(name).try_exists()? {
            return Err(io::Error::other(
                "retained deployment has no node identity; refusing replacement identity",
            ));
        }
    }
    Ok(())
}

fn validate_fresh_root(root: &Path, policy: &NodeDeploymentPolicy) -> io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| io::Error::other("unrecognized node data path"))?;
        if name == POLICY {
            // A concurrent publisher may already have completed validation.
            let current = read_node_policy(root)?.ok_or_else(|| io::Error::other("node policy missing"))?;
            if current != *policy {
                return Err(io::Error::other(
                    "concurrent startup accepted another node policy",
                ));
            }
            continue;
        }
        if temporary_policy(name) && entry.file_type()?.is_file() {
            continue;
        }
        if matches!(name, "ssh" | "disks" | "lost+found")
            && entry.file_type()?.is_dir()
            && (name != "disks"
                || matches!(policy.setup.storage, SetupStorage::Supplied { .. })
                || fs::read_dir(entry.path())?.next().is_none())
        {
            continue;
        }
        return Err(io::Error::other(format!(
            "unrecognized populated node root ({name}); use a fresh volume without removing retained data"
        )));
    }
    Ok(())
}

fn temporary_policy(name: &str) -> bool {
    name.strip_prefix(".node-policy-")
        .and_then(|suffix| suffix.strip_suffix(".json"))
        .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == id))
}
