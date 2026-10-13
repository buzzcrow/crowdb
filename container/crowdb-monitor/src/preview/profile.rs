// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{PreviewError, MAX_TEMPLATE_BYTES};
use crate::{
    disk_step_names, hardware_step_names, iceberg_step_names, kv_step_names, logical_step_names,
    s3_step_names, BootstrapSession, DeploymentProfile,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

pub(super) fn has_service(profile: &DeploymentProfile, id: &str) -> bool {
    profile.services.iter().any(|service| service.id == id)
}

fn validate_selection(profile: &DeploymentProfile) -> Result<(), PreviewError> {
    if !has_service(profile, "kv") {
        return Err(PreviewError::Invalid("KV service is absent"));
    }
    for service in &profile.services {
        let required: &[&str] = match service.id.as_str() {
            "kv" => &[],
            "diskdb" => &["kv"],
            "diskio" => &["kv", "diskdb"],
            "chunkdb" => &["kv", "diskdb", "diskio"],
            "chunk-kv" => &["kv", "chunkdb", "diskio"],
            "access" => &["kv", "chunk-kv", "chunkdb", "diskio"],
            "web" => &["access"],
            _ => return Err(PreviewError::Invalid("unknown release service")),
        };
        if required.iter().any(|dependency| {
            !has_service(profile, dependency)
                || !service
                    .dependencies
                    .iter()
                    .any(|declared| declared.as_str() == *dependency)
        }) {
            return Err(PreviewError::Invalid("release service dependency is absent"));
        }
    }
    Ok(())
}

pub(super) fn step_names(profile: &DeploymentProfile) -> Result<Vec<String>, PreviewError> {
    validate_selection(profile)?;
    let mut steps = kv_step_names(profile)?;
    if has_service(profile, "diskdb") {
        steps.extend(disk_step_names(profile));
        steps.extend(hardware_step_names());
    }
    steps.extend(logical_step_names().map(str::to_owned));
    if has_service(profile, "access") {
        steps.extend(s3_step_names().map(str::to_owned));
        steps.extend(iceberg_step_names().map(str::to_owned));
    }
    Ok(steps)
}

pub(super) fn management_seed(profile: &DeploymentProfile) -> Result<String, PreviewError> {
    let kv = profile
        .services
        .iter()
        .find(|service| service.id == "kv")
        .ok_or(PreviewError::Invalid("KV service is absent"))?;
    let seed = kv
        .probe
        .target
        .strip_suffix("/health")
        .ok_or(PreviewError::Invalid("KV management probe is incompatible"))?;
    if let Some(access) = profile.services.iter().find(|service| service.id == "access") {
        if access.env.get("CROWDB_MANAGEMENT_SEEDS").map(String::as_str) != Some(seed) {
            return Err(PreviewError::Invalid("Access management seed differs from KV"));
        }
    }
    Ok(seed.to_owned())
}

pub(super) fn kv_root(profile: &DeploymentProfile) -> Result<std::path::PathBuf, PreviewError> {
    let service = profile
        .services
        .iter()
        .find(|service| service.id == "kv")
        .ok_or(PreviewError::Invalid("KV service is absent"))?;
    let mut roots = service.args.windows(2).filter(|pair| pair[0] == "--root");
    let path = roots
        .next()
        .ok_or(PreviewError::Invalid("KV root argument is absent"))?[1]
        .as_str();
    if roots.next().is_some() {
        return Err(PreviewError::Invalid("KV root argument is duplicated"));
    }
    let path = Path::new(path);
    if !path.starts_with(profile.paths.data_root.join("kv"))
        || path == profile.paths.data_root.join("kv")
        || !crate::layout::is_clean_absolute(path)
    {
        return Err(PreviewError::Invalid(
            "KV root is outside the durable KV directory",
        ));
    }
    Ok(path.to_owned())
}

pub(super) fn config_digest_input(profile: &DeploymentProfile) -> Result<Vec<u8>, PreviewError> {
    let mut files = BTreeMap::new();
    for service in &profile.services {
        if let Some(path) = &service.config_template {
            let name = path
                .file_name()
                .ok_or(PreviewError::Invalid("template has no name"))?;
            if files
                .insert(name.to_os_string(), path)
                .is_some_and(|existing| existing != path)
            {
                return Err(PreviewError::Invalid("template name is duplicated"));
            }
        }
    }
    let mut input = Vec::new();
    for name in files.keys() {
        let path = profile.paths.template_root.join(name);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file() || metadata.len() > MAX_TEMPLATE_BYTES {
            return Err(PreviewError::Invalid("template is not a bounded regular file"));
        }
        let body = fs::read(&path)?;
        for field in [name.as_encoded_bytes(), body.as_slice()] {
            input.extend_from_slice(&(field.len() as u64).to_be_bytes());
            input.extend_from_slice(field);
        }
    }
    Ok(input)
}

pub(super) fn ensure_directory(path: &Path) -> Result<(), PreviewError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_dir() => {
            Err(PreviewError::Invalid("runtime path is not a directory"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) fn require_directory(path: &Path) -> Result<(), PreviewError> {
    if !fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(PreviewError::Invalid("required durable directory is missing"));
    }
    Ok(())
}

pub(super) fn bind_single_node(
    profile: &DeploymentProfile,
    session: &BootstrapSession,
    management_seed: &str,
) -> Result<(), PreviewError> {
    crate::node::write_single_binding(
        &profile.paths.data_root,
        &crowdb_protocol::mgmt::node::NodeBinding {
            node_id: profile.nodes[0].node_id,
            bootstrap: session.manifest().system_bootstrap_identity(),
            management_seeds: vec![management_seed.to_owned()],
        },
    )?;
    Ok(())
}
