// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};

use super::{DeploymentProfile, GroupRole, ProbeKind, ProfileError, ServiceProfile, PROFILE_VERSION};
use crate::layout::{is_clean_absolute, is_strict_descendant};

pub(super) fn validate(profile: &DeploymentProfile) -> Result<(), ProfileError> {
    if profile.version != PROFILE_VERSION {
        return invalid(format!(
            "unsupported version {}; expected {PROFILE_VERSION}",
            profile.version
        ));
    }
    require_slug("profile name", &profile.name)?;
    require_text("display name", &profile.display_name)?;
    require_text("placement mode", &profile.placement_mode)?;
    require_text("S3 tenant", &profile.s3_tenant)?;
    require_text("Iceberg catalog", &profile.iceberg_catalog)?;
    validate_paths(profile)?;
    validate_logs(profile)?;
    validate_topology(profile)?;
    validate_endpoints(profile)?;
    validate_services(profile)?;
    topological_order(profile)?;
    Ok(())
}

fn validate_logs(profile: &DeploymentProfile) -> Result<(), ProfileError> {
    if !(1024 * 1024..=1024 * 1024 * 1024).contains(&profile.logs.max_file_bytes)
        || !(1..=16).contains(&profile.logs.max_files)
    {
        return invalid("log rotation limits are outside supported bounds");
    }
    Ok(())
}

pub(super) fn services_in_start_order(
    profile: &DeploymentProfile,
) -> Result<Vec<&ServiceProfile>, ProfileError> {
    validate(profile)?;
    topological_order(profile)
}

fn validate_paths(profile: &DeploymentProfile) -> Result<(), ProfileError> {
    let paths = &profile.paths;
    for (name, path) in [
        ("install_root", paths.install_root.as_path()),
        ("bin_root", paths.bin_root.as_path()),
        ("template_root", paths.template_root.as_path()),
        ("data_root", paths.data_root.as_path()),
        ("run_root", paths.run_root.as_path()),
        ("log_root", paths.log_root.as_path()),
    ] {
        if !is_clean_absolute(path) {
            return invalid(format!("{name} must be a clean absolute path"));
        }
    }
    for (name, path) in [
        ("bin_root", paths.bin_root.as_path()),
        ("template_root", paths.template_root.as_path()),
        ("data_root", paths.data_root.as_path()),
        ("run_root", paths.run_root.as_path()),
    ] {
        if !is_strict_descendant(&paths.install_root, path) {
            return invalid(format!("{name} must be below install_root"));
        }
    }
    if !is_strict_descendant(&paths.data_root, &paths.log_root) {
        return invalid("log_root must be below data_root");
    }
    if paths.data_root.starts_with(&paths.run_root) || paths.run_root.starts_with(&paths.data_root) {
        return invalid("data_root and run_root must not overlap");
    }
    Ok(())
}

fn validate_topology(profile: &DeploymentProfile) -> Result<(), ProfileError> {
    if profile.nodes.is_empty() || profile.groups.is_empty() || profile.disks.is_empty() {
        return invalid("nodes, groups, and disks must be non-empty");
    }
    let mut node_ids = BTreeSet::new();
    for node in &profile.nodes {
        if node.node_id == 0 || node.rack_id == 0 || !node_ids.insert(node.node_id) {
            return invalid("node and rack IDs must be nonzero and node IDs unique");
        }
    }
    let mut groups = BTreeSet::new();
    let mut system_groups = 0_u32;
    for group in &profile.groups {
        if group.replica_id == 0 || !groups.insert((group.store_id, group.group_id)) {
            return invalid("group identities must be unique and replica IDs nonzero");
        }
        if group.role == GroupRole::System {
            system_groups += 1;
        }
    }
    if system_groups != 1 {
        return invalid("exactly one system group is required");
    }
    let disk_root = profile.paths.data_root.join("disks");
    let mut disk_ids = BTreeSet::new();
    let mut disk_paths = BTreeSet::new();
    for disk in &profile.disks {
        if !node_ids.contains(&disk.node_id) {
            return invalid(format!("disk {} references an unknown node", disk.disk_id));
        }
        if disk.disk_id.is_empty() || !disk_ids.insert(&disk.disk_id) || !disk_paths.insert(&disk.path) {
            return invalid("disk identities and paths must be non-empty and unique");
        }
        if disk.disk_group_id == 0 || disk.capacity_bytes == 0 || disk.zone_size_bytes == 0 {
            return invalid(format!("disk {} has invalid capacity or group", disk.disk_id));
        }
        if disk.capacity_bytes % disk.zone_size_bytes != 0 {
            return invalid(format!("disk {} capacity must contain whole zones", disk.disk_id));
        }
        if !is_strict_descendant(&disk_root, &disk.path) {
            return invalid(format!(
                "disk {} path must be below data_root/disks",
                disk.disk_id
            ));
        }
    }
    Ok(())
}

fn validate_endpoints(profile: &DeploymentProfile) -> Result<(), ProfileError> {
    if profile.public_endpoints.is_empty() {
        return invalid("at least one public endpoint is required");
    }
    let mut ids = BTreeSet::new();
    let mut listeners = BTreeSet::new();
    for endpoint in &profile.public_endpoints {
        require_slug("public endpoint ID", &endpoint.id)?;
        let address = endpoint.bind.parse::<IpAddr>().map_err(|_| {
            ProfileError::Invalid(format!("endpoint {} has invalid bind address", endpoint.id))
        })?;
        if endpoint.port == 0 || !ids.insert(&endpoint.id) || !listeners.insert((address, endpoint.port)) {
            return invalid("public endpoint IDs and listeners must be unique and nonzero");
        }
    }
    Ok(())
}

fn validate_services(profile: &DeploymentProfile) -> Result<(), ProfileError> {
    if profile.services.is_empty() {
        return invalid("at least one service is required");
    }
    let ids = profile
        .services
        .iter()
        .map(|service| service.id.as_str())
        .collect::<BTreeSet<_>>();
    if ids.len() != profile.services.len() {
        return invalid("service IDs must be unique");
    }
    for service in &profile.services {
        require_slug("service ID", &service.id)?;
        if !is_strict_descendant(&profile.paths.bin_root, &service.program) {
            return invalid(format!("service {} program must be below bin_root", service.id));
        }
        if let Some(template) = &service.config_template {
            if !is_strict_descendant(&profile.paths.template_root, template) {
                return invalid(format!(
                    "service {} template must be below template_root",
                    service.id
                ));
            }
        }
        let mut dependencies = BTreeSet::new();
        for dependency in &service.dependencies {
            if dependency == &service.id
                || !ids.contains(dependency.as_str())
                || !dependencies.insert(dependency)
            {
                return invalid(format!("service {} has an invalid dependency", service.id));
            }
        }
        for name in service.env.keys() {
            let upper = name.to_ascii_uppercase();
            if ["SECRET", "TOKEN", "PASSWORD", "MASTER_KEY", "ACCESS_KEY"]
                .iter()
                .any(|marker| upper.contains(marker))
            {
                return invalid(format!(
                    "service {} embeds a secret-like environment key",
                    service.id
                ));
            }
        }
        let mut listeners = BTreeSet::new();
        for listener in &service.fence_listeners {
            let address = listener.parse::<SocketAddr>().map_err(|_| {
                ProfileError::Invalid(format!("service {} has an invalid fence listener", service.id))
            })?;
            if !address.ip().is_loopback() || !listeners.insert(address) {
                return invalid(format!(
                    "service {} has a non-loopback or duplicate fence listener",
                    service.id
                ));
            }
        }
        validate_probe(service)?;
        let restart = &service.restart;
        if restart.max_attempts == 0
            || restart.backoff_base_ms == 0
            || restart.backoff_base_ms > restart.backoff_max_ms
            || restart.stable_after_ms == 0
        {
            return invalid(format!("service {} has invalid restart bounds", service.id));
        }
    }
    Ok(())
}

fn validate_probe(service: &ServiceProfile) -> Result<(), ProfileError> {
    let probe = &service.probe;
    if probe.timeout_ms == 0 || probe.failure_threshold == 0 {
        return invalid(format!("service {} has invalid probe bounds", service.id));
    }
    match probe.kind {
        ProbeKind::Http if !(probe.target.starts_with("http://") || probe.target.starts_with("https://")) => {
            invalid(format!("service {} has invalid HTTP probe", service.id))
        }
        ProbeKind::Tcp if probe.target.parse::<SocketAddr>().is_err() => {
            invalid(format!("service {} has invalid TCP probe", service.id))
        }
        ProbeKind::Http | ProbeKind::Tcp => Ok(()),
    }
}

fn topological_order(profile: &DeploymentProfile) -> Result<Vec<&ServiceProfile>, ProfileError> {
    let by_id = profile
        .services
        .iter()
        .map(|service| (service.id.as_str(), service))
        .collect::<BTreeMap<_, _>>();
    let mut remaining = profile
        .services
        .iter()
        .map(|service| {
            (
                service.id.as_str(),
                service
                    .dependencies
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut order = Vec::with_capacity(profile.services.len());
    while !remaining.is_empty() {
        let Some(id) = profile
            .services
            .iter()
            .map(|service| service.id.as_str())
            .find(|id| remaining.get(id).is_some_and(BTreeSet::is_empty))
        else {
            return invalid("service dependency graph contains a cycle");
        };
        remaining.remove(id);
        for dependencies in remaining.values_mut() {
            dependencies.remove(id);
        }
        order.push(by_id[id]);
    }
    Ok(order)
}

fn require_slug(field: &str, value: &str) -> Result<(), ProfileError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return invalid(format!("{field} must be a lowercase slug"));
    }
    Ok(())
}

fn require_text(field: &str, value: &str) -> Result<(), ProfileError> {
    if value.trim().is_empty() || value.len() > 256 {
        return invalid(format!("{field} must be non-empty and bounded"));
    }
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T, ProfileError> {
    Err(ProfileError::Invalid(message.into()))
}
