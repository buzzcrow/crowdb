// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{
    disk_step_names, hardware_step_names, iceberg_step_names, kv_step_names, logical_step_names,
    preview_step_names_for_tests, s3_step_names, DeploymentProfile,
};
use std::path::Path;

fn profile() -> DeploymentProfile {
    DeploymentProfile::load(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml"),
    )
    .unwrap()
}

#[test]
fn complete_profile_preserves_the_durable_bootstrap_order() {
    let profile = profile();
    let expected: Vec<_> = kv_step_names(&profile)
        .unwrap()
        .into_iter()
        .chain(disk_step_names(&profile))
        .chain(hardware_step_names())
        .chain(logical_step_names().map(str::to_owned))
        .chain(s3_step_names().map(str::to_owned))
        .chain(iceberg_step_names().map(str::to_owned))
        .collect();
    assert_eq!(preview_step_names_for_tests(&profile).unwrap(), expected);
}

#[test]
fn kv_and_storage_subsets_exclude_unselected_bootstrap_contracts() {
    for ids in [
        vec!["kv"],
        vec!["kv", "diskdb"],
        vec!["kv", "diskdb", "diskio"],
        vec!["kv", "diskdb", "diskio", "chunkdb"],
        vec!["kv", "diskdb", "diskio", "chunkdb", "chunk-kv"],
    ] {
        let mut profile = profile();
        profile
            .services
            .retain(|service| ids.contains(&service.id.as_str()));
        let mut expected = kv_step_names(&profile).unwrap();
        if ids.contains(&"diskdb") {
            expected.extend(disk_step_names(&profile));
            expected.extend(hardware_step_names());
        }
        expected.extend(logical_step_names().map(str::to_owned));
        assert_eq!(preview_step_names_for_tests(&profile).unwrap(), expected);
    }
}

#[test]
fn unknown_services_and_missing_runtime_dependency_edges_are_rejected() {
    let mut unknown = profile();
    unknown.services.last_mut().unwrap().id = "unknown".into();
    unknown.validate().unwrap();
    assert!(preview_step_names_for_tests(&unknown).is_err());
    let mut missing = profile();
    missing
        .services
        .iter_mut()
        .find(|service| service.id == "diskdb")
        .unwrap()
        .dependencies
        .clear();
    missing.validate().unwrap();
    assert!(preview_step_names_for_tests(&missing).is_err());
    let mut absent = profile();
    absent
        .services
        .retain(|service| service.id == "kv" || service.id == "diskio");
    absent
        .services
        .iter_mut()
        .find(|service| service.id == "diskio")
        .unwrap()
        .dependencies = vec!["kv".into()];
    absent.validate().unwrap();
    assert!(preview_step_names_for_tests(&absent).is_err());
}

#[test]
fn incompatible_management_seed_and_durable_root_fail_preflight() {
    let mut seed = profile();
    seed.services
        .iter_mut()
        .find(|service| service.id == "access")
        .unwrap()
        .env
        .insert("CROWDB_MANAGEMENT_SEEDS".into(), "http://127.0.0.1:1".into());
    assert!(preview_step_names_for_tests(&seed).is_err());
    let mut root = profile();
    let kv = root
        .services
        .iter_mut()
        .find(|service| service.id == "kv")
        .unwrap();
    let index = kv.args.iter().position(|argument| argument == "--root").unwrap();
    kv.args[index + 1] = "/tmp/foreign-kv".into();
    assert!(preview_step_names_for_tests(&root).is_err());
}
