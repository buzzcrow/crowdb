// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{chunk::legacy::LegacyManifest, Config, Crowdbtree, CtError};

fn manifest() -> &'static [u8] {
    let fixture = include_bytes!("../../tests/data/legacy-chunk-v7.bin");
    let length = usize::try_from(u64::from_le_bytes(fixture[8..16].try_into().unwrap())).unwrap();
    &fixture[16..16 + length]
}

#[test]
fn actual_old_writer_manifest_has_complete_pack_identities() {
    let original = LegacyManifest::decode(manifest()).unwrap();
    assert_eq!(
        (original.tree_id(), original.owner_epoch(), original.generation()),
        (42, 7, 1)
    );
    assert_eq!(original.packs().len(), 29);
    assert!(original
        .packs()
        .all(|pack| pack.owner_tree_id == 42 && pack.length != 0));
}

#[test]
fn corrupted_and_truncated_old_manifests_cannot_become_retirement_proofs() {
    for bytes in [&manifest()[..80], &manifest()[..manifest().len() - 1]] {
        assert!(matches!(LegacyManifest::decode(bytes), Err(CtError::Corruption)));
    }
    let mut damaged = manifest().to_vec();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    assert!(matches!(
        LegacyManifest::decode(&damaged),
        Err(CtError::Corruption)
    ));
}

#[test]
fn explicit_migration_requires_a_host_and_nonzero_original_generation() {
    let config = Config {
        runtime: None,
        ..Config::default()
    };
    assert!(matches!(
        Crowdbtree::migrate_legacy_chunk(&config, 1),
        Err(CtError::InvalidArgument)
    ));
    assert!(matches!(
        Crowdbtree::migrate_legacy_chunk(&config, 0),
        Err(CtError::InvalidArgument)
    ));
    assert!(matches!(
        Crowdbtree::migrate_legacy_chunk(&config, u64::MAX),
        Err(CtError::InvalidArgument)
    ));
}
