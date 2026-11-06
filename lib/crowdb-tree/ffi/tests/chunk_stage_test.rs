// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::chunk::stage::{validate_stage_transition, StageId, StagePack, StageRecord, StageState};
use crowdb_tree_ffi::{ChunkRootCatalog, CtError};

fn reserved() -> StageRecord {
    StageRecord {
        tree_id: 42,
        id: StageId {
            owner_epoch: 7,
            operation_high: 8,
            operation_low: 9,
        },
        revision: 1,
        chunk_high: 2_u64 << 56,
        chunk_low: 123,
        capacity: 4096,
        purpose: 2,
        state: StageState::Reserved,
        manifest_generation: 0,
        packs: vec![],
    }
}
#[test]
fn versioned_stage_wire_preserves_identity_and_rejects_corruption() {
    let mut record = reserved();
    record.state = StageState::Writing;
    record.revision = 2;
    record.packs.push(StagePack {
        offset: 0,
        length: 4096,
        checksum: 0x12345678,
    });
    let bytes = record.encode().unwrap();
    assert_eq!(&bytes[..8], &[b'C', b'T', b'S', b'G', 1, 0, 0, 0]);
    assert_eq!(bytes.len(), 116);
    assert_eq!(StageRecord::decode(&bytes), Ok(record));
    for offset in [8, 40, 64, 84, 88, 96, 112] {
        let mut corrupt = bytes.clone();
        corrupt[offset] ^= 1;
        assert_eq!(StageRecord::decode(&corrupt), Err(CtError::Corruption));
    }
    let mut version = bytes.clone();
    version[4] = 2;
    assert_eq!(StageRecord::decode(&version), Err(CtError::NotSupported));
    assert_eq!(
        StageRecord::decode(&bytes[..bytes.len() - 1]),
        Err(CtError::Corruption)
    );
}
#[test]
fn transitions_keep_provisional_identity_and_never_treat_unknown_write_as_reclaimed() {
    let first = reserved();
    assert!(validate_stage_transition(None, Some(&first)).is_ok());
    let mut next = first.clone();
    next.revision = 2;
    next.state = StageState::Writing;
    assert!(validate_stage_transition(Some(&first), Some(&next)).is_ok());
    assert_eq!(
        validate_stage_transition(Some(&next), None),
        Err(CtError::InvalidArgument)
    );
    let mut changed = next.clone();
    changed.chunk_low += 1;
    assert_eq!(
        validate_stage_transition(Some(&first), Some(&changed)),
        Err(CtError::InvalidArgument)
    );
    let mut complete = next.clone();
    complete.revision = 3;
    complete.state = StageState::Reloadable;
    assert_eq!(
        validate_stage_transition(Some(&next), Some(&complete)),
        Err(CtError::InvalidArgument)
    );
    complete.packs.push(StagePack {
        offset: 0,
        length: 4096,
        checksum: 0,
    });
    assert!(validate_stage_transition(Some(&next), Some(&complete)).is_ok());
    complete.packs.push(StagePack {
        offset: 1,
        length: 1,
        checksum: 0,
    });
    assert_eq!(complete.validate(), Err(CtError::InvalidArgument));
}
#[test]
fn legacy_catalog_explicitly_disables_staging_records() {
    let catalog = ChunkRootCatalog::open_memory(1).unwrap();
    assert!(!catalog.supports_staging());
    let first = reserved();
    assert_eq!(catalog.load_stage(42, first.id), Err(CtError::NotSupported));
    assert_eq!(
        catalog.compare_stage(42, 7, None, Some(&first)),
        Err(CtError::NotSupported)
    );
    assert_eq!(catalog.list_stages(42, None, 1), Err(CtError::NotSupported));
}
