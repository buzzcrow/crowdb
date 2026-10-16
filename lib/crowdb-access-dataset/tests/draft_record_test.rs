// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    composition::{CompositionRecord, CompositionSource, DraftProgress},
    split::{SplitDraftRecord, SplitName, SplitPolicy, SplitSource},
    CommitTarget, OperationId, SnapshotId, SplitRevisionId,
};

fn composition() -> CompositionRecord {
    CompositionRecord {
        version: 1,
        operation: OperationId::random(),
        parent: None,
        parent_split_revision: None,
        source: CompositionSource::All,
        revision: 1,
        edit_pages: 0,
        source_lease_deadline: 0,
        progress: DraftProgress::Editing,
    }
}

#[test]
fn composition_pins_source_snapshot_revision_and_accepts_parentless_roots() {
    let mut record = composition();
    assert_eq!(
        CompositionRecord::decode(&record.encode().unwrap()).unwrap(),
        record
    );
    let source = SplitSource {
        snapshot: SnapshotId::random(),
        revision: SplitRevisionId::random(),
        name: SplitName::new("train").unwrap(),
    };
    record.source = CompositionSource::Split {
        source: source.clone(),
        selection: None,
    };
    assert!(record.validate().is_err());
    record.parent = Some(source.snapshot);
    assert!(record.validate().is_err());
    record.parent_split_revision = Some(source.revision);
    record.validate().unwrap();
    record.parent_split_revision = Some(SplitRevisionId::random());
    assert!(record.validate().is_err());
}

#[test]
fn edits_invalidate_preview_and_cannot_modify_prepared_content() {
    let mut record = composition();
    record.progress = DraftProgress::Evaluated {
        input_revision: 1,
        binding: [1; 32],
    };
    record.progress.apply_edit(&mut record.revision, 1).unwrap();
    assert_eq!(record.revision, 2);
    assert_eq!(record.progress, DraftProgress::Editing);
    let saved = record.clone();
    assert!(record.progress.apply_edit(&mut record.revision, 1).is_err());
    assert_eq!(record, saved);
    record.progress = DraftProgress::Evaluating {
        input_revision: 1,
        next_page: 4,
    };
    assert!(record.validate().is_err());
    record.progress = DraftProgress::Prepared {
        target: CommitTarget::Snapshot {
            snapshot: SnapshotId::random(),
        },
    };
    let saved = record.clone();
    assert!(record.progress.apply_edit(&mut record.revision, 2).is_err());
    assert_eq!(record, saved);
    record.progress = DraftProgress::Editing;
    record.revision = u64::MAX;
    assert!(record
        .progress
        .apply_edit(&mut record.revision, u64::MAX)
        .is_err());
    assert_eq!(record.revision, u64::MAX);
}

#[test]
fn split_drafts_require_expected_revision_and_cannot_prepare_another_snapshot() {
    let mut draft = SplitDraftRecord {
        version: 1,
        operation: OperationId::random(),
        snapshot: SnapshotId::random(),
        expected_revision: None,
        revision: 1,
        policy: SplitPolicy::default(),
        splits: vec![],
        progress: DraftProgress::Editing,
    };
    assert_eq!(SplitDraftRecord::decode(&draft.encode().unwrap()).unwrap(), draft);
    let mut missing = serde_json::to_value(&draft).unwrap();
    missing.as_object_mut().unwrap().remove("expected_revision");
    assert!(SplitDraftRecord::decode(&serde_json::to_vec(&missing).unwrap()).is_err());
    draft.progress = DraftProgress::Prepared {
        target: CommitTarget::Snapshot {
            snapshot: draft.snapshot,
        },
    };
    assert!(draft.validate().is_err());
    draft.progress = DraftProgress::Prepared {
        target: CommitTarget::SplitRevision {
            snapshot: SnapshotId::random(),
            revision: SplitRevisionId::random(),
        },
    };
    assert!(draft.validate().is_err());
    draft.progress = DraftProgress::Prepared {
        target: CommitTarget::SplitRevision {
            snapshot: draft.snapshot,
            revision: SplitRevisionId::random(),
        },
    };
    draft.validate().unwrap();
}
