// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{composition::*, *};
use md5::{Digest, Md5};
pub(super) fn input(parent: SnapshotId) -> CompositionInput {
    CompositionInput {
        parent: Some(parent),
        source: CompositionSource::All,
        schema: None,
        inherit_parent_splits: false,
        parent_split_revision: None,
        splits: vec![],
        policy: None,
    }
}
pub(super) fn edit(revision: u64, samples: Vec<SampleChange>) -> CompositionEdit {
    CompositionEdit {
        id: OperationId::random(),
        expected_revision: revision,
        samples,
        schema: None,
        splits: None,
        policy: None,
    }
}
pub(super) fn sample(id: &[u8], value: u8) -> SampleRecord {
    SampleRecord {
        sample_id: id.to_vec(),
        fields: vec![FieldRecord {
            name: "marker".into(),
            value: FieldLocator::Inline {
                value: vec![value],
                md5: Md5::digest([value]).into(),
            },
        }],
    }
}
pub(super) async fn evaluate(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    operation: OperationId,
    revision: u64,
) -> CompositionPreview {
    for page in 0..100 {
        let preview = authority
            .evaluate_composition(identity, operation, revision, page, &ReadCancellation::new())
            .await
            .unwrap();
        if preview.complete {
            return preview;
        }
    }
    panic!("composition evaluation did not complete")
}
#[allow(dead_code)]
pub(super) async fn prepare(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    operation: OperationId,
    revision: u64,
) -> CompositionDraft {
    for _ in 0..100 {
        let draft = authority
            .prepare_composition(identity, operation, revision, &ReadCancellation::new())
            .await
            .unwrap();
        if matches!(draft.record.progress, DraftProgress::Prepared { .. }) {
            return draft;
        }
    }
    panic!("composition preparation did not complete")
}
#[allow(dead_code)]
pub(super) fn budget() -> MetadataScanBudget {
    MetadataScanBudget {
        items: 20,
        bytes: 1024 * 1024,
        timeout: std::time::Duration::from_secs(2),
    }
}

#[allow(dead_code)]
pub(super) async fn attempt(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) -> CommitAttempt {
    CommitAttempt {
        version: 1,
        id: OperationId::random(),
        target: CommitTarget::Snapshot { snapshot },
        expected_head: authority.inspect_head(identity).await.unwrap().generation,
    }
}

#[allow(dead_code)]
pub(super) async fn begin_evaluated(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    operation: OperationId,
    input: &CompositionInput,
) {
    authority
        .begin_composition(identity, operation, input)
        .await
        .unwrap();
    evaluate(authority, identity, operation, 1).await;
}
