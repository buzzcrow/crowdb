// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{composition::*, *};
use md5::{Digest, Md5};
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

pub(super) fn request(parent: SnapshotId) -> CompositionRequest {
    CompositionRequest {
        parent: Some(parent),
        source: CompositionSource::All,
        schema: None,
        samples: vec![],
        splits: vec![],
        policy: crowdb_access_dataset::split::SplitPolicy::default(),
    }
}
