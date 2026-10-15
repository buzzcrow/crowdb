// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::{
    publication,
    split_store::{attempt, fixture as base_fixture},
};
use crowdb_access_dataset::{split::*, *};
pub(super) struct TestInheritedFixture {
    pub authority: DatasetAuthority,
    pub identity: DatasetIdentity,
    pub parent: SnapshotId,
    pub child: SnapshotId,
    pub source: SplitRevisionRecord,
    pub inherited: DraftSplit,
}
pub(super) async fn fixture() -> TestInheritedFixture {
    let (_, authority, identity, root) = base_fixture().await;
    let mut manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(1)).unwrap();
    manifest.samples[0].sample_id = b"keep".to_vec();
    let parent = authority
        .publish_snapshot(&identity, Some(root), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let name = SplitName::new("train").unwrap();
    let rule = SplitDefinition::Selection(Selection::Prefix(b"keep".to_vec()));
    let operation = OperationId::random();
    authority
        .begin_split_draft(
            &identity,
            operation,
            parent,
            None,
            SplitPolicy::default(),
            vec![DraftSplit {
                name: name.clone(),
                definition: rule.clone(),
                origin: MembershipOrigin::Evaluated,
            }],
        )
        .await
        .unwrap();
    authority
        .evaluate_split_draft(&identity, operation, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    let source = authority
        .prepare_split_draft(&identity, operation, 1)
        .await
        .unwrap();
    authority
        .publish_split_draft(
            &identity,
            operation,
            &attempt(&authority, &identity, &source).await,
        )
        .await
        .unwrap();
    let mut added = manifest.samples[0].clone();
    added.sample_id = b"keep-new".to_vec();
    manifest.samples.push(added);
    let child = authority
        .publish_snapshot(&identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    let inherited = DraftSplit {
        name: name.clone(),
        definition: rule,
        origin: MembershipOrigin::Inherited(SplitSource {
            snapshot: parent,
            revision: source.revision,
            name: name.clone(),
        }),
    };
    TestInheritedFixture {
        authority,
        identity,
        parent,
        child,
        source,
        inherited,
    }
}
