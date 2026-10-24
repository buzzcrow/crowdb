// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::{publication, split_store::fixture as base_fixture};
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
    let source = authority
        .prepare_split_revision(
            &identity,
            parent,
            &SplitPreparation {
                revision: SplitRevisionId::random(),
                policy: SplitPolicy::default(),
                splits: vec![DraftSplit {
                    name: name.clone(),
                    definition: rule.clone(),
                    origin: MembershipOrigin::Evaluated,
                }],
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    authority
        .update_split_head(
            &identity,
            parent,
            &SplitHeadUpdate {
                revision: source.revision,
                expected_revision: None,
                expected_head: None,
            },
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
