// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/split_inheritance.rs"]
mod inheritance;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{split::*, *};
use split_store::fixture;

async fn prepare(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    splits: Vec<DraftSplit>,
) -> Result<SplitRevisionRecord, SplitLifecycleError> {
    authority
        .prepare_split_revision(
            identity,
            snapshot,
            &SplitPreparation {
                revision: SplitRevisionId::random(),
                policy: SplitPolicy::default(),
                splits,
            },
            &ReadCancellation::new(),
        )
        .await
}

#[tokio::test]
async fn frozen_child_membership_ignores_new_parent_revision_and_new_matching_samples() {
    let inheritance::TestInheritedFixture {
        authority,
        identity,
        parent,
        child,
        source,
        inherited,
    } = inheritance::fixture().await;
    let latest = prepare(&authority, &identity, parent, vec![]).await.unwrap();
    split_store::publish_revision(&authority, &identity, &latest).await;
    assert_ne!(latest.revision, source.revision);
    let frozen = prepare(&authority, &identity, child, vec![inherited.clone()])
        .await
        .unwrap();
    assert_eq!(frozen.splits[0].count, 1);
    assert_eq!(frozen.splits[0].origin, inherited.origin);
    let evaluated = prepare(
        &authority,
        &identity,
        child,
        vec![DraftSplit {
            origin: MembershipOrigin::Evaluated,
            ..inherited
        }],
    )
    .await
    .unwrap();
    assert_eq!(evaluated.splits[0].count, 2);
}

#[tokio::test]
async fn inherited_sources_accept_complete_parent_revisions_and_require_exact_names() {
    let (_, authority, identity, parent) = fixture().await;
    let source = split_store::prepared(&authority, &identity, parent, None).await;
    let child = authority
        .publish_snapshot(&identity, Some(parent), publication::publication_manifest(2))
        .await
        .unwrap();
    let inherited = DraftSplit {
        name: SplitName::new("inherited").unwrap(),
        definition: SplitDefinition::Selection(Selection::Explicit(vec![])),
        origin: MembershipOrigin::Inherited(SplitSource {
            snapshot: parent,
            revision: source.revision,
            name: SplitName::new("train").unwrap(),
        }),
    };
    let complete = prepare(&authority, &identity, child, vec![inherited.clone()])
        .await
        .unwrap();
    assert_eq!(complete.splits[0].count, 0);
    let mut missing_name = inherited.clone();
    if let MembershipOrigin::Inherited(source) = &mut missing_name.origin {
        source.name = SplitName::new("missing").unwrap();
    }
    assert!(matches!(
        prepare(&authority, &identity, child, vec![missing_name]).await,
        Err(SplitLifecycleError::NotFound)
    ));
    // The definition alone matches nothing; frozen membership still comes from the named source.
    assert_eq!(complete.splits[0].origin, inherited.origin);
}
