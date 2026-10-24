// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/split_inheritance.rs"]
mod inheritance;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{split::*, *};
use split_store::{attempt, fixture};

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
    let child_operation = OperationId::random();
    authority
        .begin_split_draft(
            &identity,
            child_operation,
            child,
            None,
            SplitPolicy::default(),
            vec![inherited.clone()],
        )
        .await
        .unwrap();
    let newer = OperationId::random();
    authority
        .begin_split_draft(
            &identity,
            newer,
            parent,
            Some(source.revision),
            SplitPolicy::default(),
            vec![],
        )
        .await
        .unwrap();
    authority
        .evaluate_split_draft(&identity, newer, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    let latest = authority.prepare_split_draft(&identity, newer, 1).await.unwrap();
    authority
        .publish_split_draft(&identity, newer, &attempt(&authority, &identity, &latest).await)
        .await
        .unwrap();
    let preview = authority
        .evaluate_split_draft(&identity, child_operation, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(preview.member_counts, vec![1]);
    assert_eq!(preview.validation.unassigned, 1);
    let frozen = authority
        .prepare_split_draft(&identity, child_operation, 1)
        .await
        .unwrap();
    assert_eq!(frozen.splits[0].origin, inherited.origin);
    let reevaluate = OperationId::random();
    authority
        .begin_split_draft(
            &identity,
            reevaluate,
            child,
            None,
            SplitPolicy::default(),
            vec![DraftSplit {
                origin: MembershipOrigin::Evaluated,
                ..inherited
            }],
        )
        .await
        .unwrap();
    let preview = authority
        .evaluate_split_draft(&identity, reevaluate, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(preview.member_counts, vec![2]);
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
    assert!(authority
        .begin_split_draft(
            &identity,
            OperationId::random(),
            child,
            None,
            SplitPolicy::default(),
            vec![inherited.clone()]
        )
        .await
        .is_ok());
    let mut missing_name = inherited.clone();
    if let MembershipOrigin::Inherited(source) = &mut missing_name.origin {
        source.name = SplitName::new("missing").unwrap();
    }
    assert!(matches!(
        authority
            .begin_split_draft(
                &identity,
                OperationId::random(),
                child,
                None,
                SplitPolicy::default(),
                vec![missing_name]
            )
            .await,
        Err(SplitLifecycleError::NotFound)
    ));
    let operation = OperationId::random();
    authority
        .begin_split_draft(
            &identity,
            operation,
            child,
            None,
            SplitPolicy::default(),
            vec![inherited],
        )
        .await
        .unwrap();
    let preview = authority
        .evaluate_split_draft(&identity, operation, 1, 0, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(preview.member_counts, vec![0]);
    authority
        .abandon_split_draft(&identity, operation, 1)
        .await
        .unwrap();
}
