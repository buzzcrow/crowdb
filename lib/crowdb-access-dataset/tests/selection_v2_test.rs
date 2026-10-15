// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/inspection.rs"]
#[allow(dead_code)]
mod inspection;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/selection.rs"]
mod selection;
use crowdb_access_dataset::{inspection::InspectionError, *};
use inspection::{budget, fixture, publish};

async fn selected(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    rule: &Selection,
) -> Vec<Vec<u8>> {
    let cancel = ReadCancellation::new();
    let mut after = None;
    let mut ids = Vec::new();
    loop {
        let page = authority
            .inspection_selection_page(identity, snapshot, rule, after, budget(2), &cancel)
            .await
            .unwrap();
        ids.extend(page.entries);
        after = page.after;
        if after.is_none() {
            return ids;
        }
    }
}
#[tokio::test]
async fn effective_selection_distinguishes_empty_missing_tombstone_and_chunk_values() {
    let (authority, identity, child) = selection::effective_fixture().await;
    let old = Selection::FieldMatch {
        field: "value".into(),
        value: b"old".to_vec(),
    };
    assert_eq!(
        selected(&authority, &identity, child, &old).await,
        vec![b"a".to_vec()]
    );
    assert_eq!(
        selected(
            &authority,
            &identity,
            child,
            &Selection::FieldMatch {
                field: "value".into(),
                value: vec![]
            }
        )
        .await,
        vec![b"b".to_vec()]
    );
    assert_eq!(
        selected(&authority, &identity, child, &Selection::Keyword(b"old".to_vec())).await,
        vec![b"a".to_vec()]
    );
    assert_eq!(
        selected(&authority, &identity, child, &Selection::Not(Box::new(old))).await,
        vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]
    );
    assert_eq!(
        selected(
            &authority,
            &identity,
            child,
            &Selection::And(vec![
                Selection::Range {
                    start: b"b".to_vec(),
                    end: b"e".to_vec()
                },
                Selection::Not(Box::new(Selection::Explicit(vec![b"c".to_vec()])))
            ])
        )
        .await,
        vec![b"b".to_vec(), b"d".to_vec()]
    );
    for rule in [
        Selection::FieldMatch {
            field: "unknown".into(),
            value: vec![],
        },
        Selection::Explicit(vec![b"absent".to_vec()]),
    ] {
        assert!(matches!(
            authority
                .inspection_selection_page(
                    &identity,
                    child,
                    &rule,
                    None,
                    budget(20),
                    &ReadCancellation::new()
                )
                .await,
            Err(InspectionError::Authority(AuthorityError::Invalid(_)))
        ));
    }
    assert!(
        selected(&authority, &identity, child, &Selection::Explicit(vec![]))
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn selection_cursor_is_bound_to_predicate_and_binary_membership() {
    let (_, authority, identity) = fixture().await;
    let mut manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(1)).unwrap();
    manifest.samples = [vec![0, 255], vec![0, 255, 0], vec![255]]
        .into_iter()
        .map(|sample_id| SampleRecord {
            sample_id,
            fields: manifest.samples[0].fields.clone(),
        })
        .collect();
    let snapshot = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            bincode::serialize(&manifest).unwrap(),
            OperationId::random(),
        )
        .await
        .unwrap();
    publish(&authority, &identity, snapshot).await;
    let rule = Selection::Prefix(vec![0, 255]);
    let cancel = ReadCancellation::new();
    let first = authority
        .inspection_selection_page(&identity, snapshot, &rule, None, budget(1), &cancel)
        .await
        .unwrap();
    assert_eq!(first.entries, vec![vec![0, 255]]);
    assert!(matches!(
        authority
            .inspection_selection_page(
                &identity,
                snapshot,
                &Selection::Prefix(vec![255]),
                first.after.clone(),
                budget(1),
                &cancel
            )
            .await,
        Err(InspectionError::Authority(AuthorityError::Invalid(
            DatasetError::CursorMismatch
        )))
    ));
    let second = authority
        .inspection_selection_page(&identity, snapshot, &rule, first.after, budget(1), &cancel)
        .await
        .unwrap();
    assert_eq!(second.entries, vec![vec![0, 255, 0]]);
}
