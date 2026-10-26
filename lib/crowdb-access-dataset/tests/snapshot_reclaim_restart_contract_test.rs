// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/snapshot_expiry.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use common::TestExpiryFixture;
use crowdb_access_dataset::*;
use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

const RETIRED: &[u8] = b"snapshot-metadata-retired-v1";

fn scoped(prefix: &[u8], identity: &DatasetIdentity, snapshot: SnapshotId) -> Vec<u8> {
    [prefix, &identity.key().unwrap(), snapshot.as_bytes()].concat()
}

async fn page(store: &dyn DatasetStore, prefix: Vec<u8>) -> MetadataPage {
    let page = store
        .scan_page(
            &MetadataScan {
                prefix,
                after: None,
                budget: MetadataScanBudget {
                    items: 256,
                    bytes: 1024 * 1024,
                    timeout: Duration::from_secs(2),
                },
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert!(
        page.after.is_none(),
        "the fixed fixture must fit one observation page"
    );
    page
}

async fn keys(store: &dyn DatasetStore) -> BTreeSet<Vec<u8>> {
    page(store, b"DS\0".to_vec())
        .await
        .entries
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

async fn fences(
    store: &dyn DatasetStore,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) -> BTreeSet<Vec<u8>> {
    let mut fenced = BTreeSet::new();
    for prefix in [b"DS\0M\0".as_slice(), b"DS\0I\0"] {
        for (key, value) in page(store, scoped(prefix, identity, snapshot)).await.entries {
            if value.bytes == RETIRED {
                fenced.insert(key);
            }
        }
    }
    fenced
}

fn eight_field_manifest() -> Vec<u8> {
    let mut manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(2)).unwrap();
    let field = manifest.samples[0].fields[0].clone();
    manifest.schema.fields = (0..8)
        .map(|index| FieldDefinition {
            name: format!("field-{index}"),
            required: true,
        })
        .collect();
    manifest.samples[0].fields = manifest
        .schema
        .fields
        .iter()
        .map(|definition| FieldRecord {
            name: definition.name.clone(),
            value: field.value.clone(),
        })
        .collect();
    manifest.validate().unwrap();
    bincode::serialize(&manifest).unwrap()
}

async fn discarded_prefix(
    f: &TestExpiryFixture,
    uploads: &DatasetUploads,
    snapshot: SnapshotId,
    baseline: &BTreeSet<Vec<u8>>,
) -> BTreeSet<Vec<u8>> {
    let mut prefix = None;
    for turn in 0..3 {
        let authority = DatasetAuthority::new(f.store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
        let mut cursor = SnapshotReclaimCursor::new(f.identity.clone(), snapshot);
        let status = authority
            .advance_snapshot_reclaim(uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        assert!(!status.complete);
        assert_eq!(status.phase, SnapshotReclaimPhase::IndexFences);
        let observed = fences(f.store.as_ref(), &f.identity, snapshot).await;
        assert_eq!(observed.len(), 20);
        if let Some(prefix) = &prefix {
            assert_eq!(
                &observed, prefix,
                "a fresh cursor must repeat the same bounded prefix"
            );
        } else {
            prefix = Some(observed);
        }
        assert_eq!(&keys(f.store.as_ref()).await, baseline);
        eprintln!("discarded cursor window {turn}: phase IndexFences, same 20 fences, no new keys");
    }
    prefix.unwrap()
}

#[tokio::test]
async fn repeated_cursor_loss_repeats_the_prefix_then_continuous_windows_complete() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f
        .authority
        .prepare_snapshot_candidate(
            &f.identity,
            None,
            eight_field_manifest(),
            OperationId::from_bytes(&[2; 16]).unwrap(),
        )
        .await
        .unwrap();
    f.authority
        .mark_snapshot_reclaim(&f.identity, snapshot)
        .await
        .unwrap();
    f.clock.store(2200, Ordering::Relaxed);
    let uploads = DatasetUploads::new(f.store.clone(), Arc::new(upload::TestPayload::default()));
    let baseline = keys(f.store.as_ref()).await;
    let retired_key = scoped(b"DS\0RT\0", &f.identity, snapshot);
    let retired = f.store.get(&retired_key).await.unwrap().unwrap();
    assert_eq!(retired.bytes, [1]);
    let binding_key = scoped(b"DS\0CB\0", &f.identity, snapshot);
    let binding = f.store.get(&binding_key).await.unwrap().unwrap();
    assert_eq!(binding.bytes, b"retired-snapshot-content-v1");
    let prefix = discarded_prefix(&f, &uploads, snapshot, &baseline).await;
    let authority = DatasetAuthority::new(f.store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    let mut cursor = SnapshotReclaimCursor::new(f.identity.clone(), snapshot);
    let mut completed_at = None;
    for turn in 0..32 {
        if authority
            .advance_snapshot_reclaim(&uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            completed_at = Some(turn + 1);
            break;
        }
    }
    assert!(
        completed_at.is_some(),
        "continuous bounded windows must complete the fixed candidate"
    );
    eprintln!(
        "continuous cursor completed after {} windows",
        completed_at.unwrap()
    );
    let completed = fences(f.store.as_ref(), &f.identity, snapshot).await;
    assert_eq!(
        completed.len(),
        26,
        "one Manifest partition and all twenty-five index rows stay fenced"
    );
    assert!(prefix.is_subset(&completed));
    assert_eq!(f.store.get(&retired_key).await.unwrap().unwrap(), retired);
    assert_eq!(f.store.get(&binding_key).await.unwrap().unwrap(), binding);
    let after = keys(f.store.as_ref()).await;
    let added: BTreeSet<_> = after.difference(&baseline).cloned().collect();
    let completion_key = scoped(b"DS\0G\0", &f.identity, snapshot);
    assert_eq!(added, BTreeSet::from([completion_key]));
    assert_eq!(
        authority
            .reclaim_progress(&f.identity, snapshot)
            .await
            .unwrap()
            .state,
        ReclaimState::Completed
    );
    for prefix in [b"DS\0GUP\0".as_slice(), b"DS\0GMC\0", b"DS\0GSD\0", b"DS\0CGF\0"] {
        assert!(
            !after.iter().any(|key| key.starts_with(prefix)),
            "local cursor positions are never persisted"
        );
    }
    assert!(authority.get_snapshot(&f.identity, snapshot).await.is_err());
    assert!(authority.get_snapshot(&f.identity, f.root).await.is_ok());
}
