#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/publication.rs"]
mod publication;

use crowdb_access_dataset::*;
use md5::{Digest, Md5};
use std::sync::Arc;

fn manifest() -> ManifestRecord {
    let mut manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(7)).unwrap();
    manifest.samples = (0_u8..16)
        .rev()
        .map(|number| {
            let value = vec![number; usize::from(number) + 1];
            SampleRecord {
                sample_id: vec![b's', number, 0xff],
                fields: vec![FieldRecord {
                    name: "marker".into(),
                    value: FieldLocator::Inline {
                        md5: Md5::digest(&value).into(),
                        value,
                    },
                }],
            }
        })
        .collect();
    manifest
}

fn plan(snapshot: SnapshotId) -> ReadPlan {
    ReadPlan {
        snapshot,
        selection: Selection::Prefix(b"s".to_vec()),
        ordering: crowdb_access_dataset::Ordering::SampleId,
        projection: vec!["marker".into()],
        batch_size: 3,
    }
}

#[test]
fn planner_and_shuffle_keep_binary_order_and_each_independent_budget_boundary() {
    let manifest = manifest();
    let plan = plan(manifest.snapshot);
    let mut ordered: Vec<_> = manifest.samples.iter().map(|row| row.sample_id.clone()).collect();
    ordered.sort();
    let limits = ReadLimits {
        max_samples: 16,
        max_metadata_bytes: 136,
        max_batches: 6,
        prefetch: 1,
        in_flight: 1,
    };
    assert_eq!(
        plan.scan_batches_with_limits(&manifest, limits).unwrap().concat(),
        ordered
    );
    for limits in [
        ReadLimits {
            max_samples: 15,
            ..limits
        },
        ReadLimits {
            max_metadata_bytes: 135,
            ..limits
        },
        ReadLimits {
            max_batches: 5,
            ..limits
        },
    ] {
        assert_eq!(
            plan.scan_batches_with_limits(&manifest, limits),
            Err(DatasetError::ReadLimitExceeded)
        );
    }
    let spec = ShuffleSpec {
        seed: 17,
        epoch: 3,
        max_shuffle_objects: 16,
        max_shuffle_bytes: 280,
    };
    let identity = plan.identity();
    let mut reference: Vec<_> = ordered
        .iter()
        .map(|id| {
            let mut digest = Md5::new();
            digest.update(spec.seed.to_le_bytes());
            digest.update(spec.epoch.to_le_bytes());
            digest.update(identity);
            digest.update(id);
            (<[u8; 16]>::from(digest.finalize()), id.clone())
        })
        .collect();
    reference.sort();
    let expected: Vec<_> = reference.into_iter().map(|(_, id)| id).collect();
    assert_eq!(SampleShuffle::execute(&plan, &manifest, &spec).unwrap(), expected);
    assert_eq!(
        SampleShuffle::execute_windows(&plan, &manifest, &spec)
            .unwrap()
            .concat(),
        expected
    );
    for spec in [
        ShuffleSpec {
            max_shuffle_objects: 15,
            ..spec.clone()
        },
        ShuffleSpec {
            max_shuffle_bytes: 279,
            ..spec.clone()
        },
    ] {
        assert_eq!(
            SampleShuffle::execute(&plan, &manifest, &spec),
            Err(DatasetError::ReadLimitExceeded)
        );
    }
    assert_shuffle_resume(&plan, &manifest, &spec, &expected);
}

fn assert_shuffle_resume(
    plan: &ReadPlan,
    manifest: &ManifestRecord,
    spec: &ShuffleSpec,
    expected: &[Vec<u8>],
) {
    let windows = expected.len().div_ceil(plan.batch_size);
    for group in 0..=windows {
        let length = if group == windows {
            0
        } else {
            (expected.len() - group * plan.batch_size).min(plan.batch_size)
        };
        for offset in 0..=length {
            let cursor = ReadCursor {
                group: u32::try_from(group).unwrap(),
                offset: u32::try_from(offset).unwrap(),
                ..ReadCursor::start(plan)
            };
            let start = (group * plan.batch_size).min(expected.len()) + offset;
            assert_eq!(
                SampleShuffle::resume(plan, manifest, spec, &cursor).unwrap(),
                expected[start..]
            );
        }
    }
    for (group, offset) in [(0, 4), (6, 1), (7, 0), (u32::MAX, 0)] {
        let cursor = ReadCursor {
            group,
            offset,
            ..ReadCursor::start(plan)
        };
        assert_eq!(
            SampleShuffle::resume(plan, manifest, spec, &cursor),
            Err(DatasetError::CursorMismatch)
        );
    }
}

#[tokio::test]
async fn legacy_scan_delivers_only_the_current_window_and_preserves_cursor_edges() {
    let store = access_store::TestAccessStore::shared();
    let authority = Arc::new(DatasetAuthority::new(store));
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-planning").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, bincode::serialize(&manifest()).unwrap())
        .await
        .unwrap();
    let manifest = authority.get_manifest(&identity, snapshot).await.unwrap();
    let plan = plan(snapshot);
    let service = DatasetReadService::new(authority, identity, 1).unwrap();
    for shuffle in [
        None,
        Some(ShuffleSpec::default()),
        Some(ShuffleSpec {
            seed: 17,
            epoch: 3,
            max_shuffle_objects: 16,
            max_shuffle_bytes: 280,
        }),
    ] {
        let windows = if let Some(spec) = &shuffle {
            SampleShuffle::execute_windows(&plan, &manifest, spec).unwrap()
        } else {
            plan.scan_batches(&manifest).unwrap()
        };
        for group in 0..=windows.len() {
            let length = windows.get(group).map_or(0, Vec::len);
            for offset in 0..=length {
                let cursor = ReadCursor {
                    group: u32::try_from(group).unwrap(),
                    offset: u32::try_from(offset).unwrap(),
                    ..ReadCursor::start(&plan)
                };
                let result = service
                    .scan(DatasetScanRequest {
                        plan: plan.clone(),
                        shuffle: shuffle.clone(),
                        cursor: Some(cursor),
                    })
                    .await
                    .unwrap();
                let actual: Vec<_> = result
                    .samples
                    .into_iter()
                    .map(|sample| sample.sample_id)
                    .collect();
                let expected = windows.get(group).map_or(&[][..], |ids| &ids[offset..]);
                assert_eq!(actual, expected);
                assert_eq!(result.end, group >= windows.len().saturating_sub(1));
                assert_eq!(
                    result.cursor.group as usize,
                    group + usize::from(!expected.is_empty())
                );
                assert_eq!(result.cursor.offset, 0);
                assert_eq!(service.in_flight(), 0);
            }
        }
        let cursor = ReadCursor {
            group: u32::MAX,
            ..ReadCursor::start(&plan)
        };
        assert!(service
            .scan(DatasetScanRequest {
                plan: plan.clone(),
                shuffle,
                cursor: Some(cursor)
            })
            .await
            .is_err());
        assert_eq!(service.in_flight(), 0);
    }
}
