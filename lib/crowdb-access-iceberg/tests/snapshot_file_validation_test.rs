#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/deletion_vector.rs"]
#[allow(dead_code)]
mod dv;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/manifest_list.rs"]
#[allow(dead_code)]
mod list_fixture;
#[path = "common/parquet_iceberg_fixture.rs"]
mod official;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;
#[path = "common/snapshot_files.rs"]
mod snapshot;

use crowdb_access_iceberg::file::ContentFormat;
use crowdb_access_iceberg::manifest::{validate_snapshot_files, SnapshotValidationError};
use std::sync::Arc;

#[tokio::test]
async fn complete_enumeration_binds_all_canonical_data_rows() {
    let store = Arc::new(blocks::TestBlocks::default());
    let first = snapshot::data(store.clone(), "data/first.parquet").await;
    let second = snapshot::data(store.clone(), "data/second.parquet").await;
    let input = snapshot::input(
        store.clone(),
        vec![
            vec![snapshot::entry(&first, 0, 10)],
            vec![snapshot::entry(&second, 0, 10)],
        ],
        vec![first, second],
    )
    .await;
    let summary = validate_snapshot_files(store, input, snapshot::limits())
        .await
        .unwrap();
    assert_eq!(summary.data_rows, 20);
    assert_eq!(summary.data_files, 2);
    assert_eq!(summary.manifests.entries, 2);
    assert_eq!(summary.manifests.manifests, 2);
}

#[tokio::test]
async fn wrong_counts_missing_authority_and_unsupported_formats_fail_closed() {
    for case in 0..3 {
        let store = Arc::new(blocks::TestBlocks::default());
        let mut record = snapshot::data(store.clone(), "data/first.parquet").await;
        if case == 2 {
            record.format = ContentFormat::Orc;
        }
        let entry = snapshot::entry(&record, 0, if case == 0 { 11 } else { 10 });
        let input = snapshot::input(
            store.clone(),
            vec![vec![entry]],
            if case == 1 { vec![] } else { vec![record] },
        )
        .await;
        let result = validate_snapshot_files(store, input, snapshot::limits()).await;
        match case {
            0 => assert!(matches!(result, Err(SnapshotValidationError::Parquet(_)))),
            1 => assert!(matches!(result, Err(SnapshotValidationError::Unavailable))),
            _ => assert!(matches!(result, Err(SnapshotValidationError::Unsupported))),
        }
    }
}

#[tokio::test]
async fn index_budgets_are_independent_and_scope_is_bound() {
    for case in 0..3 {
        let store = Arc::new(blocks::TestBlocks::default());
        let first = snapshot::data(store.clone(), "data/first.parquet").await;
        let second = snapshot::data(store.clone(), "data/second.parquet").await;
        let mut input = snapshot::input(
            store.clone(),
            vec![vec![
                snapshot::entry(&first, 0, 10),
                snapshot::entry(&second, 0, 10),
            ]],
            vec![first, second],
        )
        .await;
        let mut limits = snapshot::limits();
        match case {
            0 => limits.data_files = 1,
            1 => limits.index_bytes = 1,
            _ => input.scope.sequence += 1,
        }
        let result = validate_snapshot_files(store, input, limits).await;
        if case == 2 {
            assert!(matches!(result, Err(SnapshotValidationError::Binding)));
        } else {
            assert!(matches!(result, Err(SnapshotValidationError::Bounds)));
        }
    }
}

#[tokio::test]
async fn sdk_delete_pages_bind_only_applicable_canonical_rows_regardless_of_manifest_order() {
    for bytes in official::files() {
        for case in 0..4 {
            let store = Arc::new(blocks::TestBlocks::default());
            let data = snapshot::data(store.clone(), "data/target.parquet").await;
            let delete = snapshot::store(
                store.clone(),
                "data/delete.parquet",
                ContentFormat::Parquet,
                &bytes,
            )
            .await;
            let mut data_entry = snapshot::entry(&data, 0, 10);
            let mut delete_entry = snapshot::entry(&delete, 1, 100);
            if case == 2 {
                delete_entry.set(3, serde_json::json!(8));
            }
            if case == 3 {
                data_entry.set(3, serde_json::json!(8));
            }
            let mut groups = vec![vec![delete_entry]];
            if case != 1 {
                groups.push(vec![data_entry]);
            }
            let input = snapshot::input(store.clone(), groups, vec![data, delete]).await;
            let result = validate_snapshot_files(store, input, snapshot::limits()).await;
            if case == 0 || case == 3 {
                assert!(
                    matches!(result, Err(SnapshotValidationError::Parquet(_))),
                    "{result:?}"
                );
            } else {
                let summary = result.unwrap();
                assert_eq!(summary.position_rows, 100);
                assert_eq!(summary.applicable_position_rows, 0);
            }
        }
    }
}

#[tokio::test]
async fn dv_payloads_use_canonical_rows_and_supersede_position_deletes() {
    for case in 0..4 {
        let store = Arc::new(blocks::TestBlocks::default());
        let data = snapshot::data(store.clone(), "data/target.parquet").await;
        let blob = dv::blob(&[(0, dv::array(0, &[if case == 1 { 10 } else { 5 }]))]);
        let (vector, references) = dv::record_with_references(
            store.clone(),
            fixture::table(),
            "data/vector.puffin",
            &[("data/target.parquet", &blob, 1)],
        )
        .await;
        let reference = &references[0];
        let mut entry = snapshot::entry(&vector, 1, 1);
        entry.set(143, serde_json::json!(reference.referenced.to_string()));
        entry.set(144, serde_json::json!(reference.span.offset));
        entry.set(145, serde_json::json!(reference.span.length));
        let mut groups = vec![vec![entry]];
        if case != 2 {
            groups.push(vec![snapshot::entry(&data, 0, 10)]);
        }
        let mut files = vec![data, vector];
        if case == 3 {
            let delete = snapshot::store(
                store.clone(),
                "data/old-delete.parquet",
                ContentFormat::Parquet,
                &official::files().remove(0),
            )
            .await;
            groups.insert(0, vec![snapshot::entry(&delete, 1, 100)]);
            files.push(delete);
        }
        let input = snapshot::input(store.clone(), groups, files).await;
        let result = validate_snapshot_files(store, input, snapshot::limits()).await;
        if case == 1 {
            assert!(
                matches!(result, Err(SnapshotValidationError::Vector(_))),
                "{result:?}"
            );
        } else {
            let summary = result.unwrap();
            assert_eq!(summary.vectors, 1);
            assert_eq!(summary.applicable_position_rows, 0);
            assert_eq!(summary.position_rows, if case == 3 { 100 } else { 0 });
        }
    }
}

#[tokio::test]
async fn equality_schemas_and_aggregate_delete_work_are_checked_without_data_targets() {
    for case in 0..3 {
        let store = Arc::new(blocks::TestBlocks::default());
        let first = snapshot::data(store.clone(), "data/equality-first.parquet").await;
        let second = snapshot::data(store.clone(), "data/equality-second.parquet").await;
        let mut first_entry = snapshot::entry(&first, 2, 10);
        let mut second_entry = snapshot::entry(&second, 2, 10);
        first_entry.set(135, serde_json::json!([3]));
        second_entry.set(135, serde_json::json!(if case == 2 { vec![4] } else { vec![3] }));
        let input = snapshot::input(
            store.clone(),
            vec![vec![first_entry, second_entry]],
            vec![first, second],
        )
        .await;
        let mut limits = snapshot::limits();
        limits.delete_rows = if case == 1 { 19 } else { 20 };
        let result = validate_snapshot_files(store, input, limits).await;
        match case {
            0 => {
                let summary = result.unwrap();
                assert_eq!(summary.equality_files, 2);
                assert_eq!(summary.delete_rows, 20);
            }
            1 => assert!(matches!(result, Err(SnapshotValidationError::Bounds))),
            _ => assert!(result.is_err()),
        }
    }
}
