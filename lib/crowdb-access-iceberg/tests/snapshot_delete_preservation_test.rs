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

use crowdb_access_iceberg::{
    file::{ContentFormat, FileRecord},
    manifest::{validate_snapshot_delete_preservation, SnapshotValidationInput},
};
use std::sync::Arc;

async fn input(
    store: Arc<blocks::TestBlocks>,
    data: Option<&FileRecord>,
    deleted: Option<&[u16]>,
) -> SnapshotValidationInput {
    let mut groups = Vec::new();
    let mut files = Vec::new();
    if let Some(data) = data {
        groups.push(vec![snapshot::entry(data, 0, 10)]);
        files.push(data.clone());
    }
    if let Some(deleted) = deleted {
        let blob = dv::blob(&[(0, dv::array(0, deleted))]);
        let (record, references) = dv::record_with_references(
            store.clone(),
            fixture::table(),
            "data/vector.puffin",
            &[("data/target.parquet", &blob, deleted.len() as u64)],
        )
        .await;
        let mut entry = snapshot::entry(&record, 1, i64::try_from(deleted.len()).unwrap());
        entry.set(143, serde_json::json!(references[0].referenced.to_string()));
        entry.set(144, serde_json::json!(references[0].span.offset));
        entry.set(145, serde_json::json!(references[0].span.length));
        groups.push(vec![entry]);
        files.push(record);
    }
    snapshot::input(store, groups, files).await
}

#[tokio::test]
async fn replacement_vectors_must_include_positions_from_official_sdk_delete_pages() {
    for bytes in official::files() {
        let store = Arc::new(blocks::TestBlocks::default());
        let mut column = parquet::column();
        column.iter_mut().find(|field| field.0 == 5).unwrap().2 = parquet::number(100);
        let mut footer = parquet::footer();
        footer.iter_mut().find(|field| field.0 == 3).unwrap().2 = parquet::number(100);
        footer.iter_mut().find(|field| field.0 == 4).unwrap().2 =
            parquet::list(12, &[parquet::row_group(100, &column)]);
        let footer = parquet::structure(&footer);
        let mut data_bytes = b"PAR1".to_vec();
        data_bytes.resize(32, 0);
        data_bytes.extend(&footer);
        data_bytes.extend(u32::try_from(footer.len()).unwrap().to_le_bytes());
        data_bytes.extend(b"PAR1");
        let data = snapshot::store(
            store.clone(),
            "data/target.parquet",
            ContentFormat::Parquet,
            &data_bytes,
        )
        .await;
        let delete = snapshot::store(
            store.clone(),
            "data/old-delete.parquet",
            ContentFormat::Parquet,
            &bytes,
        )
        .await;
        let prior = snapshot::input(
            store.clone(),
            vec![
                vec![snapshot::entry(&data, 0, 100)],
                vec![snapshot::entry(&delete, 1, 100)],
            ],
            vec![data.clone(), delete],
        )
        .await;
        for complete in [true, false] {
            let values: Vec<u16> = (0..100).filter(|value| complete || *value != 50).collect();
            let blob = dv::blob(&[(0, dv::array(0, &values))]);
            let (record, references) = dv::record_with_references(
                store.clone(),
                fixture::table(),
                "data/vector.puffin",
                &[("data/target.parquet", &blob, values.len() as u64)],
            )
            .await;
            let mut entry = snapshot::entry(&record, 1, i64::try_from(values.len()).unwrap());
            entry.set(143, serde_json::json!(references[0].referenced.to_string()));
            entry.set(144, serde_json::json!(references[0].span.offset));
            entry.set(145, serde_json::json!(references[0].span.length));
            let mut candidate = snapshot::input(
                store.clone(),
                vec![vec![entry], vec![snapshot::entry(&data, 0, 100)]],
                vec![data.clone(), record],
            )
            .await;
            child(&mut candidate);
            let result = validate_snapshot_delete_preservation(
                store.clone(),
                &prior,
                &candidate,
                snapshot::limits(),
                10,
            )
            .await;
            assert_eq!(result.is_ok(), complete, "{result:?}");
        }
    }
}

fn child(input: &mut SnapshotValidationInput) {
    input.scope.snapshot_id = 100;
    input.scope.sequence = 10;
    input.selection.snapshot_id = 100;
    input.selection.sequence = 10;
    input.selection.parent_snapshot_id = Some(99);
}

#[tokio::test]
async fn surviving_data_requires_a_complete_replacement_vector() {
    let store = Arc::new(blocks::TestBlocks::default());
    let data = snapshot::data(store.clone(), "data/target.parquet").await;
    let prior = input(store.clone(), Some(&data), Some(&[1, 3])).await;
    for (deleted, expected) in [
        (Some(vec![0, 1, 2, 3]), true),
        (Some(vec![1, 3]), true),
        (Some(vec![1, 2]), false),
        (None, false),
    ] {
        let mut candidate = input(store.clone(), Some(&data), deleted.as_deref()).await;
        child(&mut candidate);
        let result =
            validate_snapshot_delete_preservation(store.clone(), &prior, &candidate, snapshot::limits(), 10)
                .await;
        assert_eq!(result.is_ok(), expected, "{result:?}");
    }
}

#[tokio::test]
async fn removed_data_does_not_need_a_replacement_but_orphan_vectors_are_rejected() {
    let store = Arc::new(blocks::TestBlocks::default());
    let data = snapshot::data(store.clone(), "data/target.parquet").await;
    let prior = input(store.clone(), Some(&data), Some(&[1, 3])).await;
    for deleted in [None, Some(&[1, 3][..])] {
        let mut candidate = input(store.clone(), None, deleted).await;
        child(&mut candidate);
        let result =
            validate_snapshot_delete_preservation(store.clone(), &prior, &candidate, snapshot::limits(), 10)
                .await;
        assert_eq!(result.is_ok(), deleted.is_none(), "{result:?}");
    }
}

#[tokio::test]
async fn lineage_identity_and_range_limits_fail_before_returning_a_proof() {
    let store = Arc::new(blocks::TestBlocks::default());
    let data = snapshot::data(store.clone(), "data/target.parquet").await;
    let prior = input(store.clone(), Some(&data), Some(&[1, 3])).await;
    let mut candidate = input(store.clone(), Some(&data), Some(&[1, 3])).await;
    assert!(
        validate_snapshot_delete_preservation(store.clone(), &prior, &candidate, snapshot::limits(), 10)
            .await
            .is_err()
    );
    child(&mut candidate);
    for limit in [0, 1, 1_000_001] {
        assert!(validate_snapshot_delete_preservation(
            store.clone(),
            &prior,
            &candidate,
            snapshot::limits(),
            limit
        )
        .await
        .is_err());
    }
    let replacement = snapshot::data(store.clone(), "data/target.parquet").await;
    let mut candidate = input(store.clone(), Some(&replacement), Some(&[1, 3])).await;
    child(&mut candidate);
    assert!(
        validate_snapshot_delete_preservation(store, &prior, &candidate, snapshot::limits(), 10)
            .await
            .is_err()
    );
}
