#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/deletion_vector.rs"]
mod fixtures;

use crowdb_access_iceberg::file::{
    read_deletion_vector_positions, DeletionVectorError, DeletionVectorLimits,
};
use std::sync::Arc;

fn limits() -> DeletionVectorLimits {
    DeletionVectorLimits {
        blob_bytes: 1024 * 1024,
        bitmaps: 10,
    }
}

#[tokio::test]
async fn adjacent_outer_bitmaps_share_one_range_and_empty_vectors_keep_target_identity() {
    let store = Arc::new(blocks::TestBlocks::default());
    let bytes = fixtures::blob(&[
        (0, fixtures::array(65535, &[65535])),
        (1, fixtures::array(0, &[0])),
    ]);
    let (record, reference) = fixtures::record(store.clone(), &bytes, 2).await;
    let positions = read_deletion_vector_positions(store.clone(), &record, &reference, limits(), 1)
        .await
        .unwrap();
    assert_eq!(positions.range_count(), 1);
    assert!(positions.contains(&reference.referenced, (1_u64 << 32) - 1));
    assert!(positions.contains(&reference.referenced, 1_u64 << 32));
    let (record, references) = fixtures::record_with_references(
        store.clone(),
        reference.referenced.table(),
        "empty.puffin",
        &[
            ("data.parquet", &fixtures::blob(&[]), 0),
            ("foreign.parquet", &fixtures::blob(&[]), 0),
        ],
    )
    .await;
    let empty = read_deletion_vector_positions(store.clone(), &record, &references[0], limits(), 1)
        .await
        .unwrap();
    let foreign = read_deletion_vector_positions(store, &record, &references[1], limits(), 1)
        .await
        .unwrap();
    assert_eq!(empty.range_count(), 0);
    assert!(positions.covers(&empty));
    assert!(!empty.covers(&positions));
    assert!(!positions.covers(&foreign));
    assert!(!empty.covers(&foreign));
}

#[tokio::test]
async fn ranges_preserve_membership_across_container_encodings_and_outer_keys() {
    let store = Arc::new(blocks::TestBlocks::default());
    let bytes = fixtures::blob(&[
        (0, fixtures::array(0, &[0, 1, 3, 65535])),
        (1, fixtures::runs(0, 65536, &[(0, 65535)])),
        (2, fixtures::bitset(0)),
        (0x7fff_ffff, fixtures::array(65535, &[65535])),
    ]);
    let (record, reference) = fixtures::record(store.clone(), &bytes, 69638).await;
    let positions = read_deletion_vector_positions(store, &record, &reference, limits(), 6)
        .await
        .unwrap();
    for position in [
        0,
        1,
        3,
        65535,
        1_u64 << 32,
        (1_u64 << 32) + 65535,
        2_u64 << 32,
        (2_u64 << 32) + 4096,
        i64::MAX as u64,
    ] {
        assert!(positions.contains(&reference.referenced, position));
    }
    for position in [
        2,
        65534,
        (1_u64 << 32) - 1,
        (1_u64 << 32) + 65536,
        (2_u64 << 32) + 4097,
        u64::MAX,
    ] {
        assert!(!positions.contains(&reference.referenced, position));
    }
    assert_eq!(positions.stats().cardinality, 69638);
    assert!(positions.covers(&positions));
}

#[tokio::test]
async fn replacement_must_cover_every_prior_range_and_the_same_target() {
    let store = Arc::new(blocks::TestBlocks::default());
    let (record, reference) = fixtures::record(
        store.clone(),
        &fixtures::blob(&[(0, fixtures::runs(0, 10, &[(10, 9)]))]),
        10,
    )
    .await;
    let previous = read_deletion_vector_positions(store.clone(), &record, &reference, limits(), 1)
        .await
        .unwrap();
    for (values, covers) in [
        ((9..21).collect::<Vec<u16>>(), true),
        ((10..19).collect(), false),
        ((11..20).collect(), false),
        (vec![10, 11, 12, 14, 15, 16, 17, 18, 19], false),
    ] {
        let (record, references) = fixtures::record_with_references(
            store.clone(),
            reference.referenced.table(),
            "replacement.puffin",
            &[(
                "data.parquet",
                &fixtures::blob(&[(0, fixtures::array(0, &values))]),
                values.len() as u64,
            )],
        )
        .await;
        let replacement =
            read_deletion_vector_positions(store.clone(), &record, &references[0], limits(), 10)
                .await
                .unwrap();
        assert_eq!(replacement.covers(&previous), covers);
    }
    let foreign = reference.referenced.table().file("data/foreign.parquet").unwrap();
    assert!(!previous.contains(&foreign, 10));
}

#[tokio::test]
async fn ranges_are_bounded_after_coalescing_and_never_escape_a_corrupt_vector() {
    let store = Arc::new(blocks::TestBlocks::default());
    for values in [vec![0, 1, 2], vec![0, 2, 4]] {
        let bytes = fixtures::blob(&[(0, fixtures::array(0, &values))]);
        let (record, reference) = fixtures::record(store.clone(), &bytes, 3).await;
        let result = read_deletion_vector_positions(store.clone(), &record, &reference, limits(), 1).await;
        if values[1] == 1 {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(DeletionVectorError::Bounds)));
        }
        for bound in [0, 1_000_001] {
            assert!(matches!(
                read_deletion_vector_positions(store.clone(), &record, &reference, limits(), bound).await,
                Err(DeletionVectorError::Bounds)
            ));
        }
        let mut corrupt = bytes;
        *corrupt.last_mut().unwrap() ^= 1;
        let (record, reference) = fixtures::record(store.clone(), &corrupt, 3).await;
        assert!(matches!(
            read_deletion_vector_positions(store.clone(), &record, &reference, limits(), 10).await,
            Err(DeletionVectorError::Invalid)
        ));
    }
}
