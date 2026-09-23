#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/deletion_vector.rs"]
mod fixtures;

use crowdb_access_iceberg::file::{validate_deletion_vector, DeletionVectorError, DeletionVectorLimits};
use std::sync::Arc;

fn limits() -> DeletionVectorLimits {
    DeletionVectorLimits {
        blob_bytes: 1024 * 1024,
        bitmaps: 10,
    }
}

#[tokio::test]
async fn deletion_vectors_stream_arrays_runs_and_bitsets_with_exact_cardinality_and_maximum() {
    let store = Arc::new(blocks::TestBlocks::default());
    let bytes = fixtures::blob(&[
        (0, fixtures::array(1, &[0, 5, 65535])),
        (1, fixtures::runs(2, 6, &[(3, 4), (20, 0)])),
        (2, fixtures::bitset(3)),
    ]);
    let (record, reference) = fixtures::record(store.clone(), &bytes, 4106).await;
    let result = validate_deletion_vector(store.clone(), &record, &reference, limits())
        .await
        .unwrap();
    assert_eq!(result.cardinality, 4106);
    assert_eq!(result.bitmaps, 3);
    assert_eq!(
        result.maximum_position,
        Some((2_u64 << 32) | (3_u64 << 16) | 4096)
    );
    let bytes = fixtures::blob(&[(0x7fff_ffff, fixtures::array(65535, &[65535]))]);
    let (record, reference) = fixtures::record(store.clone(), &bytes, 1).await;
    assert_eq!(
        validate_deletion_vector(store.clone(), &record, &reference, limits())
            .await
            .unwrap()
            .maximum_position,
        Some(i64::MAX as u64)
    );
    let (record, reference) = fixtures::record(store.clone(), &fixtures::blob(&[]), 0).await;
    let result = validate_deletion_vector(store, &record, &reference, limits())
        .await
        .unwrap();
    assert_eq!(
        (result.cardinality, result.maximum_position, result.bitmaps),
        (0, None, 0)
    );
}

#[tokio::test]
async fn malformed_bitmap_offsets_keys_cardinalities_and_runs_fail_even_with_a_valid_crc() {
    let store = Arc::new(blocks::TestBlocks::default());
    let mut wrong_offset = fixtures::array(0, &[1, 2]);
    wrong_offset[12..16].copy_from_slice(&17_u32.to_le_bytes());
    let mut wrong_cardinality = fixtures::bitset(0);
    wrong_cardinality[10..12].copy_from_slice(&4097_u16.to_le_bytes());
    let mut excessive = 12346_u32.to_le_bytes().to_vec();
    excessive.extend(65_537_u32.to_le_bytes());
    for bitmap in [
        fixtures::array(0, &[2, 2]),
        fixtures::array(0, &[2, 1]),
        wrong_offset,
        wrong_cardinality,
        fixtures::runs(0, 4, &[(10, 2), (12, 0)]),
        fixtures::runs(0, 2, &[(65535, 1)]),
        fixtures::runs(0, 3, &[(10, 1)]),
        excessive,
        vec![0; 4],
    ] {
        let bytes = fixtures::blob(&[(0, bitmap)]);
        let (record, reference) = fixtures::record(store.clone(), &bytes, 2).await;
        assert!(
            validate_deletion_vector(store.clone(), &record, &reference, limits())
                .await
                .is_err()
        );
    }
    for bitmaps in [
        vec![(1, fixtures::array(0, &[1])), (1, fixtures::array(0, &[2]))],
        vec![(2, fixtures::array(0, &[1])), (1, fixtures::array(0, &[2]))],
        vec![(0x8000_0000, fixtures::array(0, &[1]))],
    ] {
        let bytes = fixtures::blob(&bitmaps);
        let (record, reference) = fixtures::record(store.clone(), &bytes, 2).await;
        assert!(
            validate_deletion_vector(store.clone(), &record, &reference, limits())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn vector_length_magic_checksum_and_manifest_counts_cannot_hide_corruption() {
    let store = Arc::new(blocks::TestBlocks::default());
    let valid = fixtures::blob(&[(0, fixtures::array(0, &[1, 2]))]);
    for offset in [0, 4, valid.len() - 1] {
        let mut invalid = valid.clone();
        invalid[offset] ^= 1;
        let (record, reference) = fixtures::record(store.clone(), &invalid, 2).await;
        assert!(
            validate_deletion_vector(store.clone(), &record, &reference, limits())
                .await
                .is_err()
        );
    }
    let (record, reference) = fixtures::record(store.clone(), &valid, 3).await;
    assert!(
        validate_deletion_vector(store.clone(), &record, &reference, limits())
            .await
            .is_err()
    );
    let (record, reference) = fixtures::record(store.clone(), &valid, 2).await;
    assert!(matches!(
        validate_deletion_vector(
            store.clone(),
            &record,
            &reference,
            DeletionVectorLimits {
                blob_bytes: valid.len() as u64 - 1,
                ..limits()
            }
        )
        .await,
        Err(DeletionVectorError::Bounds)
    ));
    assert!(matches!(
        validate_deletion_vector(
            store.clone(),
            &record,
            &reference,
            DeletionVectorLimits {
                bitmaps: 0,
                ..limits()
            }
        )
        .await,
        Err(DeletionVectorError::Bounds)
    ));
    let bytes = fixtures::blob(&[(0, fixtures::array(0, &[1])), (1, fixtures::array(0, &[2]))]);
    let (record, reference) = fixtures::record(store.clone(), &bytes, 2).await;
    assert!(matches!(
        validate_deletion_vector(
            store,
            &record,
            &reference,
            DeletionVectorLimits {
                bitmaps: 1,
                ..limits()
            }
        )
        .await,
        Err(DeletionVectorError::Bounds)
    ));
}

#[tokio::test]
async fn portable_container_boundaries_and_run_offset_headers_are_checked() {
    let store = Arc::new(blocks::TestBlocks::default());
    let mut four_runs = (0x303b_u32 | (3 << 16)).to_le_bytes().to_vec();
    four_runs.push(15);
    for key in [0_u16, 2, 5, 8] {
        four_runs.extend(key.to_le_bytes());
        four_runs.extend(0_u16.to_le_bytes());
    }
    for offset in [37_u32, 43, 49, 55] {
        four_runs.extend(offset.to_le_bytes());
    }
    for value in [1_u16, 3, 5, 7] {
        four_runs.extend(1_u16.to_le_bytes());
        four_runs.extend(value.to_le_bytes());
        four_runs.extend(0_u16.to_le_bytes());
    }
    let values: Vec<u16> = (0..4096).collect();
    let bytes = fixtures::blob(&[
        (0, fixtures::array(0, &values)),
        (1, four_runs.clone()),
        (2, fixtures::runs(65535, 65536, &[(0, 65535)])),
    ]);
    let (record, reference) = fixtures::record(store.clone(), &bytes, 4096 + 4 + 65536).await;
    let result = validate_deletion_vector(store.clone(), &record, &reference, limits())
        .await
        .unwrap();
    assert_eq!(result.maximum_position, Some((3_u64 << 32) - 1));
    four_runs[9..11].copy_from_slice(&0_u16.to_le_bytes());
    let bytes = fixtures::blob(&[(0, four_runs)]);
    let (record, reference) = fixtures::record(store.clone(), &bytes, 4).await;
    assert!(validate_deletion_vector(store, &record, &reference, limits())
        .await
        .is_err());
}
