#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/parquet_deletes.rs"]
#[allow(dead_code)]
mod deletes;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/parquet_integers.rs"]
mod integers;

use crowdb_access_iceberg::file::{read_parquet_integer_column_for_tests, ParquetMetadataError};

fn plain(values: &[i32]) -> Vec<u8> {
    values.iter().flat_map(|value| value.to_le_bytes()).collect()
}

fn delta(first: i64, increment: i64, count: u8) -> Vec<u8> {
    let mut bytes = fixture::unsigned(128);
    bytes.extend([4, count]);
    bytes.extend(fixture::number(first));
    bytes.extend(fixture::number(increment));
    bytes.extend([0; 4]);
    bytes
}

#[tokio::test]
async fn int32_plain_and_split_preserve_signed_extremes_across_pages_and_codecs() {
    let expected = [i32::MIN, -1, 0, 1, i32::MAX];
    for version in [0, 3] {
        for codec in [0, 1, 2, 6, 7] {
            for encoding in [0, 9] {
                let mut payload = plain(&expected);
                if encoding == 9 {
                    payload = (0..4)
                        .flat_map(|byte| expected.iter().map(move |value| value.to_le_bytes()[byte]))
                        .collect();
                }
                let (store, record) = integers::file(
                    1,
                    encoding,
                    &[(5, payload.clone()), (5, payload)],
                    None,
                    version,
                    codec,
                )
                .await;
                let decoded = read_parquet_integer_column_for_tests(
                    store,
                    &record,
                    fixture::limits(),
                    deletes::delete_limits().page,
                    10,
                )
                .await
                .unwrap();
                assert_eq!(
                    decoded,
                    expected.repeat(2).into_iter().map(i64::from).collect::<Vec<_>>()
                );
            }
        }
    }
}

#[tokio::test]
async fn int32_dictionary_decodes_rle_and_bitpacked_indices() {
    for encoding in [2, 8] {
        for (indices, expected) in [
            (vec![2, 6, 2], vec![i64::from(i32::MAX); 3]),
            (
                vec![2, 3, 0b0010_0100, 0],
                vec![i64::from(i32::MIN), -1, i64::from(i32::MAX)],
            ),
        ] {
            let (store, record) = integers::file(
                1,
                encoding,
                &[(3, indices)],
                Some((3, plain(&[i32::MIN, -1, i32::MAX]))),
                3,
                6,
            )
            .await;
            assert_eq!(
                read_parquet_integer_column_for_tests(
                    store,
                    &record,
                    fixture::limits(),
                    deletes::delete_limits().page,
                    3
                )
                .await
                .unwrap(),
                expected
            );
        }
    }
}

#[tokio::test]
async fn int32_delta_wraps_at_physical_width_not_host_width() {
    for (first, increment, expected) in [
        (i32::MAX, 1, [i32::MAX, i32::MIN, i32::MIN + 1]),
        (i32::MIN, -1, [i32::MIN, i32::MAX, i32::MAX - 1]),
        (0, i32::MIN, [0, i32::MIN, 0]),
    ] {
        for version in [0, 3] {
            let (store, record) = integers::file(
                1,
                5,
                &[(3, delta(i64::from(first), i64::from(increment), 3))],
                None,
                version,
                1,
            )
            .await;
            assert_eq!(
                read_parquet_integer_column_for_tests(
                    store,
                    &record,
                    fixture::limits(),
                    deletes::delete_limits().page,
                    3
                )
                .await
                .unwrap(),
                expected.map(i64::from)
            );
        }
    }
}

#[tokio::test]
async fn int32_delta_accepts_full_width_residuals_and_unused_miniblock_padding() {
    let mut payload = delta(0, i64::from(i32::MIN), 3);
    let widths = payload.len() - 4;
    payload[widths..].copy_from_slice(&[32, 255, 255, 255]);
    payload.extend(0_u32.to_le_bytes());
    payload.extend([255; 31 * 4]);
    let (store, record) = integers::file(1, 5, &[(3, payload)], None, 3, 6).await;
    assert_eq!(
        read_parquet_integer_column_for_tests(
            store,
            &record,
            fixture::limits(),
            deletes::delete_limits().page,
            3,
        )
        .await
        .unwrap(),
        [0, i64::from(i32::MIN), -1]
    );
}

#[tokio::test]
async fn int32_delta_rejects_out_of_width_header_values_and_miniblocks() {
    let mut wide = delta(0, 0, 3);
    let index = wide.len() - 4;
    wide[index] = 33;
    for payload in [
        delta(i64::from(i32::MAX) + 1, 0, 3),
        delta(0, i64::from(i32::MIN) - 1, 3),
        wide,
    ] {
        let (store, record) = integers::file(1, 5, &[(3, payload)], None, 0, 0).await;
        assert!(matches!(
            read_parquet_integer_column_for_tests(
                store,
                &record,
                fixture::limits(),
                deletes::delete_limits().page,
                3
            )
            .await,
            Err(ParquetMetadataError::Invalid)
        ));
    }
}

#[tokio::test]
async fn int32_rejects_truncation_trailing_bytes_and_out_of_range_dictionary_ids() {
    for encoding in [0, 9] {
        for payload in [vec![0; 11], vec![0; 13]] {
            let (store, record) = integers::file(1, encoding, &[(3, payload)], None, 3, 0).await;
            assert!(matches!(
                read_parquet_integer_column_for_tests(
                    store,
                    &record,
                    fixture::limits(),
                    deletes::delete_limits().page,
                    3
                )
                .await,
                Err(ParquetMetadataError::Invalid)
            ));
        }
    }
    let (store, record) =
        integers::file(1, 8, &[(3, vec![2, 6, 3])], Some((3, plain(&[1, 2, 3]))), 0, 0).await;
    assert!(matches!(
        read_parquet_integer_column_for_tests(
            store,
            &record,
            fixture::limits(),
            deletes::delete_limits().page,
            3
        )
        .await,
        Err(ParquetMetadataError::Invalid)
    ));
}

#[tokio::test]
async fn int32_keeps_independent_page_value_and_materialized_byte_bounds() {
    let (store, record) = integers::file(1, 0, &[(3, plain(&[1, 2, 3])); 1], None, 0, 0).await;
    for (bytes, values) in [(24, 3), (1024, 2)] {
        let mut limits = deletes::delete_limits().page;
        limits.bytes = bytes;
        limits.values = values;
        assert!(matches!(
            read_parquet_integer_column_for_tests(store.clone(), &record, fixture::limits(), limits, 3).await,
            Err(ParquetMetadataError::Bounds)
        ));
    }
}
