#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/parquet_nullable.rs"]
#[allow(dead_code)]
mod nullable;
#[path = "common/parquet_scalar_official.rs"]
mod official;

use crowdb_access_iceberg::file::{
    read_parquet_scalar_column_for_tests, ParquetMetadataError, ParquetPageLimits,
};
use nullable::TestPage;

fn limits() -> ParquetPageLimits {
    ParquetPageLimits {
        bytes: 8192,
        values: 100,
        pages: 100,
    }
}

#[tokio::test]
async fn official_java_v1_v2_nullable_scalar_files_decode_with_and_without_dictionary() {
    for encoded in [
        official::PARQUET_1_0_FALSE,
        official::PARQUET_1_0_TRUE,
        official::PARQUET_2_0_FALSE,
        official::PARQUET_2_0_TRUE,
    ] {
        let bytes = data_encoding::BASE64.decode(encoded.as_bytes()).unwrap();
        let (store, record) = fixture::stored_content(
            &bytes,
            crowdb_access_iceberg::file::TableLocation {
                catalog: crowdb_access_iceberg::key::CatalogId::from_bytes(&[1; 16]).unwrap(),
                table: crowdb_access_iceberg::key::TableId::from_bytes(&[2; 16]).unwrap(),
            },
        )
        .await;
        for field in 1..=5 {
            let decoded = read_parquet_scalar_column_for_tests(
                store.clone(),
                &record,
                fixture::limits(),
                ParquetPageLimits {
                    bytes: 64 * 1024,
                    ..limits()
                },
                field,
                8,
            )
            .await
            .unwrap();
            let expected: Vec<_> = (0..8)
                .map(|index| {
                    if index == 0 {
                        return None;
                    }
                    Some(match field {
                        1 => vec![u8::from(index % 2 == 1)],
                        2 => [
                            0_u32,
                            0x8000_0000,
                            0x7fc0_0000,
                            0x7f80_0000,
                            0xff80_0000,
                            1.5_f32.to_bits(),
                            (-3.25_f32).to_bits(),
                            1,
                        ][index]
                            .to_le_bytes()
                            .to_vec(),
                        3 => [
                            0_u64,
                            0x8000_0000_0000_0000,
                            0x7ff8_0000_0000_0000,
                            0x7ff0_0000_0000_0000,
                            0xfff0_0000_0000_0000,
                            1.5_f64.to_bits(),
                            (-3.25_f64).to_bits(),
                            1,
                        ][index]
                            .to_le_bytes()
                            .to_vec(),
                        4 => vec![u8::try_from(index).unwrap(); 16],
                        _ => vec![u8::try_from(index).unwrap(); 2048],
                    })
                })
                .collect();
            assert_eq!(decoded, expected, "field {field}");
        }
    }
}

fn page(kind: i64, encoding: i64, values: i64, data: Vec<u8>) -> TestPage {
    TestPage {
        kind,
        values,
        nulls: 0,
        encoding,
        level_encoding: 3,
        levels: vec![],
        data,
        compressed: true,
    }
}

async fn read(
    physical: i64,
    width: Option<i64>,
    pages: &[TestPage],
    limit: ParquetPageLimits,
) -> Result<Vec<Option<Vec<u8>>>, ParquetMetadataError> {
    let (store, record) = nullable::file_with_type_length(physical, width, 0, pages, 6).await;
    read_parquet_scalar_column_for_tests(store, &record, fixture::limits(), limit, 2, 100).await
}

#[tokio::test]
async fn plain_boolean_uses_lsb_first_and_ignores_unused_padding() {
    for version in [0, 3] {
        assert_eq!(
            read(0, None, &[page(version, 0, 9, vec![0b1010_0101, 0xff])], limits())
                .await
                .unwrap(),
            [1, 0, 1, 0, 0, 1, 0, 1, 1].map(|value| Some(vec![value]))
        );
    }
}

#[tokio::test]
async fn boolean_rle_is_length_prefixed_for_both_page_versions() {
    for version in [0, 3] {
        for (encoded, expected) in [
            (vec![6, 1, 4, 0], vec![1, 1, 1, 0, 0]),
            (vec![3, 0b1110_0101], vec![1, 0, 1, 0, 0]),
        ] {
            let mut data = u32::try_from(encoded.len()).unwrap().to_le_bytes().to_vec();
            data.extend(encoded);
            assert_eq!(
                read(0, None, &[page(version, 3, 5, data)], limits())
                    .await
                    .unwrap(),
                expected
                    .into_iter()
                    .map(|value| Some(vec![value]))
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
async fn float_plain_and_split_preserve_nan_payload_infinity_and_signed_zero() {
    for (physical, values) in [
        (
            4,
            [0x8000_0000_u32, 0, 0x7f80_0000, 0xff80_0000, 0x7fc0_1234]
                .map(|value| value.to_le_bytes().to_vec())
                .to_vec(),
        ),
        (
            5,
            [
                0x8000_0000_0000_0000_u64,
                0,
                0x7ff0_0000_0000_0000,
                0xfff0_0000_0000_0000,
                0x7ff8_0000_0000_1234,
            ]
            .map(|value| value.to_le_bytes().to_vec())
            .to_vec(),
        ),
    ] {
        for version in [0, 3] {
            for encoding in [0, 9] {
                let data = if encoding == 0 {
                    values.concat()
                } else {
                    split(&values)
                };
                assert_eq!(
                    read(physical, None, &[page(version, encoding, 5, data)], limits())
                        .await
                        .unwrap(),
                    values.iter().cloned().map(Some).collect::<Vec<_>>()
                );
            }
        }
    }
}

fn split(values: &[Vec<u8>]) -> Vec<u8> {
    (0..values[0].len())
        .flat_map(|byte| values.iter().map(move |value| value[byte]))
        .collect()
}

#[tokio::test]
async fn fixed_plain_split_and_dictionary_use_declared_width() {
    for width in [3, 16] {
        let values = vec![vec![0; width], vec![255; width], vec![7; width]];
        for version in [0, 3] {
            for encoding in [0, 9] {
                let data = if encoding == 0 {
                    values.concat()
                } else {
                    split(&values)
                };
                assert_eq!(
                    read(
                        7,
                        Some(i64::try_from(width).unwrap()),
                        &[page(version, encoding, 3, data)],
                        limits()
                    )
                    .await
                    .unwrap(),
                    values.iter().cloned().map(Some).collect::<Vec<_>>()
                );
            }
            let dictionary = page(2, 0, 3, values.concat());
            let indices = page(version, 8, 3, vec![2, 3, 0b0010_0100, 0]);
            assert_eq!(
                read(
                    7,
                    Some(i64::try_from(width).unwrap()),
                    &[dictionary, indices],
                    limits()
                )
                .await
                .unwrap(),
                values.iter().cloned().map(Some).collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
async fn float_dictionary_copies_bits_without_numeric_conversion() {
    for (physical, data) in [
        (4, 0x7fc0_1234_u32.to_le_bytes().to_vec()),
        (5, 0x8000_0000_0000_0000_u64.to_le_bytes().to_vec()),
    ] {
        let dictionary = page(2, 0, 1, data.clone());
        let indices = page(3, 8, 3, vec![0, 6]);
        assert_eq!(
            read(physical, None, &[dictionary, indices], limits())
                .await
                .unwrap(),
            vec![Some(data); 3]
        );
    }
}

fn delta(first: i64, count: u8) -> Vec<u8> {
    let mut bytes = fixture::unsigned(128);
    bytes.extend([4, count]);
    bytes.extend(fixture::number(first));
    if count > 1 {
        bytes.extend([0; 5]);
    }
    bytes
}

#[tokio::test]
async fn fixed_delta_byte_arrays_validate_every_reconstructed_length() {
    for version in [0, 3] {
        let mut data = delta(0, 3);
        data.extend(delta(3, 3));
        data.extend(b"abcdefghi");
        assert_eq!(
            read(7, Some(3), &[page(version, 7, 3, data.clone())], limits())
                .await
                .unwrap(),
            [b"abc", b"def", b"ghi"].map(|value| Some(value.to_vec()))
        );
        assert!(read(7, Some(4), &[page(version, 7, 3, data)], limits())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn large_binary_values_use_page_budget_not_delete_path_length() {
    let expected = vec![42; 2048];
    let mut data = 2048_i32.to_le_bytes().to_vec();
    data.extend(&expected);
    assert_eq!(
        read(6, None, &[page(0, 0, 1, data.clone())], limits())
            .await
            .unwrap(),
        vec![Some(expected)]
    );
    assert!(read(
        6,
        None,
        &[page(0, 0, 1, data)],
        ParquetPageLimits {
            bytes: 2048,
            ..limits()
        }
    )
    .await
    .is_err());
}

#[tokio::test]
async fn malformed_and_incompatible_scalar_payloads_fail_closed() {
    for (physical, width, encoding, data) in [
        (0, None, 0, vec![]),
        (0, None, 0, vec![0, 0]),
        (0, None, 3, vec![2, 0, 0, 0, 0, 1]),
        (0, None, 3, vec![2, 0, 0, 0, 2, 2]),
        (0, None, 3, vec![2, 0, 0, 0, 4, 1]),
        (0, None, 3, vec![3, 0, 0, 0, 2, 1]),
        (0, None, 3, vec![2, 0, 0, 0, 2, 1, 0]),
        (4, None, 0, vec![0; 3]),
        (5, None, 0, vec![0; 9]),
        (4, None, 5, delta(0, 1)),
        (5, None, 3, vec![0; 8]),
        (7, Some(3), 0, vec![0; 2]),
        (7, Some(3), 9, vec![0; 4]),
        (7, None, 0, vec![0; 3]),
        (7, Some(0), 0, vec![]),
        (7, Some(1_048_577), 0, vec![]),
        (6, None, 0, (-1_i32).to_le_bytes().to_vec()),
    ] {
        assert!(
            read(physical, width, &[page(3, encoding, 1, data)], limits())
                .await
                .is_err(),
            "physical={physical}, width={width:?}, encoding={encoding}"
        );
    }
}

#[tokio::test]
async fn dictionary_expansion_and_fixed_values_obey_materialization_budget() {
    let dictionary = page(2, 0, 1, vec![4; 128]);
    let indices = page(3, 8, 100, vec![0, 200, 1]);
    assert!(read(7, Some(128), &[dictionary, indices], limits())
        .await
        .is_err());
    assert!(read(7, Some(128), &[page(0, 0, 64, vec![4; 8192])], limits())
        .await
        .is_err());
}
