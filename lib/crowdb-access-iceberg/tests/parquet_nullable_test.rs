#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/parquet_nullable.rs"]
mod nullable;
#[path = "common/parquet_nullable_official.rs"]
mod official;

use crowdb_access_iceberg::file::{
    read_parquet_nullable_integer_column_for_tests, ParquetMetadataError, ParquetPageLimits,
};
use nullable::TestPage;

#[tokio::test]
async fn official_parquet_java_v1_v2_nested_and_all_null_files_decode() {
    use crowdb_access_iceberg::{
        file::TableLocation,
        key::{CatalogId, TableId},
    };
    for (encoded, all_null) in [
        (official::PARQUET_1_0_FALSE, false),
        (official::PARQUET_1_0_TRUE, true),
        (official::PARQUET_2_0_FALSE, false),
        (official::PARQUET_2_0_TRUE, true),
    ] {
        let bytes = data_encoding::BASE64.decode(encoded.as_bytes()).unwrap();
        let (store, record) = fixture::stored_content(
            &bytes,
            TableLocation {
                catalog: CatalogId::from_bytes(&[1; 16]).unwrap(),
                table: TableId::from_bytes(&[2; 16]).unwrap(),
            },
        )
        .await;
        let expected = if all_null {
            vec![None; 64]
        } else {
            [None, None, Some(11), Some(22)].repeat(16)
        };
        assert_eq!(
            read_parquet_nullable_integer_column_for_tests(store, &record, fixture::limits(), limits(), 64)
                .await
                .unwrap(),
            expected
        );
    }
}

fn limits() -> ParquetPageLimits {
    ParquetPageLimits {
        bytes: 8192,
        values: 100,
        pages: 100,
    }
}

fn page(kind: i64) -> TestPage {
    TestPage {
        kind,
        values: 5,
        nulls: 3,
        encoding: 0,
        level_encoding: 3,
        levels: vec![3, 0x98, 0],
        data: [11_i32, 22].into_iter().flat_map(i32::to_le_bytes).collect(),
        compressed: true,
    }
}

async fn read(
    depth: usize,
    pages: &[TestPage],
    codec: i64,
) -> Result<Vec<Option<i64>>, ParquetMetadataError> {
    let (store, record) = nullable::file(1, depth, pages, codec).await;
    read_parquet_nullable_integer_column_for_tests(store, &record, fixture::limits(), limits(), 100).await
}

#[tokio::test]
async fn nested_definition_levels_preserve_nulls_and_page_boundaries() {
    for kind in [0, 3] {
        for codec in [0, 1, 6] {
            for compressed in [false, true] {
                let mut page = page(kind);
                page.compressed = compressed;
                assert_eq!(
                    read(2, &[page.clone(), page], codec).await.unwrap(),
                    [None, Some(11), None, Some(22), None].repeat(2)
                );
            }
        }
    }
}

#[tokio::test]
async fn v1_legacy_bitpacked_levels_use_most_significant_bit_order_without_length() {
    let mut page = page(0);
    page.level_encoding = 4;
    page.levels = vec![0x26, 0];
    assert_eq!(
        read(2, &[page], 6).await.unwrap(),
        [None, Some(11), None, Some(22), None]
    );
}

#[tokio::test]
async fn dictionary_indices_consume_only_present_values() {
    for kind in [0, 3] {
        let mut dictionary = page(2);
        dictionary.values = 2;
        dictionary.levels.clear();
        let mut page = page(kind);
        page.encoding = 8;
        page.data = vec![1, 3, 2];
        assert_eq!(
            read(2, &[dictionary, page], 6).await.unwrap(),
            [None, Some(11), None, Some(22), None]
        );
    }
}

#[tokio::test]
async fn all_null_pages_need_no_physical_values_or_dictionary() {
    for kind in [0, 3] {
        for encoding in [0, 5, 8, 9] {
            let mut page = page(kind);
            page.values = 3;
            page.nulls = 3;
            page.encoding = encoding;
            page.levels = vec![6, 0];
            page.data.clear();
            assert_eq!(read(1, &[page], 0).await.unwrap(), [None; 3]);
        }
    }
}

#[tokio::test]
async fn invalid_definition_streams_fail_before_page_values_are_returned() {
    for levels in [
        vec![],
        vec![0],
        vec![12, 0],
        vec![10, 3],
        vec![3, 0x98],
        vec![3, 0x98, 0, 0],
        vec![5, 0, 0, 0, 0],
        vec![0xff; 6],
    ] {
        for kind in [0, 3] {
            let mut page = page(kind);
            page.levels = levels.clone();
            assert!(read(2, &[page], 0).await.is_err());
        }
    }
}

#[tokio::test]
async fn v2_declared_null_counts_and_required_column_levels_are_checked() {
    for nulls in [-1, 0, 2, 4, 6] {
        let mut page = page(3);
        page.nulls = nulls;
        assert!(read(2, &[page], 6).await.is_err());
    }
    assert!(read(0, &[page(3)], 0).await.is_err());
}

#[tokio::test]
async fn all_null_pages_still_enforce_value_allocation_and_encoding_limits() {
    let mut page = page(3);
    page.values = 100;
    page.nulls = 100;
    page.data.clear();
    page.levels = vec![200, 1, 0];
    let (store, record) = nullable::file(1, 1, &[page.clone()], 0).await;
    let mut budget = limits();
    budget.bytes = 64;
    assert!(matches!(
        read_parquet_nullable_integer_column_for_tests(store, &record, fixture::limits(), budget, 100).await,
        Err(ParquetMetadataError::Bounds)
    ));
    page.encoding = 99;
    assert!(matches!(
        read(1, &[page], 0).await,
        Err(ParquetMetadataError::Unsupported)
    ));
}
