#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/parquet_metadata.rs"]
mod fixture;
#[path = "common/parquet_official_footer.rs"]
mod official;

use crowdb_access_iceberg::file::{read_parquet_metadata, FormatHint, ParquetMetadataError as Error};
use fixture::{column, footer, limits, list, number, row_group, schema, set, stored, structure};

#[tokio::test]
async fn apache_alltypes_plain_footer_decodes_standard_delta_field_headers() {
    let footer = official::bytes();
    assert_eq!(footer.len(), 730);
    let (store, record) = stored(&footer, 1113).await;
    let metadata = read_parquet_metadata(store, &record, limits()).await.unwrap();
    assert_eq!((metadata.rows, metadata.row_groups), (8, 1));
    assert_eq!(metadata.schema.len(), 12);
    assert_eq!(metadata.schema[11].name, "timestamp_col");
    assert_eq!(metadata.schema[11].physical_type, Some(3));
}

#[tokio::test]
async fn canonical_footer_decodes_long_form_fields_and_ignores_stored_hints() {
    let mut fields = footer();
    fields.reverse();
    let (store, mut record) = stored(&structure(&fields), 64).await;
    record.hint = Some(FormatHint { offset: 0, length: 1 });
    let metadata = read_parquet_metadata(store, &record, limits()).await.unwrap();
    assert_eq!((metadata.rows, metadata.row_groups), (10, 1));
    assert_eq!(metadata.schema.len(), 2);
    assert_eq!(metadata.schema[1].field_id, Some(3));
    assert_eq!(metadata.schema[1].physical_type, Some(2));
    assert_eq!(metadata.schema[1].name, "value");
}

#[tokio::test]
async fn footer_rows_must_match_all_row_groups_and_required_types() {
    for invalid in 0..5 {
        let mut fields = footer();
        match invalid {
            0 => set(&mut fields, 3, number(11)),
            1 => set(&mut fields, 3, number(-1)),
            2 => fields.retain(|field| field.0 != 3),
            3 => fields.iter_mut().find(|field| field.0 == 3).unwrap().1 = 5,
            _ => fields.push((3, 6, number(10))),
        }
        let (store, record) = stored(&structure(&fields), 64).await;
        assert!(read_parquet_metadata(store, &record, limits()).await.is_err());
    }
    let mut fields = footer();
    set(&mut fields, 3, number(20));
    set(
        &mut fields,
        4,
        list(12, &[row_group(10, &column()), row_group(10, &column())]),
    );
    let (store, record) = stored(&structure(&fields), 64).await;
    assert_eq!(
        read_parquet_metadata(store, &record, limits())
            .await
            .unwrap()
            .row_groups,
        2
    );
}

#[tokio::test]
async fn compact_wire_corruption_and_independent_resource_limits_fail_closed() {
    let bytes = structure(&footer());
    for end in 0..bytes.len() {
        let (store, record) = stored(&bytes[..end], 64).await;
        assert!(
            read_parquet_metadata(store, &record, limits()).await.is_err(),
            "truncation {end}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    let (store, record) = stored(&trailing, 64).await;
    assert!(read_parquet_metadata(store, &record, limits()).await.is_err());
    for bound in 0..5 {
        let (store, record) = stored(&bytes, 64).await;
        let mut limits = limits();
        match bound {
            0 => limits.footer_bytes = bytes.len() - 1,
            1 => limits.values = 3,
            2 => limits.depth = 1,
            3 => limits.schema_elements = 1,
            _ => limits.row_groups = 0,
        }
        assert!(matches!(
            read_parquet_metadata(store, &record, limits).await,
            Err(Error::Bounds)
        ));
    }
}

#[tokio::test]
async fn schema_preorder_and_column_offsets_are_checked_before_exposing_counts() {
    for invalid in 0..5 {
        let mut fields = footer();
        match invalid {
            0 => set(&mut fields, 2, list(12, &schema()[..1])),
            1 => {
                let mut nodes = schema();
                nodes.push(nodes[1].clone());
                set(&mut fields, 2, list(12, &nodes));
            }
            _ => {
                let mut column = column();
                let (id, value) = match invalid {
                    2 => (1, 1),
                    3 => (9, 100),
                    _ => (7, i64::MAX),
                };
                set(&mut column, id, number(value));
                set(&mut fields, 4, list(12, &[row_group(10, &column)]));
            }
        }
        let (store, record) = stored(&structure(&fields), 64).await;
        assert!(read_parquet_metadata(store, &record, limits()).await.is_err());
    }
}

#[tokio::test]
async fn unknown_fields_are_bounded_and_boolean_collections_consume_their_values() {
    for boolean in [1, 2, 3] {
        let mut fields = footer();
        fields.push((100, 9, vec![0x11, boolean]));
        let (store, record) = stored(&structure(&fields), 64).await;
        assert_eq!(
            read_parquet_metadata(store, &record, limits()).await.is_ok(),
            boolean != 3
        );
    }
    let mut fields = footer();
    fields.push((8, 12, vec![0]));
    let (store, record) = stored(&structure(&fields), 64).await;
    assert!(matches!(
        read_parquet_metadata(store, &record, limits()).await,
        Err(Error::Unsupported)
    ));
}

#[tokio::test]
async fn malformed_compact_integer_lengths_and_set_substitution_are_rejected() {
    for invalid in 0..5 {
        let mut fields = footer();
        match invalid {
            0 => set(&mut fields, 1, vec![255; 10]),
            1 => set(&mut fields, 1, number(i64::MAX)),
            2 => {
                let mut payload = vec![0xfc];
                payload.extend(fixture::unsigned(u64::MAX));
                set(&mut fields, 2, payload);
            }
            3 => fields.iter_mut().find(|field| field.0 == 2).unwrap().1 = 10,
            _ => fields.push((100, 8, fixture::unsigned(u64::MAX))),
        }
        let (store, record) = stored(&structure(&fields), 64).await;
        assert!(read_parquet_metadata(store, &record, limits()).await.is_err());
    }
}
