#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/parquet_metadata.rs"]
mod fixture;

use crowdb_access_iceberg::file::{
    read_parquet_metadata, ParquetLogicalType as Logical, ParquetMetadataError, ParquetTimeUnit as Unit,
};
use fixture::{binary, footer, limits, list, number, schema, set, stored, structure, TestFields};

async fn decode(
    id: i16,
    parameters: TestFields,
    physical: Option<i32>,
) -> Result<Logical, ParquetMetadataError> {
    let annotation = structure(&vec![(id, 12, structure(&parameters))]);
    let mut field = vec![
        (3, 5, number(1)),
        (4, 8, binary(b"value")),
        (9, 5, number(3)),
        (10, 12, annotation),
    ];
    match physical {
        Some(physical) => field.push((1, 5, number(i64::from(physical)))),
        None => field.push((5, 5, number(0))),
    }
    let mut fields = footer();
    set(
        &mut fields,
        2,
        list(12, &[schema()[0].clone(), structure(&field)]),
    );
    set(&mut fields, 3, number(0));
    set(&mut fields, 4, list(12, &[]));
    let (store, record) = stored(&structure(&fields), 4).await;
    let mut metadata = read_parquet_metadata(store, &record, limits()).await?;
    Ok(metadata.schema.pop().unwrap().logical_type.unwrap())
}

fn time(adjusted: bool, unit: i16) -> TestFields {
    vec![
        (1, if adjusted { 1 } else { 2 }, vec![]),
        (2, 12, structure(&vec![(unit, 12, vec![0])])),
    ]
}

#[tokio::test]
async fn timestamps_preserve_utc_flags_and_all_time_units() {
    for adjusted_to_utc in [false, true] {
        for (id, unit) in [(1, Unit::Millis), (2, Unit::Micros), (3, Unit::Nanos)] {
            assert_eq!(
                decode(8, time(adjusted_to_utc, id), Some(2)).await.unwrap(),
                Logical::Timestamp {
                    adjusted_to_utc,
                    unit
                }
            );
            assert_eq!(
                decode(7, time(adjusted_to_utc, id), Some(if id == 1 { 1 } else { 2 }))
                    .await
                    .unwrap(),
                Logical::Time {
                    adjusted_to_utc,
                    unit
                }
            );
        }
    }
}

#[tokio::test]
async fn decimal_integer_and_spatial_parameters_are_not_discarded() {
    assert_eq!(
        decode(5, vec![(1, 5, number(2)), (2, 5, number(18))], Some(2))
            .await
            .unwrap(),
        Logical::Decimal {
            scale: 2,
            precision: 18
        }
    );
    for bit_width in [8_i8, 16, 32, 64] {
        for signed in [false, true] {
            assert_eq!(
                decode(
                    10,
                    vec![
                        (1, 3, bit_width.to_ne_bytes().to_vec()),
                        (2, if signed { 1 } else { 2 }, vec![])
                    ],
                    Some(if bit_width == 64 { 2 } else { 1 })
                )
                .await
                .unwrap(),
                Logical::Integer { bit_width, signed }
            );
        }
    }
    assert_eq!(
        decode(16, vec![(1, 3, vec![1])], None).await.unwrap(),
        Logical::Variant {
            specification_version: Some(1)
        }
    );
    assert_eq!(
        decode(17, vec![(1, 8, binary(b"EPSG:4326"))], Some(6))
            .await
            .unwrap(),
        Logical::Geometry {
            crs: Some("EPSG:4326".into())
        }
    );
    assert_eq!(
        decode(18, vec![(2, 5, number(4))], Some(6)).await.unwrap(),
        Logical::Geography {
            crs: None,
            algorithm: Some(4)
        }
    );
}

#[tokio::test]
async fn malformed_required_annotation_fields_and_units_fail_closed() {
    for (id, fields) in [
        (5, vec![(1, 5, number(-1)), (2, 5, number(18))]),
        (5, vec![(1, 5, number(2)), (2, 5, number(1))]),
        (5, vec![(1, 5, number(0)), (2, 5, number(0))]),
        (8, time(false, 4)),
        (
            8,
            vec![(1, 5, number(1)), (2, 12, structure(&vec![(2, 12, vec![0])]))],
        ),
        (8, vec![(1, 1, vec![]), (2, 12, structure(&vec![]))]),
        (
            8,
            vec![
                (1, 1, vec![]),
                (2, 12, structure(&vec![(1, 12, vec![0]), (2, 12, vec![0])])),
            ],
        ),
        (10, vec![(1, 3, vec![7]), (2, 1, vec![])]),
        (10, vec![(1, 5, number(32)), (2, 1, vec![])]),
        (17, vec![(1, 8, binary(&[255]))]),
    ] {
        assert!(decode(id, fields, Some(2)).await.is_err());
    }
    assert_eq!(
        decode(100, vec![], None).await.unwrap(),
        Logical::Unrecognized(100)
    );
}
