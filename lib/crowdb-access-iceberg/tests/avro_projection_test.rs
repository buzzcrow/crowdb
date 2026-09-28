use crowdb_access_iceberg::file::{
    AvroContainerError, AvroDatumLimits, AvroProjection, AvroScalar, AvroSchema,
};

fn limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 64,
        values: 1000,
        value_bytes: 1024,
    }
}

fn schema() -> AvroSchema {
    AvroSchema::parse(
        br#"{"type":"record","name":"Manifest","fields":[
        {"name":"renamed_path","field-id":500,"type":"string"},
        {"name":"metrics","field-id":507,"type":{"type":"array","items":"long"}},
        {"name":"renamed_length","field-id":501,"type":"long"},
        {"name":"renamed_content","field-id":517,"type":["int","null"]}
    ]}"#,
    )
    .unwrap()
}

#[test]
fn projection_uses_ids_and_request_order_with_borrowed_strings_and_nullable_values() {
    let schema = schema();
    let projection = AvroProjection::new(&schema, &[517, 501, 500]).unwrap();
    let bytes = [2, b'a', 4, 2, 4, 0, 20, 0, 2, 2, b'b', 0, 40, 2];
    let mut records = projection.records(&bytes, 2, limits()).unwrap();
    let first = records.next_record().unwrap().unwrap();
    assert_eq!(
        first,
        vec![AvroScalar::Int(1), AvroScalar::Long(10), AvroScalar::String("a")]
    );
    let AvroScalar::String(path) = first[2] else {
        panic!("missing path")
    };
    assert_eq!(path.as_ptr(), bytes[1..].as_ptr());
    assert_eq!(
        records.next_record().unwrap().unwrap(),
        vec![AvroScalar::Null, AvroScalar::Long(20), AvroScalar::String("b")]
    );
    assert!(records.next_record().unwrap().is_none());
    assert!(records.next_record().unwrap().is_none());
}

#[test]
fn projection_rejects_ambiguous_ids_and_non_scalar_layouts_without_changing_avro_validation() {
    let schema = schema();
    for ids in [
        vec![],
        vec![500; 65],
        vec![500, 500],
        vec![-1],
        vec![999],
        vec![507],
    ] {
        assert!(AvroProjection::new(&schema, &ids).is_err());
    }
    for fields in [
        r#"[{"name":"a","type":"long"}]"#,
        r#"[{"name":"a","field-id":"0","type":"long"}]"#,
        r#"[{"name":"a","field-id":2147483648,"type":"long"}]"#,
        r#"[{"name":"a","field-id":-1,"type":"long"}]"#,
        r#"[{"name":"a","field-id":0,"type":"long"},{"name":"b","field-id":0,"type":"long"}]"#,
        r#"[{"name":"a","field-id":0,"type":["long","int"]}]"#,
        r#"[{"name":"a","field-id":0,"type":["null","long","int"]}]"#,
    ] {
        let bytes = format!(r#"{{"type":"record","name":"R","fields":{fields}}}"#);
        let schema = AvroSchema::parse(bytes.as_bytes()).unwrap();
        assert!(AvroProjection::new(&schema, &[0]).is_err());
    }
    let schema = AvroSchema::parse(br#""long""#).unwrap();
    assert!(AvroProjection::new(&schema, &[0]).is_err());
}

#[test]
fn skipped_fields_and_last_record_trailing_bytes_are_validated_and_poison_the_cursor() {
    let schema = schema();
    let projection = AvroProjection::new(&schema, &[501]).unwrap();
    for bytes in [
        vec![2, 255, 0, 20, 2],
        vec![2, b'a', 1, 0, 2, 0, 20, 2],
        vec![2, b'a', 0, 20, 4],
        vec![2, b'a', 0, 20, 2, 0],
        vec![2, b'a', 0, 20],
    ] {
        let mut records = projection.records(&bytes, 1, limits()).unwrap();
        assert!(records.next_record().is_err());
        assert!(matches!(records.next_record(), Err(AvroContainerError::Failed)));
    }
    assert!(projection.records(&[0], 0, limits()).is_err());
    assert!(projection
        .records(&[], 0, limits())
        .unwrap()
        .next_record()
        .unwrap()
        .is_none());
}

#[test]
fn projection_preserves_block_wide_work_depth_and_value_limits() {
    let schema = schema();
    let projection = AvroProjection::new(&schema, &[500]).unwrap();
    let bytes = [2, b'a', 0, 20, 2, 2, b'b', 0, 40, 2];
    let mut records = projection
        .records(
            &bytes,
            2,
            AvroDatumLimits {
                values: 6,
                ..limits()
            },
        )
        .unwrap();
    records.next_record().unwrap().unwrap();
    assert!(matches!(records.next_record(), Err(AvroContainerError::Bounds)));
    let mut records = projection
        .records(&bytes[..5], 1, AvroDatumLimits { depth: 1, ..limits() })
        .unwrap();
    assert!(matches!(records.next_record(), Err(AvroContainerError::Bounds)));
    assert!(projection.records(&[], 1_000_001, limits()).is_err());
    assert!(projection
        .records(
            &[],
            0,
            AvroDatumLimits {
                values: 0,
                ..limits()
            }
        )
        .is_err());
    let oversized = [4, b'a', b'b', 0, 20, 2];
    let mut records = projection
        .records(
            &oversized,
            1,
            AvroDatumLimits {
                value_bytes: 1,
                ..limits()
            },
        )
        .unwrap();
    assert!(matches!(records.next_record(), Err(AvroContainerError::Bounds)));
}
