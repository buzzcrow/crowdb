use crowdb_access_iceberg::file::{
    AvroBlock, AvroCodec, AvroContainerError, AvroDatumLimits, AvroSchema, FormatHint,
};

fn limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 64,
        values: 10_000,
        value_bytes: 1024,
    }
}

fn long(value: i64) -> Vec<u8> {
    let mut encoded = (value.unsigned_abs() << 1).wrapping_sub(u64::from(value < 0));
    let mut bytes = Vec::new();
    while encoded > 127 {
        bytes.push(u8::try_from(encoded & 127).unwrap() | 128);
        encoded >>= 7;
    }
    bytes.push(u8::try_from(encoded).unwrap());
    bytes
}

#[test]
fn binary_writer_schema_validates_every_primitive_and_exact_record_consumption() {
    for (schema, bytes) in [
        (r#""null""#, vec![]),
        (r#""boolean""#, vec![1]),
        (r#""int""#, long(i64::from(i32::MIN))),
        (r#""long""#, long(i64::MIN)),
        (r#""float""#, 1.25_f32.to_le_bytes().to_vec()),
        (r#""double""#, f64::NAN.to_le_bytes().to_vec()),
        (r#""bytes""#, vec![4, 0, 255]),
        (r#""string""#, vec![4, 0xc3, 0xa9]),
        (r#"{"type":"fixed","name":"Hash","size":2}"#, vec![0, 255]),
        (r#"{"type":"enum","name":"State","symbols":["A","B"]}"#, vec![2]),
    ] {
        let schema = AvroSchema::parse(schema.as_bytes()).unwrap();
        schema.validate_block(&bytes, 1, limits()).unwrap();
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(schema.validate_block(&trailing, 1, limits()).is_err());
        if !bytes.is_empty() {
            assert!(schema
                .validate_block(&bytes[..bytes.len() - 1], 1, limits())
                .is_err());
        }
    }
    let integer = AvroSchema::parse(br#""int""#).unwrap();
    assert!(integer
        .validate_block(&long(i64::from(i32::MAX) + 1), 1, limits())
        .is_err());
    let boolean = AvroSchema::parse(br#""boolean""#).unwrap();
    assert!(boolean.validate_block(&[2], 1, limits()).is_err());
    let string = AvroSchema::parse(br#""string""#).unwrap();
    assert!(string.validate_block(&[2, 255], 1, limits()).is_err());
    let integer = AvroSchema::parse(br#""long""#).unwrap();
    assert!(integer.validate_block(&[255; 10], 1, limits()).is_err());
}

#[test]
fn named_recursive_records_resolve_enclosing_and_explicit_namespaces() {
    let schema = AvroSchema::parse(
        br#"{
        "type":"record","name":"example.Node","namespace":"ignored",
        "fields":[{"name":"value","type":"long"},{"name":"next","type":["null","Node"]}]
    }"#,
    )
    .unwrap();
    schema.validate_block(&[2, 2, 4, 0], 1, limits()).unwrap();
    assert!(schema.validate_block(&[2, 4], 1, limits()).is_err());
    let schema = AvroSchema::parse(
        br#"{
        "type":"record","name":"Pair","namespace":"example",
        "fields":[
            {"name":"first","type":{"type":"fixed","name":"Item","size":2}},
            {"name":"second","type":"example.Item"},
            {"name":"third","type":"Item"}
        ]
    }"#,
    )
    .unwrap();
    schema.validate_block(&[0; 6], 1, limits()).unwrap();
}

#[test]
fn collection_blocks_validate_positive_negative_and_exact_sized_boundaries() {
    let array = AvroSchema::parse(br#"{"type":"array","items":"long"}"#).unwrap();
    array.validate_block(&[4, 2, 4, 1, 2, 6, 0], 1, limits()).unwrap();
    for bytes in [
        vec![1, 0, 2, 0],
        vec![1, 4, 2, 0],
        vec![1, 2, 2],
        vec![1, 1],
        vec![2, 2],
    ] {
        assert!(array.validate_block(&bytes, 1, limits()).is_err());
    }
    let map = AvroSchema::parse(br#"{"type":"map","values":["null","string"]}"#).unwrap();
    map.validate_block(&[1, 10, 2, b'k', 2, 2, b'v', 0], 1, limits())
        .unwrap();
    assert!(map.validate_block(&[2, 2, 255, 0, 0], 1, limits()).is_err());
    let enumeration = AvroSchema::parse(br#"{"type":"enum","name":"State","symbols":["A"]}"#).unwrap();
    assert!(enumeration.validate_block(&[2], 1, limits()).is_err());
    assert!(enumeration.validate_block(&[1], 1, limits()).is_err());
}

#[test]
fn invalid_schema_names_unions_references_and_shapes_fail_before_record_reads() {
    for schema in [
        r#"{"type":"record","name":"1Bad","fields":[]}"#,
        r#"{"type":"record","name":"long","namespace":"x","fields":[]}"#,
        r#"{"type":"record","name":"R","namespace":"a..b","fields":[]}"#,
        r#"{"type":"record","name":"R","fields":[{"name":"a","type":"Missing"}]}"#,
        r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"a","type":"int"}]}"#,
        r#"{"type":"enum","name":"E","symbols":["A","A"]}"#,
        r#"{"type":"enum","name":"E","symbols":["A"],"default":"B"}"#,
        r#"{"type":"fixed","name":"F","size":-1}"#,
        r#"["int","int"]"#,
        r#"["null",["long","int"]]"#,
        "[]",
        r#"[{"type":"array","items":"int"},{"type":"array","items":"long"}]"#,
        r#"[{"type":"record","name":"R","fields":[]},{"type":"record","name":"R","fields":[]}]"#,
        r#"{"type":"array"}"#,
        r#"{"type":"map","values":true}"#,
    ] {
        assert!(AvroSchema::parse(schema.as_bytes()).is_err(), "accepted {schema}");
    }
}

#[test]
fn schema_and_datum_resource_limits_are_independent_of_encoded_size() {
    assert!(matches!(
        AvroSchema::parse(&vec![b' '; 1024 * 1024 + 1]),
        Err(AvroContainerError::Bounds)
    ));
    let fields: Vec<_> = (0..4096)
        .map(|index| serde_json::json!({"name":format!("field{index}"),"type":"null"}))
        .collect();
    let oversized =
        serde_json::to_vec(&serde_json::json!({"type":"record","name":"Record","fields":fields})).unwrap();
    assert!(matches!(
        AvroSchema::parse(&oversized),
        Err(AvroContainerError::Bounds)
    ));
    let array = AvroSchema::parse(br#"{"type":"array","items":"null"}"#).unwrap();
    let mut many = long(1_000_000);
    many.push(0);
    assert!(matches!(
        array.validate_block(&many, 1, limits()),
        Err(AvroContainerError::Bounds)
    ));
    let recursive =
        AvroSchema::parse(br#"{"type":"record","name":"R","fields":[{"name":"next","type":["null","R"]}]}"#)
            .unwrap();
    let mut deep = vec![2; 64];
    deep.push(0);
    assert!(matches!(
        recursive.validate_block(&deep, 1, limits()),
        Err(AvroContainerError::Bounds)
    ));
    let bytes = AvroSchema::parse(br#""bytes""#).unwrap();
    assert!(matches!(
        bytes.validate_block(
            &[4, 1, 2],
            1,
            AvroDatumLimits {
                value_bytes: 1,
                ..limits()
            }
        ),
        Err(AvroContainerError::Bounds)
    ));
    assert!(bytes
        .validate_block(&[], 0, AvroDatumLimits { depth: 0, ..limits() })
        .is_err());
    assert!(bytes
        .validate_block(
            &[],
            0,
            AvroDatumLimits {
                values: 0,
                ..limits()
            }
        )
        .is_err());
    assert!(bytes.validate_block(&[], 1_000_001, limits()).is_err());
}

#[test]
fn decoded_container_blocks_validate_record_counts_and_ignore_nonbinary_annotations() {
    let schema =
        AvroSchema::parse(br#"{"type":"long","logicalType":"timestamp-micros","custom":"annotation"}"#)
            .unwrap();
    let block = |records| AvroBlock {
        records,
        payload: FormatHint { offset: 0, length: 2 },
        encoded: vec![2, 4],
    };
    assert_eq!(
        block(2)
            .decode_validated(AvroCodec::Null, 100, &schema, limits())
            .unwrap(),
        vec![2, 4]
    );
    assert!(block(1)
        .decode_validated(AvroCodec::Null, 100, &schema, limits())
        .is_err());
    assert!(block(3)
        .decode_validated(AvroCodec::Null, 100, &schema, limits())
        .is_err());
}
