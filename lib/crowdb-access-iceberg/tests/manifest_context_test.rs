use crowdb_access_iceberg::manifest::{ManifestContext, ManifestVersion, PartitionTransform, PrimitiveType};
use serde_json::{json, Value};

fn parse(
    fields: impl Into<Value>,
    partitions: impl Into<Value>,
    version: ManifestVersion,
) -> Result<ManifestContext, crowdb_access_iceberg::manifest::ManifestContextError> {
    ManifestContext::parse(
        version,
        7,
        2,
        &serde_json::to_vec(&json!({"type":"struct","schema-id":7,"fields":fields.into()})).unwrap(),
        &serde_json::to_vec(&partitions.into()).unwrap(),
    )
}

#[test]
fn nested_schema_indexes_all_ids_and_derives_partition_result_types() {
    let context = parse(json!([
        {"id":1,"name":"parent","required":false,"type":{"type":"struct","fields":[{"id":2,"name":"ts","required":true,"type":"timestamp_ns"}]}},
        {"id":3,"name":"items","required":true,"type":{"type":"list","element-id":4,"element-required":false,"element":"decimal(10, 2)"}},
        {"id":5,"name":"map","required":true,"type":{"type":"map","key-id":6,"key":"string","value-id":7,"value-required":false,"value":"long"}}
    ]), json!([
        {"field-id":1000,"name":"day","source-id":2,"transform":"day"},
        {"field-id":1001,"name":"future","source-id":2,"transform":"future[9]"}
    ]),ManifestVersion::V3).unwrap();
    assert!(!context.field(2).unwrap().required_path);
    assert!(!context.field(2).unwrap().repeated);
    assert!(context.field(4).unwrap().repeated);
    assert!(context.field(6).unwrap().repeated);
    assert_eq!(context.partitions()[0].result, Some(PrimitiveType::Int));
    assert!(matches!(
        context.partitions()[1].transform,
        PartitionTransform::Unknown(_)
    ));
    assert_eq!(context.partitions()[1].result, None);
}

#[test]
fn schema_rejects_global_id_collisions_reserved_ids_invalid_types_and_missing_nullability() {
    for field in [
        json!({"id":1,"name":"a","required":true,"type":{"type":"list","element-id":1,"element-required":false,"element":"int"}}),
        json!({"id":2_147_483_448_i64,"name":"a","required":true,"type":"int"}),
        json!({"id":1,"name":"a","required":true,"type":"decimal(39,2)"}),
        json!({"id":1,"name":"a","required":true,"type":"decimal(3,4)"}),
        json!({"id":1,"name":"a","required":true,"type":"fixed[0]"}),
        json!({"id":1,"name":"a","required":true,"type":"unknown"}),
        json!({"id":1,"name":"a","type":"int"}),
    ] {
        assert!(parse(json!([field]), json!([]), ManifestVersion::V3).is_err());
    }
    assert!(parse(
        json!([{"id":1,"name":"a","required":false,"type":"timestamp_ns"}]),
        json!([]),
        ManifestVersion::V2
    )
    .is_err());
}

#[test]
fn partition_sources_transforms_and_version_one_ids_are_checked() {
    let fields = json!([{"id":1,"name":"a","required":false,"type":"int"}]);
    for transform in ["bucket[0]", "truncate[-1]", "hour", "year"] {
        assert!(parse(
            fields.clone(),
            json!([{"field-id":1000,"name":"p","source-id":1,"transform":transform}]),
            ManifestVersion::V2
        )
        .is_err());
    }
    let spec = json!([{"name":"p","source-id":1,"transform":"bucket[8]"}]);
    assert_eq!(
        parse(fields.clone(), spec.clone(), ManifestVersion::V1)
            .unwrap()
            .partitions()[0]
            .id,
        1000
    );
    assert!(parse(fields.clone(), spec, ManifestVersion::V2).is_err());
    assert!(parse(
        fields,
        json!([{"field-id":1000,"name":"p","source-id":2,"transform":"identity"}]),
        ManifestVersion::V3
    )
    .is_err());
}

#[test]
fn schema_and_partition_resources_are_bounded_before_retaining_context() {
    let fields: Vec<_> = (1..=4097)
        .map(|id| json!({"id":id,"name":format!("c{id}"),"required":false,"type":"int"}))
        .collect();
    assert!(parse(json!(fields), json!([]), ManifestVersion::V3).is_err());
    let mut nested = json!("int");
    for id in 1..=34 {
        nested = json!({"type":"struct","fields":[{"id":id,"name":"n","required":false,"type":nested}]});
    }
    assert!(parse(
        json!([{"id":100,"name":"root","required":false,"type":nested}]),
        json!([]),
        ManifestVersion::V3
    )
    .is_err());
    let partitions: Vec<_> = (1000..1257)
        .map(|id| json!({"field-id":id,"name":format!("p{id}"),"source-id":1,"transform":"identity"}))
        .collect();
    assert!(parse(
        json!([{"id":1,"name":"a","required":false,"type":"int"}]),
        json!(partitions),
        ManifestVersion::V3
    )
    .is_err());
}

#[test]
fn history_retains_dropped_columns_and_metadata_binding_uses_the_writer_schema() {
    use crowdb_access_iceberg::manifest::{ManifestContent, ManifestMetadata};
    let version = ManifestVersion::V3;
    let old = parse(
        json!([{"id":1,"name":"old","required":false,"type":"int"}]),
        json!([]),
        version,
    )
    .unwrap();
    let promoted = parse(
        json!([{"id":1,"name":"renamed","required":false,"type":"long"}]),
        json!([]),
        version,
    )
    .unwrap();
    let current = parse(json!([]), json!([]), version)
        .unwrap()
        .with_schema_history(&[old, promoted])
        .unwrap();
    assert!(current.field(1).is_none());
    assert_eq!(
        current.retained_field(1).unwrap().primitive,
        Some(PrimitiveType::Long)
    );
    let metadata = ManifestMetadata {
        version,
        content: ManifestContent::Data,
        schema_id: Some(7),
        partition_spec_id: Some(2),
        schema_json: br#"{"type":"struct","schema-id":7,"fields":[]}"#,
        partition_spec_json: b"[]",
    };
    assert!(current.validate_metadata(metadata, 2).is_ok());
    assert!(current.validate_metadata(metadata, 3).is_err());
    let wrong = ManifestMetadata {
        schema_id: Some(8),
        ..metadata
    };
    assert!(current.validate_metadata(wrong, 2).is_err());
    let wrong=ManifestMetadata{schema_json:br#"{"type":"struct","schema-id":7,"fields":[{"id":1,"name":"old","required":false,"type":"long"}]}"#,..metadata};
    assert!(current.validate_metadata(wrong, 2).is_err());
}
#[test]
fn void_partition_sources_may_be_expired_but_other_transforms_need_a_source_type() {
    use crowdb_access_iceberg::manifest::{ManifestContext, ManifestVersion};
    for transform in ["void", "identity", "bucket[8]"] {
        let spec = serde_json::to_vec(
            &serde_json::json!([{"field-id":1000,"source-id":1,"name":"old","transform":transform}]),
        )
        .unwrap();
        let context = ManifestContext::parse(
            ManifestVersion::V1,
            0,
            0,
            br#"{"type":"struct","schema-id":0,"fields":[]}"#,
            &spec,
        );
        assert_eq!(context.is_ok(), transform == "void");
        if let Ok(context) = context {
            assert!(context.partitions()[0].result.is_none());
        }
    }
}
