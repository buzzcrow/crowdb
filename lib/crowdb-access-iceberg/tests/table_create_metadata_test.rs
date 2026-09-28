#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    commit::{evaluate_table_creation, CreateTableRequest, InitialTableMetadata},
    key::OperationId,
    table::{TableHead, TableMetadataError},
};
use serde_json::{json, Value};

fn request() -> Value {
    json!({"name":"events","schema":{"type":"struct","schema-id":41,
        "identifier-field-ids":[90],"fields":[
        {"id":90,"name":"id","type":"long","required":true},
        {"id":70,"name":"nested","required":false,"type":{"type":"struct","fields":[
            {"id":60,"name":"text","type":"string","required":false}]}},
        {"id":50,"name":"items","required":false,"type":{"type":"list","element-id":40,
            "element-required":false,"element":{"type":"struct","fields":[
                {"id":30,"name":"value","type":"long","required":false}]}}},
        {"id":20,"name":"mapping","required":false,"type":{"type":"map","key-id":10,
            "key":"string","value-id":9,"value-required":false,"value":"long"}}]},
        "partition-spec":{"spec-id":91,"fields":[{"source-id":90,"field-id":1017,
            "name":"id_bucket","transform":"bucket[16]"}]},
        "write-order":{"order-id":81,"fields":[{"source-id":60,"transform":"identity",
            "direction":"desc","null-order":"nulls-last"}]}})
}

fn target() -> TableHead {
    let mut head = fixture::head(&[], 2, Some(uuid::Uuid::new_v4()));
    head.pending_operation = Some(OperationId::random());
    head
}

fn evaluate(value: &Value) -> Result<InitialTableMetadata, TableMetadataError> {
    let request = CreateTableRequest::decode(&serde_json::to_vec(value).unwrap(), fixture::limits())?;
    evaluate_table_creation(&request, target(), 1000, fixture::limits())
}

#[test]
fn creation_assigns_sibling_first_ids_and_remaps_all_layouts() {
    for version in 1..=3 {
        let mut input = request();
        input["properties"] = json!({"format-version":version.to_string()});
        let result = evaluate(&input).unwrap();
        let root = result.document.fields();
        let schema = &root["schemas"][0];
        let fields = &schema["fields"];
        assert_eq!(schema["schema-id"], 0);
        assert_eq!(schema["identifier-field-ids"], json!([1]));
        for index in 0..4 {
            assert_eq!(fields[index]["id"], index + 1);
        }
        assert_eq!(fields[1]["type"]["fields"][0]["id"], 5);
        assert_eq!(fields[2]["type"]["element-id"], 6);
        assert_eq!(fields[2]["type"]["element"]["fields"][0]["id"], 7);
        assert_eq!(fields[3]["type"]["key-id"], 8);
        assert_eq!(fields[3]["type"]["value-id"], 9);
        assert_eq!(root["last-column-id"], 9);
        assert_eq!(root["partition-specs"][0]["fields"][0]["source-id"], 1);
        assert_eq!(root["partition-specs"][0]["fields"][0]["field-id"], 1000);
        assert_eq!(root["last-partition-id"], 1000);
        assert_eq!(root["default-spec-id"], 0);
        assert_eq!(root["sort-orders"][0]["fields"][0]["source-id"], 5);
        assert_eq!(root["default-sort-order-id"], 1);
        assert_eq!(result.head.format_version, version);
        assert_eq!(root.contains_key("schema"), version == 1);
        assert_eq!(root.contains_key("last-sequence-number"), version > 1);
        assert_eq!(root.contains_key("next-row-id"), version == 3);
        assert!(!result.stage_create);
    }
}

#[test]
fn creation_defaults_and_reserved_properties_follow_new_table_profile() {
    let input = json!({"name":"events","schema":{"type":"struct","fields":[]},
        "stage-create":true,"properties":{"uuid":"ignored","owner":"user"}});
    let result = evaluate(&input).unwrap();
    let root = result.document.fields();
    assert_eq!(root["format-version"], 2);
    assert_eq!(root["last-column-id"], 0);
    assert_eq!(root["last-partition-id"], 999);
    assert_eq!(root["default-sort-order-id"], 0);
    assert_eq!(
        root["properties"],
        json!({"owner":"user","write.parquet.compression-codec":"zstd"})
    );
    assert!(result.stage_create);
}

#[test]
fn sdk_null_optional_fields_use_absent_defaults() {
    let input = json!({"name":"events","schema":{"type":"struct","fields":[]},
        "location":null,"partition-spec":null,"write-order":null,"properties":null});
    let result = evaluate(&input).unwrap();
    assert_eq!(result.head.format_version, 2);
    assert_eq!(result.document.fields()["default-sort-order-id"], 0);
    assert_eq!(
        result.document.fields()["partition-specs"][0]["fields"],
        json!([])
    );
}

#[test]
fn creation_preserves_and_remaps_nested_defaults() {
    let mut input = request();
    input["properties"] = json!({"format-version":"3"});
    input["schema"]["fields"][0]["initial-default"] = json!(i64::MAX);
    input["schema"]["fields"][2]["initial-default"] = json!([{"30":42}]);
    input["schema"]["fields"][3]["write-default"] = json!({"keys":["one"],"values":[99]});
    let result = evaluate(&input).unwrap();
    let fields = &result.document.fields()["schemas"][0]["fields"];
    assert_eq!(fields[0]["initial-default"], i64::MAX);
    assert_eq!(fields[2]["initial-default"], json!([{"7":42}]));
    assert_eq!(fields[3]["write-default"], json!({"keys":["one"],"values":[99]}));
}

#[test]
fn invalid_input_is_not_repaired_by_fresh_id_assignment() {
    for pointer in ["/schema/fields/1/id", "/schema/fields/2/type/element-id"] {
        let mut input = request();
        *input.pointer_mut(pointer).unwrap() = json!(90);
        assert!(evaluate(&input).is_err());
    }
    for (pointer, value) in [
        ("/schema/identifier-field-ids", json!([60])),
        ("/partition-spec/fields/0/source-id", json!(999)),
        ("/partition-spec/fields/0/transform", json!("hour")),
        ("/partition-spec/spec-id", json!(-1)),
        ("/partition-spec/fields/0/field-id", json!("1000")),
        ("/write-order/order-id", json!(0)),
        ("/write-order/fields/0/direction", json!("backwards")),
        ("/write-order/fields/0/source-id", json!(30)),
    ] {
        let mut input = request();
        *input.pointer_mut(pointer).unwrap() = value;
        assert!(evaluate(&input).is_err(), "{pointer}");
    }
}

#[test]
fn creation_validates_properties_and_sdk_collection_column_aliases() {
    for properties in [
        json!({"format-version":"4"}),
        json!({"format-version":2}),
        json!({"commit.retry.num-retries":"-1"}),
        json!({"commit.retry.total-timeout-ms":"2147483648"}),
        json!({"write.metadata.metrics.column.missing":"counts"}),
        json!({"encryption.key-id":"unsupported-in-v2"}),
    ] {
        let mut input = request();
        input["properties"] = properties;
        assert!(evaluate(&input).is_err());
    }
    let mut input = request();
    input["properties"] = json!({"write.metadata.metrics.column.items.value":"counts",
        "write.metadata.metrics.column.items.element.value":"full",
        "commit.retry.num-retries":"0","write.parquet.compression-codec":"snappy"});
    let result = evaluate(&input).unwrap();
    assert_eq!(
        result.document.fields()["properties"]["write.parquet.compression-codec"],
        "snappy"
    );
}

#[test]
fn creation_is_deterministic_and_bound_to_initial_target_and_location() {
    let mut input = request();
    input["location"] = json!(fixture::table().to_string());
    let request =
        CreateTableRequest::decode(&serde_json::to_vec(&input).unwrap(), fixture::limits()).unwrap();
    let head = target();
    let first = evaluate_table_creation(&request, head.clone(), 1000, fixture::limits()).unwrap();
    let replay = evaluate_table_creation(&request, head.clone(), 1000, fixture::limits()).unwrap();
    assert_eq!(first.document.canonical(), replay.document.canonical());
    assert_eq!(first.head, replay.head);
    let mut wrong = head.clone();
    wrong.generation = 2;
    assert!(evaluate_table_creation(&request, wrong, 1000, fixture::limits()).is_err());
    let mut wrong = head;
    wrong.pending_operation = None;
    assert!(evaluate_table_creation(&request, wrong, 1000, fixture::limits()).is_err());
    input["location"] = json!("s3://external/table");
    assert!(evaluate(&input).is_err());
}

#[test]
fn creation_bounds_decode_revalidation_and_output_independently() {
    assert!(CreateTableRequest::decode(
        br#"{"name":"events","name":"again","schema":{}}"#,
        fixture::limits()
    )
    .is_err());
    for (name, value) in [
        ("stage-create", json!("true")),
        ("properties", json!([])),
        ("name", json!("")),
    ] {
        let mut input = request();
        input[name] = value;
        assert!(CreateTableRequest::decode(&serde_json::to_vec(&input).unwrap(), fixture::limits()).is_err());
    }
    let bytes = serde_json::to_vec(&request()).unwrap();
    let request = CreateTableRequest::decode(&bytes, fixture::limits()).unwrap();
    let mut small = fixture::limits();
    small.values = 10;
    assert!(evaluate_table_creation(&request, target(), 1000, small).is_err());
    small = fixture::limits();
    small.bytes = bytes.len();
    small.string_bytes = small.bytes;
    assert!(evaluate_table_creation(&request, target(), 1000, small).is_err());
}
