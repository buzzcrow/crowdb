#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    commit::{
        evaluate_metadata_updates, CommitRequest, CommitRequestLimits, EvaluationLimits, RequirementLimits,
    },
    key::FileId,
    table::TableMetadataDocument,
};
use serde_json::{json, Value};

fn evaluate(metadata: &Value, updates: Value) -> Option<Value> {
    let bytes = serde_json::to_vec(metadata).unwrap();
    let version = u8::try_from(metadata["format-version"].as_u64().unwrap()).unwrap();
    let mut head = fixture::head(
        &bytes,
        version,
        Some(uuid::Uuid::parse_str(metadata["table-uuid"].as_str().unwrap()).unwrap()),
    );
    let document = TableMetadataDocument::parse(bytes, &head, fixture::limits()).unwrap();
    let mut request = json!({"requirements":[]});
    request["updates"] = updates;
    let bytes = serde_json::to_vec(&request).unwrap();
    let request = CommitRequest::decode(
        &bytes,
        CommitRequestLimits {
            json: fixture::limits(),
            requirements: 100,
            updates: 100,
        },
    )
    .unwrap();
    head.generation += 1;
    head.metadata_file = FileId::random();
    head.metadata_location = fixture::table().file("metadata/next.json").unwrap();
    let result = evaluate_metadata_updates(
        &document,
        &request,
        head,
        1100,
        EvaluationLimits {
            metadata: fixture::limits(),
            requirements: RequirementLimits {
                count: 100,
                text_bytes: 4096,
            },
            updates: 100,
            work_bytes: 8 * 1024 * 1024,
        },
    )
    .ok()?;
    Some(serde_json::from_slice(result.document.canonical()).unwrap())
}

fn spec(id: i32, source: i32, transform: &str, name: &str) -> Value {
    json!({"fields":[{"field-id":id,"source-id":source,"transform":transform,"name":name}]})
}

#[test]
fn layout_additions_assign_ids_and_bind_the_schema_at_the_update() {
    let metadata = fixture::metadata(2);
    let partition = spec(1000, 1, "bucket[16]", "bucket");
    let sort = json!({"fields":[{"source-id":1,"transform":"identity","direction":"desc","null-order":"nulls-last"}]});
    let result = evaluate(&metadata, json!([
        {"action":"add-spec","spec":partition}, {"action":"set-default-spec","spec-id":-1},
        {"action":"add-sort-order","sort-order":sort}, {"action":"set-default-sort-order","sort-order-id":-1}
    ])).unwrap();
    assert_eq!(result["default-spec-id"], 1);
    assert_eq!(result["last-partition-id"], 1000);
    assert_eq!(result["default-sort-order-id"], 1);
    assert!(evaluate(&metadata, json!([{"action":"set-default-spec","spec-id":-1}])).is_none());
    assert!(evaluate(
        &metadata,
        json!([{"action":"add-spec","spec":spec(1000,2,"identity","missing")}])
    )
    .is_none());
    assert!(evaluate(
        &metadata,
        json!([{"action":"set-default-sort-order","sort-order-id":999}])
    )
    .is_none());
}

#[test]
fn partition_ids_retain_meaning_and_matching_fields_reuse_ids() {
    let mut metadata = fixture::metadata(2);
    let mut partition = spec(1000, 1, "bucket[16]", "bucket");
    partition["spec-id"] = json!(1);
    metadata["partition-specs"]
        .as_array_mut()
        .unwrap()
        .push(partition.clone());
    metadata["last-partition-id"] = json!(1000);
    assert!(evaluate(
        &metadata,
        json!([{"action":"add-spec","spec":spec(1000,1,"bucket[32]","bucket")}])
    )
    .is_none());
    let result = evaluate(
        &metadata,
        json!([{"action":"add-spec","spec":spec(1000,1,"bucket[16]","renamed")},
        {"action":"set-default-spec","spec-id":-1}]),
    )
    .unwrap();
    assert_eq!(result["default-spec-id"], 2);
    assert_eq!(result["last-partition-id"], 1000);
    let mut wrong = partition;
    wrong["fields"][0]["field-id"] = json!(1001);
    wrong["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"source-id":1,"field-id":1002,"name":"extra","transform":"identity"}));
    assert!(evaluate(&metadata, json!([{"action":"add-spec","spec":wrong}])).is_none());
}

#[test]
fn v1_requires_sequential_partition_ids_and_refreshes_legacy_fields() {
    let metadata = fixture::metadata(1);
    assert!(evaluate(
        &metadata,
        json!([{"action":"add-spec","spec":spec(1001,1,"identity","id")}])
    )
    .is_none());
    let result = evaluate(
        &metadata,
        json!([{"action":"add-spec","spec":spec(1000,1,"identity","id")},
        {"action":"set-default-spec","spec-id":-1}]),
    )
    .unwrap();
    assert_eq!(result["partition-spec"], result["partition-specs"][1]["fields"]);
    assert!(evaluate(
        &metadata,
        json!([{"action":"remove-partition-specs","spec-ids":[0]}])
    )
    .is_none());
}

#[test]
fn date_promotion_must_preserve_partition_values() {
    for (transform, valid) in [
        ("year", true),
        ("month", true),
        ("day", true),
        ("identity", false),
        ("bucket[16]", false),
    ] {
        let mut metadata = fixture::metadata(3);
        metadata["schemas"][0]["fields"][0]["type"] = json!("date");
        let mut partition = spec(1000, 1, transform, "partition");
        partition["spec-id"] = json!(0);
        metadata["partition-specs"] = json!([partition]);
        metadata["last-partition-id"] = json!(1000);
        let mut schema = metadata["schemas"][0].clone();
        schema["fields"][0]["type"] = json!("timestamp");
        assert_eq!(
            evaluate(
                &metadata,
                json!([{"action":"add-schema","schema":schema},
            {"action":"set-current-schema","schema-id":-1}])
            )
            .is_some(),
            valid,
            "{transform}"
        );
    }
}
