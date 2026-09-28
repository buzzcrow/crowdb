#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    commit::{
        evaluate_metadata_updates, CommitRequest, CommitRequestLimits, EvaluatedMetadata, EvaluationError,
        EvaluationLimits, RequirementLimits,
    },
    key::FileId,
    table::{TableMetadataDocument, TableMetadataError},
};
use serde_json::{json, Value};

fn evaluate(metadata: &Value, updates: Value) -> Result<EvaluatedMetadata, EvaluationError> {
    let mut request = json!({"requirements":[]});
    request["updates"] = updates;
    run(metadata, &request, 8 * 1024 * 1024)
}

fn run(metadata: &Value, request: &Value, work: usize) -> Result<EvaluatedMetadata, EvaluationError> {
    let bytes = serde_json::to_vec(metadata).unwrap();
    let mut head = fixture::head(
        &bytes,
        u8::try_from(metadata["format-version"].as_u64().unwrap()).unwrap(),
        metadata["table-uuid"]
            .as_str()
            .map(|text| uuid::Uuid::parse_str(text).unwrap()),
    );
    let prior = TableMetadataDocument::parse(bytes, &head, fixture::limits()).unwrap();
    let request = CommitRequest::decode(
        &serde_json::to_vec(request).unwrap(),
        CommitRequestLimits {
            json: fixture::limits(),
            requirements: 100,
            updates: 100,
        },
    )
    .unwrap();
    head.generation += 1;
    head.metadata_file = FileId::random();
    head.metadata_location = fixture::table().file("metadata/two.metadata.json").unwrap();
    evaluate_metadata_updates(
        &prior,
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
            work_bytes: work,
        },
    )
}

fn add(schema: &Value) -> Value {
    json!({"action":"add-schema","schema":schema})
}

#[test]
fn exhausted_schema_ids_allow_reuse_but_not_new_definitions() {
    let mut metadata = fixture::metadata(3);
    metadata["schemas"][0]["schema-id"] = json!(i32::MAX);
    metadata["current-schema-id"] = json!(i32::MAX);
    let schema = metadata["schemas"][0].clone();
    assert!(evaluate(&metadata, json!([add(&schema)])).is_ok());
    let mut changed = schema;
    changed["fields"][0]["name"] = json!("renamed");
    assert!(evaluate(&metadata, json!([add(&changed)])).is_err());
}

#[test]
fn exhausted_layout_ids_do_not_prevent_reusing_a_later_definition() {
    let mut metadata = fixture::metadata(3);
    metadata["partition-specs"] = json!([
        {"spec-id":i32::MAX,"fields":[{"field-id":1000,"source-id":1,
        "name":"bucket","transform":"bucket[16]"}]},
        {"spec-id":0,"fields":[]}
    ]);
    metadata["last-partition-id"] = json!(1000);
    let reused = json!({"action":"add-spec","spec":{"fields":[]}});
    assert!(evaluate(&metadata, json!([reused])).is_ok());
    let novel = json!({"action":"add-spec","spec":{"fields":[
        {"field-id":1001,"source-id":1,"name":"identity","transform":"identity"}
    ]}});
    assert!(evaluate(&metadata, json!([novel])).is_err());
    metadata["sort-orders"] = json!([
        {"order-id":i32::MAX,"fields":[{"source-id":1,"transform":"identity",
        "direction":"asc","null-order":"nulls-first"}]},
        {"order-id":0,"fields":[]}
    ]);
    assert!(evaluate(
        &metadata,
        json!([{"action":"add-sort-order","sort-order":{"fields":[]}}])
    )
    .is_ok());
}
fn select(id: i32) -> Value {
    json!({"action":"set-current-schema","schema-id":id})
}

#[test]
fn ordered_schema_ids_and_last_added_follow_actual_updates() {
    let metadata = fixture::metadata(3);
    let mut schema = metadata["schemas"][0].clone();
    schema.as_object_mut().unwrap().remove("schema-id");
    schema["fields"][0]["name"] = json!("renamed");
    let result = evaluate(&metadata, json!([add(&schema), select(-1)])).unwrap();
    assert_eq!(result.document.fields()["current-schema-id"], 1);
    assert_eq!(result.document.fields()["schemas"][1]["schema-id"], 1);
    assert_eq!(result.document.fields()["schemas"][0], metadata["schemas"][0]);
    assert_eq!(metadata["current-schema-id"], 0);
    assert!(evaluate(&metadata, json!([select(-1), add(&schema)])).is_err());
    assert!(evaluate(&metadata, json!([add(&metadata["schemas"][0]), select(-1)])).is_err());
    assert!(evaluate(
        &metadata,
        json!([add(&schema), add(&metadata["schemas"][0]), select(-1)])
    )
    .is_err());
    assert!(evaluate(&metadata, json!([add(&schema), add(&schema), select(-1)])).is_ok());
}

#[test]
fn schema_evolution_uses_selected_schema_not_array_order() {
    let mut metadata = fixture::metadata(3);
    let mut schema = metadata["schemas"][0].clone();
    schema["schema-id"] = json!(1);
    schema["fields"][0]["type"] = json!("int");
    metadata["schemas"].as_array_mut().unwrap().push(schema.clone());
    let mut promoted = schema.clone();
    promoted["fields"][0]["name"] = json!("promoted");
    promoted["fields"][0]["type"] = json!("long");
    assert!(evaluate(&metadata, json!([select(1), add(&promoted), select(-1)])).is_ok());
    schema["fields"][0]["name"] = json!("narrowed");
    assert!(evaluate(&metadata, json!([add(&schema)])).is_err());
}

#[test]
fn defaults_high_water_and_parentage_are_validated_before_returning_candidate() {
    let mut metadata = fixture::metadata(3);
    metadata["last-column-id"] = json!(9);
    let mut schema = metadata["schemas"][0].clone();
    schema["fields"].as_array_mut().unwrap().push(json!({
        "id":10,"name":"added","required":true,"type":"long","initial-default":7,"write-default":7
    }));
    let result = evaluate(&metadata, json!([add(&schema), select(-1)])).unwrap();
    assert_eq!(result.document.fields()["last-column-id"], 10);
    schema["fields"][1]["id"] = json!(9);
    assert!(evaluate(&metadata, json!([add(&schema)])).is_err());
    schema["fields"][1]["id"] = json!(10);
    schema["fields"][1]
        .as_object_mut()
        .unwrap()
        .remove("initial-default");
    assert!(evaluate(&metadata, json!([add(&schema)])).is_err());
    let nested = json!({"type":"struct","fields":[{"id":10,"name":"parent","required":false,
        "type":{"type":"struct","fields":[metadata["schemas"][0]["fields"][0].clone()]}}]});
    assert!(evaluate(&metadata, json!([add(&nested)])).is_err());
}

#[test]
fn initial_defaults_are_immutable_but_write_defaults_may_change() {
    let mut metadata = fixture::metadata(3);
    metadata["schemas"][0]["fields"][0]["initial-default"] = json!(7);
    metadata["schemas"][0]["fields"][0]["write-default"] = json!(7);
    let mut schema = metadata["schemas"][0].clone();
    schema["fields"][0]["write-default"] = json!(9);
    assert!(evaluate(&metadata, json!([add(&schema), select(-1)])).is_ok());
    schema["fields"][0]["initial-default"] = json!(9);
    assert!(evaluate(&metadata, json!([add(&schema), select(-1)])).is_err());
}

#[test]
fn scalar_updates_are_ordered_and_invalid_updates_never_return_partial_success() {
    let metadata = fixture::metadata(2);
    let result = evaluate(
        &metadata,
        json!([
            {"action":"set-properties","updates":{"owner":"one","removed":"yes"}},
            {"action":"remove-properties","removals":["removed"]},
            {"action":"set-properties","updates":{"owner":"two"}}
        ]),
    )
    .unwrap();
    assert_eq!(result.document.fields()["properties"], json!({"owner":"two"}));
    assert_eq!(result.document.fields()["metadata-log"][0]["timestamp-ms"], 1000);
    assert!(matches!(
        evaluate(
            &metadata,
            json!([
                {"action":"set-properties","updates":{"owner":"never"}},
                {"action":"add-snapshot","snapshot":{}}
            ])
        ),
        Err(EvaluationError::Metadata(_))
    ));
    assert!(evaluate(
        &metadata,
        json!([{"action":"set-location","location":"s3://other/table"}])
    )
    .is_err());
    assert!(evaluate(
        &metadata,
        json!([{"action":"assign-uuid","uuid":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}])
    )
    .is_err());
}

#[test]
fn optional_new_containers_do_not_materialize_required_children_in_existing_rows() {
    let metadata = fixture::metadata(3);
    let mut schema = metadata["schemas"][0].clone();
    schema["fields"].as_array_mut().unwrap().push(json!({
        "id":2,"name":"parent","required":false,
        "type":{"type":"struct","fields":[{"id":3,"name":"child","required":true,"type":"long"}]}
    }));
    assert!(evaluate(&metadata, json!([add(&schema), select(-1)])).is_ok());
    schema["fields"][1]["initial-default"] = json!({});
    assert!(evaluate(&metadata, json!([add(&schema), select(-1)])).is_err());
}

#[test]
fn initial_defaults_require_upgrade_before_the_schema_update() {
    let metadata = fixture::metadata(2);
    let mut schema = metadata["schemas"][0].clone();
    schema["fields"].as_array_mut().unwrap().push(json!({
        "id":2,"name":"defaulted","type":"long","required":true,"initial-default":7,"write-default":7
    }));
    let upgrade = json!({"action":"upgrade-format-version","format-version":3});
    assert!(evaluate(&metadata, json!([add(&schema), upgrade])).is_err());
    assert!(evaluate(&metadata, json!([upgrade, add(&schema), select(-1)])).is_ok());
}

#[test]
fn requirements_work_limits_and_expanded_upgrades_fail_closed() {
    let metadata = fixture::metadata(1);
    assert!(run(
        &metadata,
        &json!({"requirements":[{"type":"assert-current-schema-id","current-schema-id":99}],
        "updates":[]}),
        8 * 1024 * 1024
    )
    .is_err());
    assert!(matches!(
        run(&metadata, &json!({"requirements":[],"updates":[]}), 1),
        Err(EvaluationError::Metadata(TableMetadataError::Bounds))
    ));
    let result = evaluate(
        &metadata,
        json!([
            {"action":"upgrade-format-version","format-version":2},
            {"action":"upgrade-format-version","format-version":3}
        ]),
    )
    .unwrap();
    assert_eq!(result.upgrades, vec![2, 3]);
    assert_eq!(result.document.fields()["next-row-id"], 0);
    assert!(!result.document.fields().contains_key("schema"));
    let direct = evaluate(
        &metadata,
        json!([{"action":"upgrade-format-version","format-version":3}]),
    )
    .unwrap();
    assert_eq!(direct.upgrades, vec![2, 3]);
    assert_eq!(direct.document.fields(), result.document.fields());
    assert!(evaluate(
        &fixture::metadata(3),
        json!([{"action":"upgrade-format-version","format-version":2}])
    )
    .is_err());
}

#[test]
fn selected_or_removed_schema_ids_are_checked_in_order() {
    let metadata = fixture::metadata(2);
    let mut schema = metadata["schemas"][0].clone();
    schema["fields"][0]["name"] = json!("next");
    assert!(evaluate(&metadata, json!([{"action":"remove-schemas","schema-ids":[0]}])).is_err());
    let result = evaluate(
        &metadata,
        json!([add(&schema), select(-1),
        {"action":"remove-schemas","schema-ids":[0,0,99]}]),
    )
    .unwrap();
    assert_eq!(result.document.fields()["schemas"].as_array().unwrap().len(), 1);
    assert!(evaluate(
        &metadata,
        json!([add(&schema), select(-1),
        {"action":"remove-schemas","schema-ids":[0]}, select(0)])
    )
    .is_err());
}
