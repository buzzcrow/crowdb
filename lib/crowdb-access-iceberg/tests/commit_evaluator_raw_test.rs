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
use serde_json::json;

#[test]
fn unknown_optional_numbers_survive_root_collection_and_payload_rewrites() {
    let mut metadata = fixture::metadata(3);
    metadata["future"] = json!("RAW_LARGE");
    metadata["schemas"][0]["future"] = json!("RAW_LARGE");
    let huge = "18446744073709551617001";
    let bytes = serde_json::to_string(&metadata)
        .unwrap()
        .replace("\"RAW_LARGE\"", huge)
        .into_bytes();
    let mut head = fixture::head(
        &bytes,
        3,
        Some(uuid::Uuid::parse_str(metadata["table-uuid"].as_str().unwrap()).unwrap()),
    );
    let prior = TableMetadataDocument::parse(bytes, &head, fixture::limits()).unwrap();
    let mut schema = metadata["schemas"][0].clone();
    schema["fields"][0]["name"] = json!("renamed");
    schema["fields"][0]["future"] = json!("RAW_LARGE");
    let request = json!({"requirements":[],"updates":[
        {"action":"add-schema","schema":schema}, {"action":"set-current-schema","schema-id":-1}
    ]});
    let request = serde_json::to_string(&request)
        .unwrap()
        .replace("\"RAW_LARGE\"", huge);
    let request = CommitRequest::decode(
        request.as_bytes(),
        CommitRequestLimits {
            json: fixture::limits(),
            requirements: 10,
            updates: 10,
        },
    )
    .unwrap();
    head.generation += 1;
    head.metadata_file = FileId::random();
    head.metadata_location = fixture::table().file("metadata/next.json").unwrap();
    let result = evaluate_metadata_updates(
        &prior,
        &request,
        head,
        1100,
        EvaluationLimits {
            metadata: fixture::limits(),
            requirements: RequirementLimits {
                count: 10,
                text_bytes: 4096,
            },
            updates: 10,
            work_bytes: 8 * 1024 * 1024,
        },
    )
    .unwrap();
    assert_eq!(
        std::str::from_utf8(result.document.canonical())
            .unwrap()
            .matches(huge)
            .count(),
        4
    );
    assert_eq!(
        std::str::from_utf8(prior.canonical())
            .unwrap()
            .matches(huge)
            .count(),
        2
    );
}
