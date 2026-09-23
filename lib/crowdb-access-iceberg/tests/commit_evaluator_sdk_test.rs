#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/commit_metadata_fixture.rs"]
mod sdk;

use base64::Engine;
use crowdb_access_iceberg::{
    commit::{
        evaluate_metadata_updates, CommitRequest, CommitRequestLimits, EvaluationLimits, RequirementLimits,
    },
    key::FileId,
    table::TableMetadataDocument,
};
use serde_json::Value;

#[test]
fn ordered_schema_layout_and_property_updates_match_pinned_java_builder() {
    for (version, input, request, expected) in [
        (1, sdk::INPUT_V1, sdk::REQUEST_V1, sdk::OUTPUT_V1),
        (2, sdk::INPUT_V2, sdk::REQUEST_V2, sdk::OUTPUT_V2),
        (3, sdk::INPUT_V3, sdk::REQUEST_V3, sdk::OUTPUT_V3),
    ] {
        let decode = |text| base64::engine::general_purpose::STANDARD.decode(text).unwrap();
        let input = decode(input);
        let expected: Value = serde_json::from_slice(&decode(expected)).unwrap();
        let uuid = uuid::Uuid::parse_str(expected["table-uuid"].as_str().unwrap()).unwrap();
        let mut head = fixture::head(&input, version, Some(uuid));
        let prior = TableMetadataDocument::parse(input, &head, fixture::limits()).unwrap();
        let request = CommitRequest::decode(
            &decode(request),
            CommitRequestLimits {
                json: fixture::limits(),
                requirements: 100,
                updates: 100,
            },
        )
        .unwrap();
        head.generation += 1;
        head.metadata_file = FileId::random();
        head.metadata_location = fixture::table().file("metadata/next.metadata.json").unwrap();
        let result = evaluate_metadata_updates(
            &prior,
            &request,
            head,
            expected["last-updated-ms"].as_i64().unwrap(),
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
        .unwrap();
        assert_eq!(
            Value::Object(result.document.fields().clone()),
            expected,
            "version {version}"
        );
    }
}
