#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/create_metadata_fixture.rs"]
mod sdk;

use base64::Engine;
use crowdb_access_iceberg::{
    commit::{evaluate_table_creation, CreateTableRequest},
    key::OperationId,
};
use serde_json::Value;

#[test]
fn initial_nested_schema_layout_and_properties_match_pinned_java() {
    for (version, request, expected) in [
        (1, sdk::REQUEST_V1, sdk::OUTPUT_V1),
        (2, sdk::REQUEST_V2, sdk::OUTPUT_V2),
        (3, sdk::REQUEST_V3, sdk::OUTPUT_V3),
    ] {
        let decode = |text| base64::engine::general_purpose::STANDARD.decode(text).unwrap();
        let expected: Value = serde_json::from_slice(&decode(expected)).unwrap();
        let uuid = uuid::Uuid::parse_str(expected["table-uuid"].as_str().unwrap()).unwrap();
        let mut head = fixture::head(&[], version, Some(uuid));
        head.pending_operation = Some(OperationId::random());
        let request = CreateTableRequest::decode(&decode(request), fixture::limits()).unwrap();
        let result = evaluate_table_creation(
            &request,
            head,
            expected["last-updated-ms"].as_i64().unwrap(),
            fixture::limits(),
        )
        .unwrap();
        assert_eq!(
            Value::Object(result.document.fields().clone()),
            expected,
            "version {version}"
        );
    }
}
