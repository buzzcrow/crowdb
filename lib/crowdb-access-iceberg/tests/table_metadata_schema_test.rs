#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use serde_json::json;

#[test]
fn schemas_validate_nested_ids_names_types_and_identifiers() {
    for invalid in [
        json!({"id":1,"name":"other","type":"long","required":false}),
        json!({"id":2,"name":"id","type":"long","required":false}),
        json!({"id":2,"name":"other","type":"unsupported","required":false}),
        json!({"id":2,"name":"other","type":{"type":"list","element-id":1,"element":"long","element-required":false},"required":false}),
    ] {
        let mut value = fixture::metadata(3);
        value["last-column-id"] = json!(10);
        value["schemas"][0]["fields"]
            .as_array_mut()
            .unwrap()
            .push(invalid);
        assert!(fixture::parse(&value).is_err());
    }
    let mut value = fixture::metadata(3);
    value["schemas"][0]["identifier-field-ids"] = json!([2]);
    assert!(fixture::parse(&value).is_err());
}

#[test]
fn retained_schemas_are_independent_and_bounded_by_last_column_id() {
    let mut value = fixture::metadata(3);
    let historical = json!({"type":"struct","schema-id":10,"fields":[
        {"id":5,"name":"removed","type":"string","required":false}
    ]});
    value["schemas"].as_array_mut().unwrap().push(historical);
    assert!(fixture::parse(&value).is_err());
    value["last-column-id"] = json!(5);
    assert!(fixture::parse(&value).is_ok());
}
