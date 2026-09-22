use std::collections::BTreeMap;

use crowdb_access_iceberg::error::ValidationError;
use crowdb_access_iceberg::namespace::{
    NamespaceIdentifier, NamespaceProperties, PropertyChanges, MAX_IDENTIFIER_BYTES, MAX_NAMESPACE_LEVELS,
    MAX_PROPERTIES,
};

#[test]
fn multipart_identifiers_preserve_unicode_and_delimiter_like_text() {
    let components = vec!["数据库".into(), "a.b / %1F".into(), "leaf".into()];
    let identifier = NamespaceIdentifier::new(components.clone()).unwrap();
    assert_eq!(
        NamespaceIdentifier::decode(&identifier.encode().unwrap()).unwrap(),
        identifier
    );
    assert_eq!(
        NamespaceIdentifier::from_rest(&components.join("\u{1f}")).unwrap(),
        identifier
    );
    assert_eq!(identifier.name(), "leaf");
    assert_eq!(identifier.parent().unwrap().components(), &components[..2]);
    assert!(NamespaceIdentifier::new(vec!["root".into()])
        .unwrap()
        .parent()
        .is_none());
}

#[test]
fn namespace_identifier_limits_apply_to_encoded_bytes_and_levels() {
    let levels = NamespaceIdentifier::new(vec!["x".into(); MAX_NAMESPACE_LEVELS]).unwrap();
    assert_eq!(
        NamespaceIdentifier::decode(&levels.encode().unwrap()).unwrap(),
        levels
    );
    assert!(NamespaceIdentifier::new(vec!["x".into(); MAX_NAMESPACE_LEVELS + 1]).is_err());
    let components = vec!["a".repeat(2046), "b".repeat(2046)];
    let full = NamespaceIdentifier::new(components.clone()).unwrap();
    assert_eq!(full.encode().unwrap().len(), MAX_IDENTIFIER_BYTES);
    assert_eq!(
        NamespaceIdentifier::decode(&full.encode().unwrap()).unwrap(),
        full
    );
    let mut oversized = components;
    oversized[1].push('b');
    assert!(NamespaceIdentifier::new(oversized).is_err());
    assert!(NamespaceIdentifier::new(vec!["x".repeat(4056)]).is_err());
}

#[test]
fn malformed_namespace_inputs_fail_closed() {
    for input in ["", "\u{1f}", "a\u{1f}", "\u{1f}a", "a\u{1f}\u{1f}b", "a\0b"] {
        assert!(NamespaceIdentifier::from_rest(input).is_err(), "{input:?}");
    }
    for bytes in [&[][..], &[0], &[0, 2, b'a'], &[0, 1, 255], &[0, 0]] {
        assert!(NamespaceIdentifier::decode(bytes).is_err(), "{bytes:?}");
    }
    assert!(NamespaceIdentifier::new(vec!["a\u{1f}b".into()]).is_err());
}

#[test]
fn property_updates_are_atomic_and_report_removed_updated_and_missing() {
    let original = NamespaceProperties::new(BTreeMap::from([
        ("keep".into(), "original".into()),
        ("remove".into(), "old".into()),
    ]))
    .unwrap();
    let changes = PropertyChanges {
        removals: vec!["remove".into(), "missing".into(), "remove".into()],
        updates: BTreeMap::from([("keep".into(), "new".into()), ("added".into(), "value".into())]),
    };
    let result = original.apply(&changes).unwrap();
    assert_eq!(result.removed, ["remove"]);
    assert_eq!(result.missing, ["missing"]);
    assert_eq!(result.updated, ["added", "keep"]);
    assert_eq!(result.properties.entries()["keep"], "new");
    assert_eq!(original.entries()["keep"], "original");
    let overlap = PropertyChanges {
        removals: vec!["keep".into()],
        updates: BTreeMap::from([("keep".into(), "invalid".into())]),
    };
    assert_eq!(original.apply(&overlap), Err(ValidationError::PropertyOverlap));
    assert_eq!(original.entries().len(), 2);
}

#[test]
fn property_limits_are_byte_based_and_include_result_cardinality() {
    let properties = (0..MAX_PROPERTIES)
        .map(|index| (index.to_string(), String::new()))
        .collect();
    let full = NamespaceProperties::new(properties).unwrap();
    let changes = PropertyChanges {
        removals: Vec::new(),
        updates: BTreeMap::from([("overflow".into(), String::new())]),
    };
    assert_eq!(full.apply(&changes), Err(ValidationError::RecordTooLarge));
    let replacement = PropertyChanges {
        removals: vec!["0".into()],
        ..changes
    };
    assert_eq!(
        full.apply(&replacement).unwrap().properties.entries().len(),
        MAX_PROPERTIES
    );
    for (key, value, valid) in [
        ("k".repeat(1024), "v".repeat(8192), true),
        ("k".repeat(1025), String::new(), false),
        ("键".repeat(342), String::new(), false),
        ("k".into(), "v".repeat(8193), false),
        ("\0".into(), String::new(), false),
        ("k".into(), "\0".into(), false),
    ] {
        assert_eq!(
            NamespaceProperties::new(BTreeMap::from([(key, value)])).is_ok(),
            valid
        );
    }
    let oversized = (0..9)
        .map(|index| (index.to_string(), "v".repeat(8192)))
        .collect();
    assert_eq!(
        NamespaceProperties::new(oversized),
        Err(ValidationError::RecordTooLarge)
    );
}
