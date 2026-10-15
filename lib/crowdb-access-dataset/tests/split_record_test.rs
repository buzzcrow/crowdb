// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    split::{
        key, MembershipOrigin, MembershipPage, MembershipPageBinding, NamedSplitRecord, SplitDefinition,
        SplitHead, SplitName, SplitPolicy, SplitRevisionRecord,
    },
    DatasetIdentity, NamespacePath, OperationId, Selection, SnapshotId, SplitRevisionId,
};

fn revision() -> SplitRevisionRecord {
    SplitRevisionRecord {
        version: 1,
        snapshot: SnapshotId::from_bytes(&[1; 16]).unwrap(),
        revision: SplitRevisionId::from_bytes(&[2; 16]).unwrap(),
        operation: OperationId::from_bytes(&[3; 16]).unwrap(),
        evaluator_version: 1,
        policy: SplitPolicy::default(),
        splits: vec![],
    }
}

#[test]
fn empty_set_revision_is_distinct_from_no_revision() {
    let record = revision();
    assert_eq!(
        SplitRevisionRecord::decode(&record.encode().unwrap()).unwrap(),
        record
    );
    let absent = SplitHead {
        version: 1,
        snapshot: record.snapshot,
        revision: None,
        fence: record.operation,
    };
    let published = SplitHead {
        revision: Some(record.revision),
        ..absent.clone()
    };
    assert_ne!(absent.encode().unwrap(), published.encode().unwrap());
    let mut missing = serde_json::to_value(&absent).unwrap();
    missing.as_object_mut().unwrap().remove("revision");
    assert!(SplitHead::decode(&serde_json::to_vec(&missing).unwrap()).is_err());
    assert_eq!(
        SplitHead::decode(&published.encode().unwrap()).unwrap(),
        published
    );
}

#[test]
fn opaque_names_are_exact_and_keys_bind_every_identity() {
    assert!(SplitName::new("").is_err());
    assert!(SplitName::new("é".repeat(128)).is_err());
    assert!(SplitName::new("a".repeat(255)).is_ok());
    assert_ne!(SplitName::new("é").unwrap(), SplitName::new("e\u{301}").unwrap());
    let record = revision();
    let identity = DatasetIdentity::new(NamespacePath::root(), "a").unwrap();
    let name = SplitName::new("train/../\0%2F").unwrap();
    let first = key::membership_page(&identity, record.snapshot, record.revision, &name, 0).unwrap();
    let mut keys = std::collections::BTreeSet::from([first]);
    for (dataset, snapshot, revision, name, page) in [
        (
            identity.clone(),
            record.snapshot,
            record.revision,
            name.clone(),
            1,
        ),
        (
            identity.clone(),
            record.snapshot,
            record.revision,
            SplitName::new("train").unwrap(),
            0,
        ),
        (
            identity.clone(),
            record.snapshot,
            SplitRevisionId::random(),
            name.clone(),
            0,
        ),
        (
            identity.clone(),
            SnapshotId::random(),
            record.revision,
            name.clone(),
            0,
        ),
        (
            DatasetIdentity::new(NamespacePath::root(), "ab").unwrap(),
            record.snapshot,
            record.revision,
            name,
            0,
        ),
    ] {
        assert!(keys.insert(key::membership_page(&dataset, snapshot, revision, &name, page).unwrap()));
    }
    assert!(keys.insert(key::head(&identity, record.snapshot).unwrap()));
    assert!(keys.insert(key::revision(&identity, record.snapshot, record.revision).unwrap()));
}

#[test]
fn versions_identities_and_trailing_data_fail_closed() {
    let record = revision();
    let bytes = record.encode().unwrap();
    for field in ["version", "evaluator_version"] {
        let mut value = serde_json::to_value(&record).unwrap();
        value[field] = 99.into();
        assert!(SplitRevisionRecord::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    for field in ["snapshot", "revision", "operation"] {
        let mut value = serde_json::to_value(&record).unwrap();
        value[field] = serde_json::to_value([0u8; 16]).unwrap();
        assert!(SplitRevisionRecord::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(b'x');
    assert!(SplitRevisionRecord::decode(&trailing).is_err());
    assert!(SplitRevisionRecord::decode(&bytes[..bytes.len() - 1]).is_err());
    assert!(SplitRevisionRecord::decode(&vec![0; 1024 * 1024 + 1]).is_err());
    let mut future = serde_json::to_value(record).unwrap();
    future["required_future_policy"] = true.into();
    assert!(SplitRevisionRecord::decode(&serde_json::to_vec(&future).unwrap()).is_err());
}

#[test]
fn pages_require_unique_ordered_nonempty_ids_and_complete_bindings() {
    let mut record = revision();
    let mut page = MembershipPage {
        version: 1,
        snapshot: record.snapshot,
        revision: record.revision,
        name: SplitName::new("train").unwrap(),
        page: 0,
        members: vec![vec![0], vec![255]],
    };
    assert_eq!(MembershipPage::decode(&page.encode().unwrap()).unwrap(), page);
    let binding = page.binding().unwrap();
    page.verify_binding(record.snapshot, record.revision, &page.name, &binding)
        .unwrap();
    assert!(page
        .verify_binding(SnapshotId::random(), record.revision, &page.name, &binding)
        .is_err());
    page.members[1] = vec![254];
    assert!(page
        .verify_binding(record.snapshot, record.revision, &page.name, &binding)
        .is_err());
    for ids in [
        vec![],
        vec![vec![]],
        vec![vec![1], vec![1]],
        vec![vec![2], vec![1]],
    ] {
        page.members = ids;
        assert!(page.encode().is_err());
    }
    record.splits.push(NamedSplitRecord {
        name: page.name,
        definition: SplitDefinition::Selection(Selection::Explicit(vec![vec![0], vec![255]])),
        origin: MembershipOrigin::Evaluated,
        count: 2,
        pages: vec![MembershipPageBinding {
            page: 0,
            count: 2,
            sha256: [1; 32],
        }],
    });
    record.validate().unwrap();
    record.splits[0].count = 3;
    assert!(record.validate().is_err());
    record.splits[0].count = 2;
    record.splits[0].pages[0].page = 1;
    assert!(record.validate().is_err());
    record.splits[0].pages[0].page = 0;
    record.splits.push(record.splits[0].clone());
    assert!(record.validate().is_err());
}

#[test]
fn recursive_rules_are_bounded_before_matching_and_empty_equality_remains_valid() {
    let mut record = revision();
    record.splits.push(NamedSplitRecord {
        name: SplitName::new("empty").unwrap(),
        definition: SplitDefinition::Selection(Selection::FieldMatch {
            field: "label".into(),
            value: vec![],
        }),
        origin: MembershipOrigin::Evaluated,
        count: 0,
        pages: vec![],
    });
    record.validate().unwrap();
    let mut selection = Selection::Explicit(vec![]);
    for _ in 0..34 {
        selection = Selection::Not(Box::new(selection));
    }
    record.splits[0].definition = SplitDefinition::Selection(selection);
    assert!(record.encode().is_err());
    let mut malicious = String::from("{\"Selection\":");
    for _ in 0..200 {
        malicious.push_str("{\"Not\":");
    }
    malicious.push_str("{\"Explicit\":[]}");
    malicious.push_str(&"}".repeat(201));
    let mut value = serde_json::to_value(revision()).unwrap();
    value["splits"] = serde_json::json!([{
        "name":"train", "definition":"PLACEHOLDER", "origin":"Evaluated", "count":0, "pages":[]
    }]);
    let bytes = value.to_string().replace("\"PLACEHOLDER\"", &malicious);
    assert!(SplitRevisionRecord::decode(bytes.as_bytes()).is_err());
}
