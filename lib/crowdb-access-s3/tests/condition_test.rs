// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::condition::{
    evaluate, ConditionOutcome, EtagSelection, ObjectConditions, ReadCondition, ReadPreconditions,
};
use crowdb_access_s3::metadata::{BucketId, ObjectRecord};

fn record() -> ObjectRecord {
    ObjectRecord {
        bucket_id: BucketId::new([1; 16]),
        key: b"key".to_vec(),
        logical_length: 4,
        checksum: b"sum".to_vec(),
        etag: "abc".into(),
        created_at_ms: 1_000,
        modified_at_ms: 2_000,
        content_type: "text/plain".into(),
        attributes: Vec::new(),
        data_reference: vec![1],
        data_length: 4,
    }
}

#[test]
fn etag_conditions_take_precedence_over_dates() {
    let value = record();
    assert_eq!(
        evaluate(
            &value,
            &ObjectConditions {
                if_match: Some("\"other\""),
                if_unmodified_since: Some("Thu, 01 Jan 2099 00:00:00 GMT"),
                ..ObjectConditions::default()
            }
        ),
        ConditionOutcome::PreconditionFailed
    );
    assert_eq!(
        evaluate(
            &value,
            &ObjectConditions {
                if_none_match: Some("\"abc\""),
                ..ObjectConditions::default()
            }
        ),
        ConditionOutcome::NotModified
    );
}

#[test]
fn typed_conditions_match_http_condition_precedence() {
    let value = record();
    for if_match in [None, Some("abc"), Some("other")] {
        for if_none_match in [None, Some("abc"), Some("other")] {
            for seconds in [1, 3] {
                let date =
                    httpdate::fmt_http_date(std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds));
                let http = ObjectConditions {
                    if_match,
                    if_none_match,
                    if_modified_since: Some(&date),
                    if_unmodified_since: Some(&date),
                };
                let typed = ReadPreconditions {
                    if_match: if_match.map(|value| EtagSelection::Values(vec![value.into()])),
                    if_none_match: if_none_match.map(|value| EtagSelection::Values(vec![value.into()])),
                    if_modified_since_seconds: Some(seconds),
                    if_unmodified_since_seconds: Some(seconds),
                };
                assert_eq!(typed.evaluate(&value), evaluate(&value, &http));
            }
        }
    }
}
