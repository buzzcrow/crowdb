// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::condition::{evaluate, ConditionOutcome, ObjectConditions};
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
