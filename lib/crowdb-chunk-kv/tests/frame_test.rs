// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{
    canonical_operation_digest, decode_frame, encode_frame, CompareCondition, FrameDecode, MutationOperation,
    MutationResult, PartitionId, PartitionRange, RequestId, SplitChild, SplitPlan, TransitionId,
    ValueRevision, WalRecord,
};
fn record(operation: MutationOperation) -> WalRecord {
    WalRecord {
        partition_id: PartitionId { high: 1, low: 2 },
        ownership_epoch: 7,
        mutation_seq: 9,
        request_id: RequestId {
            client_high: 3,
            client_low: 4,
            client_sequence: 5,
        },
        operation_digest: canonical_operation_digest(&operation),
        result: MutationResult::Applied { revision: 9 },
        operation,
    }
}

#[derive(serde::Serialize)]
enum TestLegacyOperation<'a> {
    Put {
        key: &'a Vec<u8>,
        value: &'a Vec<u8>,
    },
    Delete {
        key: &'a Vec<u8>,
    },
    PutIfAbsent {
        key: &'a Vec<u8>,
        value: &'a Vec<u8>,
    },
    CompareExchange {
        key: &'a Vec<u8>,
        condition: TestLegacyCondition<'a>,
        value: &'a Vec<u8>,
    },
    ConditionalDelete {
        key: &'a Vec<u8>,
        condition: TestLegacyCondition<'a>,
    },
}

#[derive(serde::Serialize)]
enum TestLegacyCondition<'a> {
    Revision(u64),
    Value(&'a Vec<u8>),
}

#[derive(serde::Serialize)]
enum TestLegacyResult<'a> {
    Applied { revision: u64 },
    ConditionFailed { observed: Option<(u64, &'a Vec<u8>)> },
}

fn legacy_condition(condition: &CompareCondition) -> TestLegacyCondition<'_> {
    match condition {
        CompareCondition::Revision(revision) => TestLegacyCondition::Revision(*revision),
        CompareCondition::Value(value) => TestLegacyCondition::Value(value),
    }
}

fn legacy_operation(operation: &MutationOperation) -> TestLegacyOperation<'_> {
    match operation {
        MutationOperation::Put { key, value } => TestLegacyOperation::Put { key, value },
        MutationOperation::Delete { key } => TestLegacyOperation::Delete { key },
        MutationOperation::PutIfAbsent { key, value } => TestLegacyOperation::PutIfAbsent { key, value },
        MutationOperation::CompareExchange {
            key,
            condition,
            value,
        } => TestLegacyOperation::CompareExchange {
            key,
            condition: legacy_condition(condition),
            value,
        },
        MutationOperation::ConditionalDelete { key, condition } => TestLegacyOperation::ConditionalDelete {
            key,
            condition: legacy_condition(condition),
        },
    }
}

#[test]
fn bulk_byte_codec_preserves_legacy_wal_bytes_for_every_mutation() {
    let operations = [
        MutationOperation::Put {
            key: b"k".to_vec(),
            value: vec![37; 65_536],
        },
        MutationOperation::Delete { key: b"k".to_vec() },
        MutationOperation::PutIfAbsent {
            key: b"k".to_vec(),
            value: vec![0, 255],
        },
        MutationOperation::CompareExchange {
            key: b"k".to_vec(),
            condition: CompareCondition::Revision(9),
            value: vec![1, 255],
        },
        MutationOperation::CompareExchange {
            key: b"k".to_vec(),
            condition: CompareCondition::Value(vec![2, 255]),
            value: vec![3, 255],
        },
        MutationOperation::ConditionalDelete {
            key: b"k".to_vec(),
            condition: CompareCondition::Value(vec![4, 255]),
        },
    ];
    for operation in operations {
        for result in [
            MutationResult::Applied { revision: 9 },
            MutationResult::ConditionFailed { observed: None },
            MutationResult::ConditionFailed {
                observed: Some(ValueRevision {
                    revision: 7,
                    value: vec![0, 255],
                }),
            },
        ] {
            let mut record = record(operation.clone());
            record.result = result;
            let result = match &record.result {
                MutationResult::Applied { revision } => TestLegacyResult::Applied { revision: *revision },
                MutationResult::ConditionFailed { observed } => TestLegacyResult::ConditionFailed {
                    observed: observed.as_ref().map(|value| (value.revision, &value.value)),
                },
            };
            let legacy = bincode::serialize(&(
                record.partition_id,
                record.ownership_epoch,
                record.mutation_seq,
                record.request_id,
                record.operation_digest,
                result,
                legacy_operation(&record.operation),
            ))
            .unwrap();
            let frame = encode_frame(&record).unwrap();
            assert_eq!(&frame[12..], legacy, "WAL format must remain unchanged");
            let FrameDecode::Complete(decoded) = decode_frame(&frame).unwrap() else {
                panic!("complete legacy-compatible frame expected");
            };
            assert_eq!(decoded.record, record);
        }
    }
}

#[test]
fn frame_round_trip_and_concatenation_boundary() {
    let record = record(MutationOperation::Put {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
    });
    let frame = encode_frame(&record).unwrap();
    let mut joined = frame.clone();
    joined.extend_from_slice(b"next");
    let FrameDecode::Complete(decoded) = decode_frame(&joined).unwrap() else {
        panic!("complete frame expected");
    };
    assert_eq!(decoded.record, record);
    assert_eq!(decoded.bytes_consumed, frame.len());
}

#[test]
fn incomplete_tail_reports_exact_required_size() {
    let frame = encode_frame(&record(MutationOperation::Delete { key: b"k".to_vec() })).unwrap();
    for length in 0..frame.len() {
        let FrameDecode::Incomplete { required_bytes } = decode_frame(&frame[..length]).unwrap() else {
            panic!("truncated frame must remain incomplete");
        };
        assert!(required_bytes > length);
    }
}

#[test]
fn header_corruption_is_rejected_by_the_journal_payload_parser() {
    let mut frame = encode_frame(&record(MutationOperation::PutIfAbsent {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    }))
    .unwrap();
    frame[0] ^= 1;
    assert!(decode_frame(&frame).is_err());
}

#[test]
fn digest_covers_condition_and_value_but_not_routing() {
    let first = MutationOperation::CompareExchange {
        key: b"k".to_vec(),
        condition: CompareCondition::Revision(4),
        value: b"a".to_vec(),
    };
    let same = first.clone();
    let changed = MutationOperation::CompareExchange {
        key: b"k".to_vec(),
        condition: CompareCondition::Revision(5),
        value: b"a".to_vec(),
    };
    assert_eq!(
        canonical_operation_digest(&first),
        canonical_operation_digest(&same)
    );
    assert_ne!(
        canonical_operation_digest(&first),
        canonical_operation_digest(&changed)
    );
}

#[test]
fn partition_ranges_are_half_open_and_split_exactly() {
    let range = crowdb_chunk_kv::PartitionRange {
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
    };
    assert!(range.contains(b"a"));
    assert!(range.contains(b"m"));
    assert!(!range.contains(b"z"));
    let (left, right) = range.split(b"m").unwrap();
    assert_eq!(left.end, right.start);
    assert!(range.split(b"a").is_err());
    assert!(range.split(b"z").is_err());
}

#[test]
fn split_plan_requires_retained_parent_and_exact_child() {
    let parent = PartitionRange {
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
    };
    let plan = SplitPlan {
        transition_id: TransitionId { high: 8, low: 9 },
        parent_id: PartitionId { high: 1, low: 1 },
        parent_range: parent.clone(),
        parent_epoch: 4,
        parent_next_epoch: 5,
        split_key: b"m".to_vec(),
        child: SplitChild {
            partition_id: PartitionId { high: 2, low: 2 },
            range: PartitionRange {
                start: Some(b"m".to_vec()),
                end: Some(b"z".to_vec()),
            },
            ownership_epoch: 6,
        },
    };
    plan.validate().unwrap();

    let mut changed = plan.clone();
    changed.child.range.start = Some(b"n".to_vec());
    assert!(changed.validate().is_err());
    let mut duplicate = plan;
    duplicate.child.partition_id = duplicate.parent_id;
    assert!(duplicate.validate().is_err());
}
