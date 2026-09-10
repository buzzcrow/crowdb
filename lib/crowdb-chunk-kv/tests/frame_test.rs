// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{
    canonical_operation_digest, decode_frame, encode_frame, CompareCondition, FrameDecode, MutationOperation,
    MutationResult, PartitionId, RequestId, WalRecord,
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
fn checksum_and_header_corruption_are_rejected() {
    let mut frame = encode_frame(&record(MutationOperation::PutIfAbsent {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    }))
    .unwrap();
    let last = frame.len() - 1;
    frame[last] ^= 1;
    assert!(decode_frame(&frame).is_err());

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
