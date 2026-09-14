// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_server::{validate_and_clip_scan, ClippedScan, ScanValidationError};
use crowdb_protocol::chunk_kv::{
    ClientRequestId, Id128, KeyRange, RequestRouting, ScanContinuation, ScanDirection, ScanRequest,
};

fn request() -> ScanRequest {
    ScanRequest {
        routing: RequestRouting {
            request_id: ClientRequestId {
                client_instance_id: Id128 { high: 1, low: 2 },
                client_sequence: 3,
            },
            map_revision: 4,
            partition_id: Id128 { high: 5, low: 6 },
            owner_epoch: 7,
            min_journal_position: None,
            deadline_ms: None,
        },
        start: Some(b"a".to_vec()),
        end: Some(b"z".to_vec()),
        direction: ScanDirection::Forward,
        limit: 20,
        continuation: None,
    }
}

#[test]
fn interval_is_clipped_to_partition_before_execution() {
    let clipped = validate_and_clip_scan(
        &request(),
        &KeyRange {
            start: b"g".to_vec(),
            end: Some(b"t".to_vec()),
        },
    )
    .unwrap();
    assert_eq!(
        clipped,
        ClippedScan {
            start: b"g".to_vec(),
            end: Some(b"t".to_vec()),
            direction: ScanDirection::Forward,
            limit: 20,
            resume_after: None,
        }
    );
}

#[test]
fn topology_change_requires_refresh() {
    let mut request = request();
    request.continuation = Some(ScanContinuation {
        direction: ScanDirection::Forward,
        last_key: b"m".to_vec(),
        partition_id: request.routing.partition_id,
        owner_epoch: request.routing.owner_epoch - 1,
        map_revision: request.routing.map_revision,
    });
    assert_eq!(
        validate_and_clip_scan(
            &request,
            &KeyRange {
                start: Vec::new(),
                end: None,
            }
        ),
        Err(ScanValidationError::RefreshRequired)
    );
}

#[test]
fn disjoint_and_out_of_interval_continuations_fail_closed() {
    let mut scan = request();
    assert_eq!(
        validate_and_clip_scan(
            &scan,
            &KeyRange {
                start: b"zz".to_vec(),
                end: None,
            }
        ),
        Err(ScanValidationError::NotMyRange)
    );
    scan.continuation = Some(ScanContinuation {
        direction: ScanDirection::Forward,
        last_key: b"0".to_vec(),
        partition_id: scan.routing.partition_id,
        owner_epoch: scan.routing.owner_epoch,
        map_revision: scan.routing.map_revision,
    });
    assert_eq!(
        validate_and_clip_scan(
            &scan,
            &KeyRange {
                start: Vec::new(),
                end: None,
            }
        ),
        Err(ScanValidationError::InvalidRequest)
    );
}
