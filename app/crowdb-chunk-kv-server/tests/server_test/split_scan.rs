// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) async fn assert_old_parent_right_interval(service: &ChunkKvService) {
    for (index, direction) in [ScanDirection::Forward, ScanDirection::Reverse]
        .into_iter()
        .enumerate()
    {
        let request = ScanRequest {
            routing: routing(100 + index as u64),
            start: Some(b"s".to_vec()),
            end: Some(b"z".to_vec()),
            direction,
            limit: 8,
            continuation: None,
        };
        let response = service.handle_scan(request.clone(), 1_500, 50_100).await;
        let OperationResult::Scan { items, continuation } = response.result.unwrap() else {
            panic!("expected a scan through the historical parent route")
        };
        assert_eq!(
            items.iter().map(|item| item.key.as_slice()).collect::<Vec<_>>(),
            [b"t"],
            "a stale parent route must scan the right writer when its lower bound is past the split"
        );
        assert!(continuation.is_none());
        if direction == ScanDirection::Forward {
            let response = service.handle_bounded_scan(request, 1024, 1_500, 50_100).await;
            let OperationResult::Scan { items, .. } = response.result.unwrap() else {
                panic!("expected a bounded historical parent scan")
            };
            assert_eq!(
                items.iter().map(|item| item.key.as_slice()).collect::<Vec<_>>(),
                [b"t"]
            );
        }
    }
    assert_current_assignment_bounds(service).await;
}

async fn assert_current_assignment_bounds(service: &ChunkKvService) {
    let mut request = ScanRequest {
        routing: RequestRouting {
            map_revision: 2,
            owner_epoch: EPOCH + 1,
            ..routing(102)
        },
        start: Some(b"s".to_vec()),
        end: Some(b"z".to_vec()),
        direction: ScanDirection::Forward,
        limit: 8,
        continuation: None,
    };
    let response = service.handle_scan(request.clone(), 1_500, 50_100).await;
    assert_eq!(
        response.result.unwrap_err().code,
        ChunkKvRpcErrorCode::NotMyRange,
        "the current left assignment must not inherit the historical parent's full range"
    );
    request.routing.partition_id = Id128 { high: 22, low: 25 };
    let response = service.handle_scan(request, 1_500, 50_100).await;
    let OperationResult::Scan { items, continuation } = response.result.unwrap() else {
        panic!("expected the current child assignment to contain the right interval")
    };
    assert_eq!(
        items.iter().map(|item| item.key.as_slice()).collect::<Vec<_>>(),
        [b"t"]
    );
    assert!(continuation.is_none());
}
