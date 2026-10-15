// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn bounded_scan_rejects_oversized_first_value_and_resumes_without_skipping() {
    let (service, _) = fixture().await;
    for (sequence, key, size) in [(1, b"a", 2), (2, b"b", 128), (3, b"c", 2)] {
        let response = service
            .handle_point(
                PointRequest {
                    routing: routing(sequence),
                    operation: PointOperation::Put {
                        key: key.to_vec(),
                        value: vec![7; size],
                    },
                },
                1_500,
                50_100,
            )
            .await;
        assert!(response.result.is_ok(), "{response:?}");
    }
    let request = ScanRequest {
        routing: routing(4),
        start: Some(b"a".to_vec()),
        end: None,
        direction: ScanDirection::Forward,
        limit: 10,
        continuation: None,
    };
    let first = service
        .handle_bounded_scan(request.clone(), 16, 1_500, 50_100)
        .await;
    let OperationResult::Scan { items, continuation } = first.result.unwrap() else {
        panic!("scan expected")
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].key, b"a");
    let mut next = request;
    next.routing = routing(5);
    next.continuation = Some(continuation.expect("budget boundary must retain continuation"));
    let rejected = service.handle_bounded_scan(next.clone(), 16, 1_500, 50_100).await;
    assert_eq!(
        rejected.result.unwrap_err().code,
        ChunkKvRpcErrorCode::InvalidRequest
    );
    let resumed = service.handle_bounded_scan(next, 256, 1_500, 50_100).await;
    let OperationResult::Scan { items, continuation } = resumed.result.unwrap() else {
        panic!("scan expected")
    };
    assert_eq!(
        items.iter().map(|item| item.key.as_slice()).collect::<Vec<_>>(),
        vec![b"b".as_slice(), b"c"]
    );
    assert!(continuation.is_none());
}

pub(super) async fn assert_split_writer_budget(service: &ChunkKvService) {
    let mut request = ScanRequest {
        routing: routing(90),
        start: Some(b"b".to_vec()),
        end: Some(b"z".to_vec()),
        direction: ScanDirection::Forward,
        limit: 8,
        continuation: None,
    };
    let response = service
        .handle_bounded_scan(request.clone(), 3, 1_500, 50_100)
        .await;
    let OperationResult::Scan { items, continuation } = response.result.unwrap() else {
        panic!("scan expected")
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].key, b"b");
    request.continuation = Some(continuation.unwrap());
    request.routing = routing(91);
    let response = service.handle_bounded_scan(request, 3, 1_500, 50_100).await;
    let OperationResult::Scan { items, .. } = response.result.unwrap() else {
        panic!("scan expected")
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].key, b"t");
}
