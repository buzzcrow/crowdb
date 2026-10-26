// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv_client::{ChunkKvClient, ClientError, MultiScanRequest};
use crowdb_protocol::chunk_kv::{ChunkKvRpcErrorCode, ScanDirection};

#[path = "common/strict_terminal.rs"]
mod fixture;
use fixture::{catalog, client, TestCatalog, TestFault, TestTransport};

fn request(items: usize, bytes: usize) -> MultiScanRequest {
    MultiScanRequest {
        start: None,
        end: None,
        direction: ScanDirection::Forward,
        max_items: items,
        max_bytes: bytes,
        continuation: None,
    }
}

fn single(keys: &[&[u8]]) -> (ChunkKvClient, Arc<TestCatalog>, Arc<TestTransport>) {
    let source = Arc::new(TestCatalog::new(vec![catalog(1, &[(b"", None, 1)])]));
    let transport = Arc::new(TestTransport::new(keys));
    (client(source.clone(), transport.clone()), source, transport)
}

async fn collect(client: &ChunkKvClient, mut request: MultiScanRequest) -> Vec<Vec<u8>> {
    let mut output = Vec::new();
    for _ in 0..16 {
        let page = client.scan_bounded(request.clone()).await.unwrap();
        assert!(page.terminal_failure.is_none(), "{page:?}");
        output.extend(page.items.into_iter().map(|item| item.key));
        request.continuation = page.continuation;
        if request.continuation.is_none() {
            return output;
        }
    }
    panic!("scan did not finish within the finite fixture");
}

#[tokio::test]
async fn complete_last_partition_returns_its_rows_without_an_empty_confirmation() {
    let (client, source, transport) = single(&[b"a", b"b"]);
    let page = client.scan_bounded(request(256, 1024)).await.unwrap();
    assert_eq!(
        page.items
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        [b"a", b"b"]
    );
    assert!(page.continuation.is_none());
    assert!(page.terminal_failure.is_none());
    assert_eq!(transport.calls().len(), 1);
    assert_eq!(source.loads(), 1);
}

#[tokio::test]
async fn an_explicit_terminal_page_is_final_even_at_exact_item_and_byte_caps() {
    let (client, _, transport) = single(&[b"a"]);
    let page = client.scan_bounded(request(1, 8)).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(page.continuation.is_none());
    assert_eq!(transport.calls()[0].bytes, 8);
}

#[tokio::test]
async fn local_exhaustion_does_not_hide_other_partitions_on_the_same_endpoint() {
    let source = Arc::new(TestCatalog::new(vec![catalog(
        1,
        &[(b"", Some(b"m"), 1), (b"m", None, 1)],
    )]));
    let transport = Arc::new(TestTransport::new(&[b"a", b"z"]));
    let client = client(source, transport.clone());
    let mut query = request(256, 1024);
    let first = client.scan_bounded(query.clone()).await.unwrap();
    assert_eq!(first.items[0].key, b"a");
    assert!(first.continuation.is_some());
    assert_eq!(transport.calls().len(), 1);
    query.continuation = first.continuation;
    let last = client.scan_bounded(query).await.unwrap();
    assert_eq!(last.items[0].key, b"z");
    assert!(last.continuation.is_none());
    let calls = transport.calls();
    assert_eq!(calls.len(), 3);
    assert!(calls.iter().all(|call| call.endpoint == "owner-1"));
    assert_eq!(
        calls
            .iter()
            .map(|call| call.request.routing.partition_id.low)
            .collect::<Vec<_>>(),
        [1, 1, 2]
    );
}

#[tokio::test]
async fn empty_first_middle_and_final_partitions_do_not_drop_rows_or_loop() {
    for keys in [
        vec![b"z".as_slice()],
        vec![b"a".as_slice(), b"z"],
        vec![b"a".as_slice()],
    ] {
        let source = Arc::new(TestCatalog::new(vec![catalog(
            1,
            &[(b"", Some(b"m"), 1), (b"m", Some(b"t"), 2), (b"t", None, 3)],
        )]));
        let transport = Arc::new(TestTransport::new(&keys));
        let client = client(source, transport.clone());
        let expected: Vec<_> = keys.iter().map(|key| key.to_vec()).collect();
        assert_eq!(collect(&client, request(256, 1024)).await, expected);
        assert!(transport.calls().len() <= 5);
    }
}

#[tokio::test]
async fn requested_upper_bounds_select_the_last_relevant_catalog_partition() {
    for (end, expected, first_final) in [
        (b"b".as_slice(), vec![b"a".to_vec()], true),
        (b"m", vec![b"a".to_vec()], true),
        (b"s", vec![b"a".to_vec(), b"n".to_vec()], false),
        (b"t", vec![b"a".to_vec(), b"n".to_vec()], false),
        (b"zz", vec![b"a".to_vec(), b"n".to_vec(), b"z".to_vec()], false),
    ] {
        let source = Arc::new(TestCatalog::new(vec![catalog(
            1,
            &[(b"", Some(b"m"), 1), (b"m", Some(b"t"), 2), (b"t", None, 3)],
        )]));
        let transport = Arc::new(TestTransport::new(&[b"a", b"n", b"z"]));
        let client = client(source, transport.clone());
        let mut query = request(256, 1024);
        query.end = Some(end.to_vec());
        let first = client.scan_bounded(query.clone()).await.unwrap();
        assert_eq!(first.continuation.is_none(), first_final, "end={end:?}");
        let mut actual: Vec<_> = first.items.into_iter().map(|item| item.key).collect();
        if first.continuation.is_some() {
            query.continuation = first.continuation;
            actual.extend(collect(&client, query).await);
        }
        assert_eq!(actual, expected, "end={end:?}");
    }
}

#[tokio::test]
async fn binary_half_open_bounds_and_resumed_limits_preserve_every_key() {
    let (client, _, transport) = single(&[b"p\0a", b"p\0\xff", b"p\x01a"]);
    let mut query = request(1, 64);
    query.start = Some(b"p\0".to_vec());
    query.end = Some(b"p\x01".to_vec());
    assert_eq!(
        collect(&client, query).await,
        [b"p\0a".to_vec(), b"p\0\xff".to_vec()]
    );
    let calls = transport.calls();
    assert_eq!(calls.len(), 2);
    assert!(calls
        .iter()
        .all(|call| call.request.end.as_deref() == Some(b"p\x01".as_slice())));
}

#[tokio::test]
async fn a_native_budget_continuation_is_retained_even_in_the_final_partition() {
    let (client, _, transport) = single(&[b"a"]);
    transport.fault(1, TestFault::Continuation);
    let mut query = request(1, 8);
    let first = client.scan_bounded(query.clone()).await.unwrap();
    assert!(first.continuation.is_some());
    assert!(transport.faults_consumed());
    query.continuation = first.continuation;
    let last = client.scan_bounded(query).await.unwrap();
    assert!(last.items.is_empty());
    assert!(last.continuation.is_none());
    assert_eq!(transport.calls().len(), 2);
}

#[tokio::test]
async fn a_success_under_another_catalog_generation_keeps_the_conservative_cursor() {
    let (client, source, transport) = single(&[b"a"]);
    transport.fault(1, TestFault::Generation(2));
    let mut query = request(256, 1024);
    let first = client.scan_bounded(query.clone()).await.unwrap();
    assert!(first.continuation.is_some());
    assert!(transport.faults_consumed());
    assert_eq!(transport.calls().len(), 1);
    assert_eq!(source.loads(), 1);
    query.continuation = first.continuation;
    let last = client.scan_bounded(query).await.unwrap();
    assert!(last.items.is_empty());
    assert!(last.continuation.is_none());
}

#[tokio::test]
async fn only_a_fully_consumed_terminal_response_can_end_the_requested_range() {
    for (items, bytes) in [(256, 8), (1, 1024)] {
        let (client, _, transport) = single(&[b"a", b"b"]);
        transport.fault(1, TestFault::OverBudgetRows);
        let mut query = request(items, bytes);
        let first = client.scan_bounded(query.clone()).await.unwrap();
        assert_eq!(first.items[0].key, b"a");
        assert!(first.continuation.is_some());
        assert!(transport.faults_consumed());
        query.continuation = first.continuation;
        let last = client.scan_bounded(query).await.unwrap();
        assert_eq!(last.items[0].key, b"b");
        assert!(last.continuation.is_none());
        assert_eq!(transport.calls().len(), 2);
    }
}

#[tokio::test]
async fn stale_owner_refresh_replans_only_after_the_last_emitted_key() {
    for code in [
        ChunkKvRpcErrorCode::NotMyRange,
        ChunkKvRpcErrorCode::RefreshRequired,
    ] {
        let source = Arc::new(TestCatalog::new(vec![
            catalog(1, &[(b"", Some(b"m"), 1), (b"m", None, 2)]),
            catalog(2, &[(b"", Some(b"m"), 3), (b"m", Some(b"t"), 4), (b"t", None, 5)]),
        ]));
        let transport = Arc::new(TestTransport::new(&[b"a", b"n", b"z"]));
        transport.fault(3, TestFault::Route(code));
        let client = client(source.clone(), transport.clone());
        assert_eq!(
            collect(&client, request(256, 1024)).await,
            [b"a".to_vec(), b"n".to_vec(), b"z".to_vec()]
        );
        assert!(transport.faults_consumed(), "code={code:?}");
        assert_eq!(source.loads(), 2, "code={code:?}");
        let calls = transport.calls();
        assert!(calls
            .iter()
            .skip(3)
            .all(|call| call.request.routing.map_revision == 2));
        assert!(calls.iter().skip(3).all(|call| call
            .request
            .continuation
            .as_ref()
            .map_or(true, |token| token.last_key.as_slice() >= b"a".as_slice())));
    }
}

#[tokio::test]
async fn a_merged_catalog_between_pages_keeps_the_unconsumed_range() {
    let source = Arc::new(TestCatalog::new(vec![
        catalog(1, &[(b"", Some(b"m"), 1), (b"m", None, 2)]),
        catalog(2, &[(b"", None, 3)]),
    ]));
    let transport = Arc::new(TestTransport::new(&[b"a", b"n", b"z"]));
    let client = client(source.clone(), transport.clone());
    let mut query = request(256, 1024);
    let first = client.scan_bounded(query.clone()).await.unwrap();
    assert_eq!(first.items[0].key, b"a");
    client.refresh_catalog().await.unwrap();
    query.continuation = first.continuation;
    let last = client.scan_bounded(query).await.unwrap();
    assert_eq!(
        last.items
            .iter()
            .map(|item| item.key.as_slice())
            .collect::<Vec<_>>(),
        [b"n", b"z"]
    );
    assert!(last.continuation.is_none());
    assert_eq!(transport.calls().len(), 2);
    assert_eq!(source.loads(), 2);
}

#[tokio::test]
async fn an_oversized_first_or_following_row_is_never_a_false_successful_end() {
    for oversized in [0, 1] {
        let source = Arc::new(TestCatalog::new(vec![catalog(1, &[(b"", None, 1)])]));
        let transport = Arc::new(TestTransport::new(&[b"a", b"b"]).with_value_size(oversized, 64));
        let client = client(source, transport.clone());
        let mut query = request(256, 8);
        let first = client.scan_bounded(query.clone()).await.unwrap();
        let rejected = if oversized == 0 {
            first
        } else {
            assert_eq!(first.items[0].key, b"a");
            assert!(first.continuation.is_some());
            query.continuation = first.continuation;
            client.scan_bounded(query).await.unwrap()
        };
        assert!(rejected.items.is_empty());
        assert_eq!(
            rejected.terminal_failure.unwrap().code,
            ChunkKvRpcErrorCode::ScanLimitExceeded
        );
        assert_eq!(transport.calls().len(), oversized + 1);
        assert_eq!(transport.legacy_calls(), 0);
    }
}

#[tokio::test]
async fn empty_and_reversed_bounds_still_fail_before_dispatch() {
    for (start, end) in [(b"m", b"m"), (b"z", b"a")] {
        let (client, source, transport) = single(&[b"a"]);
        let mut query = request(256, 1024);
        query.start = Some(start.to_vec());
        query.end = Some(end.to_vec());
        assert!(matches!(
            client.scan_bounded(query).await,
            Err(ClientError::InvalidRequest(_))
        ));
        assert!(transport.calls().is_empty());
        assert_eq!(source.loads(), 0);
    }
}
