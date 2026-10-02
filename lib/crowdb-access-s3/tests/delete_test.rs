// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use crowdb_access_s3::delete::{validate_integrity, DeleteSelection, MAX_DELETE_BODY};
use crowdb_access_s3::route::{classify_request, S3Operation};
use crowdb_access_s3::{wire, S3ErrorCode};
use hyper::{body::Bytes, header::HeaderValue};
use hyper::{HeaderMap, Method};

#[test]
fn batch_route_and_exact_keys() {
    let route = classify_request(
        &Method::POST,
        &"/bucket?delete".parse().unwrap(),
        &HeaderMap::new(),
    )
    .unwrap();
    assert_eq!(route.operation, S3Operation::DeleteObjects);
    let selection = DeleteSelection::parse("<Delete><Object><Key>a&amp;&lt;雪</Key></Object><Object><Key>a&amp;&lt;雪</Key></Object><Quiet>true</Quiet></Delete>".as_bytes()).unwrap();
    assert!(selection.quiet);
    assert_eq!(selection.keys, vec!["a&<雪".as_bytes(), "a&<雪".as_bytes()]);
}

#[test]
fn whole_request_rejects_extensions_and_bad_structure() {
    for xml in [
        "",
        "<Delete/>",
        "<Delete></Delete>",
        "<Delete><Object><Key>ok</Key><VersionId>1</VersionId></Object></Delete>",
        "<Delete><Object><Key>ok</Key></Object><Quiet>1</Quiet></Delete>",
        "<Delete><Object><Key>ok</Key><Key>bad</Key></Object></Delete>",
        "<!DOCTYPE Delete><Delete><Object><Key>x</Key></Object></Delete>",
        "<Delete><Object><Key>x</Key></Object></Delete><Delete>",
        "<Delete><Object><Key>x</Object></Key></Delete>",
    ] {
        assert!(DeleteSelection::parse(xml.as_bytes()).is_err(), "{xml}");
    }
}

#[test]
fn bounded_selection_and_key_lengths() {
    let entry = "<Object><Key>x</Key></Object>";
    let xml = format!("<Delete>{}</Delete>", entry.repeat(1_000));
    assert_eq!(DeleteSelection::parse(xml.as_bytes()).unwrap().keys.len(), 1_000);
    let xml = format!("<Delete>{}</Delete>", entry.repeat(1_001));
    assert!(DeleteSelection::parse(xml.as_bytes()).is_err());
    let xml = format!(
        "<Delete><Object><Key>{}</Key></Object></Delete>",
        "x".repeat(1_025)
    );
    assert!(DeleteSelection::parse(xml.as_bytes()).is_err());
}

#[test]
fn quiet_retains_errors_and_verbose_escapes_successes() {
    let results = vec![
        (b"a&<".to_vec(), Ok(())),
        (b"error".to_vec(), Err(S3ErrorCode::ServiceUnavailable)),
    ];
    let quiet = wire::delete_objects(&results, true);
    assert!(!quiet.contains("<Deleted>"));
    assert!(quiet.contains("<Code>ServiceUnavailable</Code>"));
    let verbose = wire::delete_objects(&results, false);
    assert!(verbose.contains("<Deleted><Key>a&amp;&lt;</Key></Deleted>"));
}

#[test]
fn integrity_requires_and_checks_every_declared_supported_digest() {
    let bytes = Bytes::from_static(b"<Delete><Object><Key>key</Key></Object></Delete>");
    let mut headers = HeaderMap::new();
    assert_eq!(
        validate_integrity(&headers, &bytes),
        Err(S3ErrorCode::InvalidDigest)
    );
    headers.insert(
        "content-md5",
        HeaderValue::from_str(&STANDARD.encode(md5::compute(&bytes).0)).unwrap(),
    );
    assert!(validate_integrity(&headers, &bytes).is_ok());
    headers.insert(
        "x-amz-checksum-crc32",
        HeaderValue::from_str(
            &STANDARD.encode(
                crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC)
                    .checksum(&bytes)
                    .to_be_bytes(),
            ),
        )
        .unwrap(),
    );
    headers.insert("x-amz-sdk-checksum-algorithm", HeaderValue::from_static("CRC32"));
    assert!(validate_integrity(&headers, &bytes).is_ok());
    headers.remove("content-md5");
    assert!(validate_integrity(&headers, &bytes).is_ok());
    headers.insert(
        "content-md5",
        HeaderValue::from_str(&STANDARD.encode([0; 16])).unwrap(),
    );
    assert_eq!(validate_integrity(&headers, &bytes), Err(S3ErrorCode::BadDigest));
    headers.remove("content-md5");
    headers.insert(
        "x-amz-checksum-crc32",
        HeaderValue::from_str(&STANDARD.encode([0; 4])).unwrap(),
    );
    assert_eq!(validate_integrity(&headers, &bytes), Err(S3ErrorCode::BadDigest));
    headers.insert("x-amz-checksum-crc32", HeaderValue::from_static("bad"));
    assert_eq!(
        validate_integrity(&headers, &bytes),
        Err(S3ErrorCode::InvalidDigest)
    );
    headers.append("x-amz-checksum-crc32", HeaderValue::from_static("AAAAAA=="));
    assert_eq!(
        validate_integrity(&headers, &bytes),
        Err(S3ErrorCode::InvalidDigest)
    );
}

#[test]
fn xml_controls_declarations_depth_and_body_budget_fail_closed() {
    for xml in [
        "<?xml version=\"1.0\"?><?xml version=\"1.0\"?><Delete><Object><Key>x</Key></Object></Delete>",
        "<?xml version=\"1.0\" encoding=\"UTF-16\"?><Delete><Object><Key>x</Key></Object></Delete>",
        "<Delete><Object><Key>&#0;</Key></Object></Delete>",
        "<Delete><Object><Key>&#1;</Key></Object></Delete>",
        "<Delete><Object><Key><Nested>x</Nested></Key></Object></Delete>",
        "<Delete><Object><Key>&unknown;</Key></Object></Delete>",
    ] {
        assert!(DeleteSelection::parse(xml.as_bytes()).is_err(), "{xml}");
    }
    assert!(DeleteSelection::parse(&vec![b' '; MAX_DELETE_BODY + 1]).is_err());
    let selection = DeleteSelection::parse(b"<Delete><Object><Key>a&#13;b</Key></Object></Delete>").unwrap();
    assert_eq!(selection.keys, vec![b"a\rb".to_vec()]);
    assert!(wire::delete_objects(&[(selection.keys[0].clone(), Ok(()))], false).contains("a&#13;b"));
    let selection = DeleteSelection::parse(b"<Delete><Object><Key>a\r\nb</Key></Object></Delete>").unwrap();
    assert_eq!(selection.keys, vec![b"a\nb".to_vec()]);
}

#[tokio::test]
async fn independent_failures_and_duplicates_preserve_order_without_rollback() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let count = AtomicUsize::new(0);
    let deleted = AtomicUsize::new(0);
    let selection = DeleteSelection {
        keys: vec![b"a".to_vec(), b"failure".to_vec(), b"a".to_vec(), b"c".to_vec()],
        quiet: true,
    };
    let results = selection
        .execute(|key| {
            count.fetch_add(1, Ordering::Relaxed);
            let deleted = &deleted;
            async move {
                if key == b"failure" {
                    return Err(S3ErrorCode::ServiceUnavailable);
                }
                deleted.fetch_or(if key == b"a" { 1 } else { 4 }, Ordering::Relaxed);
                Ok(())
            }
        })
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 4);
    assert_eq!(deleted.load(Ordering::Relaxed), 5);
    assert_eq!(
        results.iter().map(|(key, _)| key).collect::<Vec<_>>(),
        selection.keys.iter().collect::<Vec<_>>()
    );
    let xml = wire::delete_objects(&results, true);
    assert!(!xml.contains("<Deleted>"));
    assert!(xml.contains("<Key>failure</Key><Code>ServiceUnavailable</Code>"));
    let invalid = DeleteSelection {
        keys: vec![b"a".to_vec(), vec![]],
        quiet: false,
    };
    assert!(invalid
        .execute(|_| {
            count.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(()))
        })
        .await
        .is_err());
    assert_eq!(count.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn dropping_a_pending_batch_stops_after_completed_and_one_inflight_key() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let started = AtomicUsize::new(0);
    let completed = AtomicUsize::new(0);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut sender = Some(sender);
    let selection = DeleteSelection {
        keys: (0..1000)
            .map(|index| format!("key-{index}").into_bytes())
            .collect(),
        quiet: false,
    };
    let mut future = Box::pin(selection.execute(|key| {
        started.fetch_add(1, Ordering::Relaxed);
        let signal = if key == b"key-1" { sender.take() } else { None };
        let completed = &completed;
        async move {
            if let Some(sender) = signal {
                let _ = sender.send(());
                std::future::pending::<()>().await;
            }
            completed.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }));
    tokio::select! { _ = receiver => {}, _ = &mut future => panic!("batch unexpectedly completed") }
    drop(future);
    assert_eq!(started.load(Ordering::Relaxed), 2);
    assert_eq!(completed.load(Ordering::Relaxed), 1);
}
