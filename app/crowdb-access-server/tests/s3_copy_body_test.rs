// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::{wire, S3ErrorCode};
use crowdb_access_server::s3::copy_body;
use http_body_util::BodyExt;
use std::time::Duration;

struct TestCopyFailure;
impl crowdb_access_server::s3::S3HttpHandler for TestCopyFailure {
    fn handle(
        &self,
        _request: hyper::Request<hyper::body::Incoming>,
    ) -> crowdb_access_server::s3::HandlerFuture {
        Box::pin(async {
            Ok(copy_body::response(
                async {
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                    Err(S3ErrorCode::ServiceUnavailable)
                },
                "/bucket/key".into(),
            )
            .unwrap())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the pinned boto3 environment"]
async fn official_boto3_recognizes_a_copy_error_after_http_200_and_keepalives() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(crowdb_access_server::s3::serve(
        listener,
        std::sync::Arc::new(TestCopyFailure),
        async {
            let _ = receiver.await;
        },
    ));
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("timeout")
            .arg("60")
            .arg(std::env::var_os("CROWDB_S3_E2E_PYTHON").expect("run the pinned boto3 task"))
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/s3_e2e/embedded_copy_error.py"
            ))
            .arg(format!("http://{address}"))
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    let _ = sender.send(());
    server.await.unwrap().unwrap();
    assert!(status.success());
}

#[tokio::test(start_paused = true)]
async fn copy_keepalives_end_in_success_or_embedded_error_without_an_invalid_xml_declaration() {
    for fail in [false, true] {
        let response = copy_body::response(
            async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                if fail {
                    Err(S3ErrorCode::ServiceUnavailable)
                } else {
                    Ok(wire::copy_result("abc", 1000, false))
                }
            },
            "/bucket/key".into(),
        )
        .unwrap();
        assert_eq!(response.status(), 200);
        let mut body = response.into_body();
        let progress = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(progress.as_ref(), b"\n");
        let xml = String::from_utf8(body.collect().await.unwrap().to_bytes().to_vec()).unwrap();
        assert!(!xml.contains("<?xml"));
        if fail {
            assert!(xml.contains("<Error><Code>ServiceUnavailable</Code>"));
        } else {
            assert!(xml.contains("<CopyObjectResult") && xml.contains("&quot;abc&quot;"));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn copy_deadline_is_an_embedded_error_and_dropping_body_cancels_pending_work() {
    let response = copy_body::response(std::future::pending(), "/bucket/key".into()).unwrap();
    let data = response.into_body().collect().await.unwrap().to_bytes();
    assert!(std::str::from_utf8(&data)
        .unwrap()
        .contains("<Code>ServiceUnavailable</Code>"));
    let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
    let response = copy_body::response(
        async move {
            let _sender = sender;
            std::future::pending::<Result<String, S3ErrorCode>>().await
        },
        "/bucket/key".into(),
    )
    .unwrap();
    drop(response);
    assert!(receiver.await.is_err());
}
