// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[cfg(feature = "s3")]
#[tokio::test]
async fn listener_can_bind_an_ephemeral_port() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert_ne!(listener.local_addr().unwrap().port(), 0);
}

#[cfg(feature = "s3")]
mod s3_dispatcher {
    use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use crowdb_access_s3::auth::{AuthError, RawAuthRequest, RequestAuthenticator};
    use crowdb_access_s3::metrics::{OutcomeClass, S3Health, S3Metrics, S3MetricsSnapshot};
    use crowdb_access_s3::native_buffer::NativeBodyAllocator;
    use crowdb_access_s3::route::{S3Operation, S3Route};
    use crowdb_access_server::s3::{
        install_body_receive_provider, serve, ResponseBody, S3Dispatcher, S3Operations, S3OperationsFuture,
    };
    use http_body_util::{BodyExt, Full};
    use hyper::body::{Bytes, Incoming};
    use hyper::{Request, Response, StatusCode};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct TestAuthenticator {
        reject: AtomicBool,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl RequestAuthenticator for TestAuthenticator {
        async fn authenticate(&self, _request: RawAuthRequest<'_>) -> Result<(), AuthError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if self.reject.load(Ordering::Relaxed) {
                Err(AuthError::Rejected)
            } else {
                Ok(())
            }
        }
    }

    struct TestOperations {
        calls: AtomicUsize,
        body_bytes: AtomicUsize,
        status: AtomicU16,
    }

    impl Default for TestOperations {
        fn default() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                body_bytes: AtomicUsize::new(0),
                status: AtomicU16::new(StatusCode::OK.as_u16()),
            }
        }
    }

    impl S3Operations for TestOperations {
        fn execute(
            self: Arc<Self>,
            _route: S3Route,
            mut request: Request<Incoming>,
            _request_id: String,
            _host_id: String,
        ) -> S3OperationsFuture {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                install_body_receive_provider(&mut request);
                let body = request.into_body().collect().await.unwrap().to_bytes();
                self.body_bytes.fetch_add(body.len(), Ordering::Relaxed);
                Response::builder()
                    .status(self.status.load(Ordering::Relaxed))
                    .body(test_body("ok"))
                    .unwrap()
            })
        }
    }

    #[tokio::test]
    async fn authentication_precedes_route_and_storage_dispatch() {
        let authenticator = Arc::new(TestAuthenticator {
            reject: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
        });
        let operations = Arc::new(TestOperations::default());
        let metrics = Arc::new(S3Metrics::default());
        let health = Arc::new(S3Health::ready(1));
        let body_allocator = Arc::new(NativeBodyAllocator::new(2 * 1024 * 1024, 1024 * 1024).unwrap());
        let dispatcher = Arc::new(
            S3Dispatcher::new(
                authenticator.clone(),
                operations.clone(),
                metrics.clone(),
                "host".into(),
                true,
            )
            .with_body_receive_provider_factory({
                let body_allocator = body_allocator.clone();
                move || Arc::new(body_allocator.object_receiver())
            })
            .with_health(Arc::clone(&health)),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            serve(listener, dispatcher, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
        });

        assert_operational_endpoints(address, &authenticator).await;

        let rejected = request(address, "POST /bucket/key?uploads HTTP/1.1").await;
        assert!(rejected.starts_with("HTTP/1.1 403"));
        assert!(rejected.to_ascii_lowercase().contains("x-amz-request-id:"));
        assert!(rejected.to_ascii_lowercase().contains("x-amz-id-2:"));
        assert_eq!(operations.calls.load(Ordering::Relaxed), 0);

        let rejected_head = request(address, "HEAD /bucket/key HTTP/1.1").await;
        assert!(rejected_head.starts_with("HTTP/1.1 403"));
        assert!(rejected_head.ends_with("\r\n\r\n"));

        authenticator.reject.store(false, Ordering::Relaxed);
        let unsupported = request_with_headers(
            address,
            "PUT /bucket/key HTTP/1.1",
            "x-amz-storage-class: GLACIER\r\nContent-Length: 0\r\n",
        )
        .await;
        assert!(unsupported.starts_with("HTTP/1.1 501"));
        assert_eq!(operations.calls.load(Ordering::Relaxed), 0);

        let accepted = request(address, "GET /bucket/key HTTP/1.1").await;
        assert!(accepted.starts_with("HTTP/1.1 200"));
        assert!(accepted.to_ascii_lowercase().contains("x-amz-request-id:"));
        assert!(accepted.to_ascii_lowercase().contains("x-amz-id-2:"));
        assert_eq!(operations.calls.load(Ordering::Relaxed), 1);
        assert_eq!(authenticator.calls.load(Ordering::Relaxed), 4);
        assert_get_metrics(&metrics.snapshot());

        let put = request_with_headers_and_body(
            address,
            "PUT /bucket/key HTTP/1.1",
            "Content-Length: 4\r\n",
            "body",
        )
        .await;
        assert!(put.starts_with("HTTP/1.1 200"));
        assert_eq!(operations.body_bytes.load(Ordering::Relaxed), 4);
        assert_eq!(body_allocator.allocation_count(), 0);
        assert_eq!(body_allocator.prefix_copy_bytes(), 0);
        assert_eq!(body_allocator.direct_bytes(), 0);

        let direct_put = request_with_split_body(
            address,
            "PUT /bucket/direct HTTP/1.1",
            "Content-Length: 8\r\n",
            &[b"bod", b"ybody"],
        )
        .await;
        assert!(direct_put.starts_with("HTTP/1.1 200"));
        assert_eq!(operations.body_bytes.load(Ordering::Relaxed), 12);
        assert_eq!(body_allocator.allocation_count(), 1);
        assert_eq!(body_allocator.prefix_copy_bytes(), 0);
        assert_eq!(body_allocator.direct_bytes(), 8);
        assert_eq!(body_allocator.retained_bytes(), 0);

        let chunked_put = request_with_split_body(
            address,
            "PUT /bucket/chunked HTTP/1.1",
            "Transfer-Encoding: chunked\r\n",
            &[b"3\r\n", b"abc", b"\r\n5\r\n", b"defgh", b"\r\n0\r\n\r\n"],
        )
        .await;
        assert!(chunked_put.starts_with("HTTP/1.1 200"));
        assert_eq!(operations.body_bytes.load(Ordering::Relaxed), 20);
        assert_eq!(body_allocator.direct_bytes(), 16);
        assert_eq!(body_allocator.retained_bytes(), 0);
        assert_terminal_outcomes(address, &operations, &metrics).await;

        metrics.enqueue_cleanup(2);
        let not_ready = request(address, "GET /_crowdb/health/ready HTTP/1.1").await;
        assert!(not_ready.starts_with("HTTP/1.1 503"));
        assert!(not_ready.contains(r#""cleanup":"unavailable""#));

        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    async fn assert_operational_endpoints(address: std::net::SocketAddr, authenticator: &TestAuthenticator) {
        let live = request(address, "GET /_crowdb/health/live HTTP/1.1").await;
        assert!(live.starts_with("HTTP/1.1 200"));
        assert!(live.contains(r#"{"live":true}"#));
        let ready = request(address, "GET /_crowdb/health/ready HTTP/1.1").await;
        assert!(ready.starts_with("HTTP/1.1 200"));
        assert!(ready.contains(r#""ready":true"#));
        let exported = request(address, "GET /_crowdb/metrics HTTP/1.1").await;
        assert!(exported.starts_with("HTTP/1.1 200"));
        assert!(exported.contains(r#"crowdb_s3_requests_total{operation="put_object",outcome="success"} 0"#));
        assert!(!exported.contains("bucket/key"));
        assert_eq!(authenticator.calls.load(Ordering::Relaxed), 0);
    }

    fn assert_get_metrics(snapshot: &S3MetricsSnapshot) {
        assert_eq!(snapshot.trusted_auth_bypass, 2);
        assert_eq!(snapshot.in_flight, 0);
        assert_eq!(snapshot.max_in_flight, 1);
        assert_eq!(
            snapshot.predispatch_requests[OutcomeClass::ClientError as usize],
            3
        );
        assert_eq!(
            snapshot.requests[S3Operation::GetObject as usize][OutcomeClass::Success as usize],
            1
        );
        assert!(
            snapshot.time_to_first_byte_ns[S3Operation::GetObject as usize][OutcomeClass::Success as usize]
                > 0
        );
        assert!(
            snapshot.authentication_latency_ns[S3Operation::GetObject as usize]
                [OutcomeClass::Success as usize]
                > 0
        );
        assert!(
            snapshot.operation_latency_ns[S3Operation::GetObject as usize][OutcomeClass::Success as usize]
                > 0
        );
    }

    async fn assert_terminal_outcomes(
        address: std::net::SocketAddr,
        operations: &TestOperations,
        metrics: &S3Metrics,
    ) {
        for status in [429, 504, 503, 500] {
            operations.status.store(status, Ordering::Relaxed);
            let response = request(address, "GET /bucket/key HTTP/1.1").await;
            assert!(response.starts_with(&format!("HTTP/1.1 {status}")));
        }
        let snapshot = metrics.snapshot();
        let operation = S3Operation::GetObject as usize;
        for outcome in [
            OutcomeClass::Throttled,
            OutcomeClass::Timeout,
            OutcomeClass::Unavailable,
            OutcomeClass::Internal,
        ] {
            assert_eq!(snapshot.requests[operation][outcome as usize], 1);
            assert!(snapshot.request_latency_ns[operation][outcome as usize] > 0);
            assert!(snapshot.time_to_first_byte_ns[operation][outcome as usize] > 0);
        }
    }

    async fn request(address: std::net::SocketAddr, start_line: &str) -> String {
        request_with_headers(address, start_line, "").await
    }

    async fn request_with_headers(address: std::net::SocketAddr, start_line: &str, headers: &str) -> String {
        request_with_headers_and_body(address, start_line, headers, "").await
    }

    async fn request_with_headers_and_body(
        address: std::net::SocketAddr,
        start_line: &str,
        headers: &str,
        body: &str,
    ) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("{start_line}\r\nHost: localhost\r\n{headers}Connection: close\r\n\r\n{body}")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    async fn request_with_split_body(
        address: std::net::SocketAddr,
        start_line: &str,
        headers: &str,
        body_parts: &[&[u8]],
    ) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("{start_line}\r\nHost: localhost\r\n{headers}Connection: close\r\n\r\n").as_bytes(),
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        for part in body_parts {
            stream.write_all(part).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    fn test_body(value: &'static str) -> ResponseBody {
        Full::new(Bytes::from_static(value.as_bytes()))
            .map_err(|never| match never {})
            .boxed()
    }
}
