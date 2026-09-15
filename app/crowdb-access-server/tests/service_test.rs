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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use crowdb_access_s3::auth::{AuthError, RawAuthRequest, RequestAuthenticator};
    use crowdb_access_s3::metrics::{OutcomeClass, S3Metrics};
    use crowdb_access_s3::native_buffer::NativeBodyAllocator;
    use crowdb_access_s3::route::{S3Operation, S3Route};
    use crowdb_access_server::s3::{serve, ResponseBody, S3Dispatcher, S3Operations, S3OperationsFuture};
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

    #[derive(Default)]
    struct TestOperations {
        calls: AtomicUsize,
        body_bytes: AtomicUsize,
    }

    impl S3Operations for TestOperations {
        fn execute(
            self: Arc<Self>,
            _route: S3Route,
            request: Request<Incoming>,
            _request_id: String,
            _host_id: String,
        ) -> S3OperationsFuture {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                let body = request.into_body().collect().await.unwrap().to_bytes();
                self.body_bytes.fetch_add(body.len(), Ordering::Relaxed);
                Response::builder()
                    .status(StatusCode::OK)
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
        let body_allocator = Arc::new(NativeBodyAllocator::new(1024, 128).unwrap());
        let dispatcher = Arc::new(
            S3Dispatcher::new(
                authenticator.clone(),
                operations.clone(),
                metrics.clone(),
                "host".into(),
                true,
            )
            .with_body_allocator(body_allocator.clone()),
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
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.trusted_auth_bypass, 2);
        assert_eq!(snapshot.in_flight, 0);
        assert_eq!(
            snapshot.requests[S3Operation::GetObject as usize][OutcomeClass::Success as usize],
            1
        );

        let put = request_with_headers_and_body(
            address,
            "PUT /bucket/key HTTP/1.1",
            "Content-Length: 4\r\n",
            "body",
        )
        .await;
        assert!(put.starts_with("HTTP/1.1 200"));
        assert_eq!(operations.body_bytes.load(Ordering::Relaxed), 4);
        assert_eq!(body_allocator.allocation_count(), 1);
        assert_eq!(body_allocator.retained_bytes(), 0);

        let _ = shutdown_tx.send(());
        server.await.unwrap();
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

    fn test_body(value: &'static str) -> ResponseBody {
        Full::new(Bytes::from_static(value.as_bytes()))
            .map_err(|never| match never {})
            .boxed()
    }
}
