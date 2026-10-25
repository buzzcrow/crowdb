// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{body::Body, http::Request, response::Response, routing::any, Router};
use std::time::Duration;

pub struct TestListener {
    pub url: String,
    task: tokio::task::JoinHandle<()>,
}

impl TestListener {
    pub async fn serve(status: u16, body: String, delay: Duration, authenticated: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/probe", listener.local_addr().unwrap());
        let app = Router::new().route(
            "/probe",
            any(move |request: Request<Body>| {
                let body = body.clone();
                async move {
                    assert_eq!(request.method(), "GET", "Readiness must not mutate data");
                    assert_eq!(
                        request
                            .headers()
                            .get("authorization")
                            .map(|v| v.to_str().unwrap()),
                        authenticated.then_some("Bearer fixture-token")
                    );
                    tokio::time::sleep(delay).await;
                    Response::builder()
                        .status(status)
                        .header("location", "/must-not-follow")
                        .body(Body::from(body))
                        .unwrap()
                }
            }),
        );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, task }
    }
}

impl Drop for TestListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}
