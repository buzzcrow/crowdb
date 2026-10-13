// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_web::{router, AppState};

#[tokio::test]
async fn swagger_routes_are_removed() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let app = router(AppState::default());
    for path in ["/api/swagger/", "/api/swagger.json", "/api/nodes/1/openapi.json"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(
            !body.contains("\"swagger\""),
            "{path} must not return Swagger JSON"
        );
        if path.ends_with("openapi.json") {
            assert!(
                !body.contains("\"openapi\""),
                "{path} must not return OpenAPI JSON"
            );
        }
    }
}
