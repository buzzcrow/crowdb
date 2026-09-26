use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use crowdb_web::{router, AppState};
use tower::ServiceExt;

#[tokio::test]
async fn managed_mode_does_not_expose_local_topology_or_mutations() {
    let app = router(AppState::default().with_managed_ui(PathBuf::from("/tmp/crowdb-ui")));
    for (method, path) in [
        (Method::GET, "/api/racks"),
        (Method::GET, "/api/stores"),
        (Method::POST, "/api/racks"),
        (Method::DELETE, "/api/nodes/1"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/authority")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn old_mixed_config_is_rejected_before_startup() {
    let path = std::env::temp_dir().join(format!(
        "crowdb-web-old-config-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, "[[rack]]\nid = 1\n").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-web"))
        .args(["--config", path.to_str().unwrap()])
        .output()
        .unwrap();
    std::fs::remove_file(path).unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("version") || error.contains("unknown field"),
        "{error}"
    );
}
