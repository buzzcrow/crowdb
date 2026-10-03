use super::{path, setup};
use reqwest::Method;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_list_objects_v2_serves_selected_files_and_rejects_foreign_scope() {
    let (_stack, _process, client, table) = setup().await;
    for key in ["data/a.json", "data/nested/b.json", "metadata/c.json"] {
        let response = client.send(Method::PUT, &path(table, key), "", b"{}", true).await;
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    }
    let bucket = format!("/{}", table.bucket());
    let query = format!("list-type=2&prefix={}data/&max-keys=1", table.object_prefix());
    let response = client.send(Method::GET, &bucket, &query, b"", false).await;
    assert_eq!(response.status(), 200);
    let xml = response.text().await.unwrap();
    assert!(
        xml.contains(&format!("<Key>{}data/a.json</Key>", table.object_prefix())),
        "{xml}"
    );
    let token = xml
        .split("<NextContinuationToken>")
        .nth(1)
        .unwrap()
        .split("</NextContinuationToken>")
        .next()
        .unwrap();
    let response = client
        .send(
            Method::GET,
            &bucket,
            &format!("{query}&continuation-token={token}"),
            b"",
            false,
        )
        .await;
    assert_eq!(response.status(), 200);
    let xml = response.text().await.unwrap();
    assert!(xml.contains("data/nested/b.json</Key>"), "{xml}");
    assert!(!xml.contains("data/a.json</Key>"));
    let query = format!(
        "list-type=2&prefix={}data/&delimiter=%2F&encoding-type=url",
        table.object_prefix()
    );
    let response = client.send(Method::GET, &bucket, &query, b"", false).await;
    assert_eq!(response.status(), 200);
    let xml = response.text().await.unwrap();
    assert!(xml.contains("<CommonPrefixes>"), "{xml}");
    assert!(xml.contains("data%2Fnested%2F"), "{xml}");
    let foreign = crowdb_access_iceberg::file::TableLocation {
        catalog: table.catalog,
        table: crowdb_access_iceberg::key::TableId::random(),
    };
    let response = client
        .send(
            Method::GET,
            &bucket,
            &format!("list-type=2&prefix={}", foreign.object_prefix()),
            b"",
            false,
        )
        .await;
    assert_eq!(response.status(), 403);
    let response = client
        .send(Method::GET, &bucket, "list-type=2&prefix=", b"", false)
        .await;
    assert_eq!(response.status(), 400);
    let response = client
        .client
        .get(format!(
            "http://{}{bucket}?list-type=2&prefix={}",
            client.address,
            table.object_prefix()
        ))
        .header("x-amz-security-token", "general-s3-token")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the pinned PyIceberg/PyArrow environment and native storage"]
async fn official_pyarrow_native_fileio_creates_exact_files_and_lists_table_prefixes() {
    use std::io::Write as _;
    use std::process::{Command, Stdio};
    let (_stack, _process, client, table) = setup().await;
    let config = serde_json::json!({ "endpoint": format!("http://{}", client.address),
        "bucket": table.bucket(), "prefix": table.object_prefix(),
        "access_key": client.credentials.access_key_id(), "secret_key": client.credentials.secret_access_key(),
        "session_token": client.credentials.session_token() });
    let status = tokio::task::spawn_blocking(move || {
        let python =
            std::env::var_os("CROWDB_ICEBERG_E2E_PYTHON").expect("run the native listing client task");
        let mut child = Command::new("timeout")
            .arg("60")
            .arg(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_listing_native_client.py"
            ))
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(config.to_string().as_bytes())
            .unwrap();
        child.wait().unwrap()
    })
    .await
    .unwrap();
    assert!(
        status.success(),
        "official PyArrow/PyIceberg native listing workflow failed"
    );
}
