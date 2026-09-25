#![cfg(feature = "iceberg-e2e")]

#[path = "common/iceberg_file_blocks.rs"]
#[allow(dead_code)]
mod blocks;
#[path = "common/iceberg_store.rs"]
mod common;
#[path = "common/iceberg_table_http.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/iceberg_stack.rs"]
#[allow(dead_code)]
mod native_stack;
#[path = "common/iceberg_process.rs"]
#[allow(dead_code)]
mod process;

use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    key::OperationId,
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
    wire::BearerAuthenticator,
};
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use fixture::TestTableHttp;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
#[ignore = "builds the pinned official Apache Iceberg Rust client"]
async fn official_rust_client_namespace_and_table_lifecycle() {
    run_official_client(false).await;
}

#[tokio::test]
#[ignore = "builds the pinned official Apache Iceberg Rust client"]
async fn official_rust_client_observes_lost_create_reply_on_another_listener() {
    run_official_client(true).await;
}

async fn run_official_client(response_loss: bool) {
    let fixture = TestTableHttp::writable().await;
    let backend_origin = fixture.endpoint();
    let service = IcebergHttpService::new(
        Arc::new(CatalogRepository::new(fixture.store.clone(), ClearBounds::default()).unwrap()),
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap(),
        Duration::from_secs(2),
    )
    .with_namespaces(fixture.store.clone())
    .unwrap()
    .with_tables(fixture.store.clone(), Arc::new(blocks::TestFileBlocks::default()))
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_origin = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        serve(listener, Arc::new(service), async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    let (origin, proxy) = if response_loss {
        let (origin, proxy, observed) = start_loss_proxy(backend_origin).await;
        (origin, Some((proxy, observed)))
    } else {
        (backend_origin, None)
    };
    let status = tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new("timeout");
        command
            .arg("600")
            .arg("pixi")
            .args(["run", "cargo", "run", "--locked", "--manifest-path"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_rust/Cargo.toml"
            ))
            .env("CROWDB_ICEBERG_RUST_ORIGIN", origin)
            .env("CROWDB_ICEBERG_RUST_SECOND_ORIGIN", second_origin)
            .env("CROWDB_ICEBERG_RUST_TOKEN", "w".repeat(32))
            .env(
                "CROWDB_ICEBERG_RUST_NAMESPACE",
                if response_loss {
                    "rust_sdk_loss"
                } else {
                    "rust_sdk"
                },
            );
        if response_loss {
            command.env("CROWDB_ICEBERG_RUST_RESPONSE_LOSS", "1");
        }
        command.status().unwrap()
    })
    .await
    .unwrap();
    if let Some((proxy, observed)) = proxy {
        proxy.abort();
        assert!(
            observed.load(Ordering::SeqCst),
            "proxy did not drop the create response"
        );
    }
    stop.send(()).unwrap();
    server.await.unwrap();
    assert!(status.success(), "official Rust REST client failed");
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned Apache Iceberg Rust client and native storage"]
async fn official_rust_client_lost_reply_survives_native_storage_restart() {
    let mut stack = native_stack::TestIcebergStack::start().await;
    let repository = CatalogRepository::new(
        stack.store().await,
        ClearBounds {
            request_ms: 300_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )
    .unwrap();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: native_stack::now_ms(),
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "rust-native".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            native_stack::now_ms(),
        )
        .await
        .unwrap();
    native_stack::activate(&repository).await;
    let first = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let second = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let (origin, proxy, observed) = start_loss_proxy(format!("http://{}", first.address)).await;
    assert!(run_rust_fixture(&origin, &format!("http://{}", second.address), true, false, true).await);
    proxy.abort();
    assert!(observed.load(Ordering::SeqCst));
    drop(first);
    drop(second);
    stack.chunk_kv.restart().await;
    let first = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let second = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    assert!(
        run_rust_fixture(
            &format!("http://{}", first.address),
            &format!("http://{}", second.address),
            false,
            true,
            false
        )
        .await
    );
}

async fn run_rust_fixture(
    origin: &str,
    second_origin: &str,
    response_loss: bool,
    verify_existing: bool,
    keep_table: bool,
) -> bool {
    let origin = origin.to_owned();
    let second_origin = second_origin.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new("timeout");
        command
            .arg("600")
            .arg("pixi")
            .args(["run", "cargo", "run", "--locked", "--manifest-path"])
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_rust/Cargo.toml"
            ))
            .env("CROWDB_ICEBERG_RUST_ORIGIN", origin)
            .env("CROWDB_ICEBERG_RUST_SECOND_ORIGIN", second_origin)
            .env("CROWDB_ICEBERG_RUST_TOKEN", "w".repeat(32))
            .env("CROWDB_ICEBERG_RUST_NAMESPACE", "rust_sdk_loss");
        if response_loss {
            command.env("CROWDB_ICEBERG_RUST_RESPONSE_LOSS", "1");
        }
        if verify_existing {
            command.env("CROWDB_ICEBERG_RUST_VERIFY_EXISTING", "1");
        }
        if keep_table {
            command.env("CROWDB_ICEBERG_RUST_KEEP_TABLE", "1");
        }
        command.status().unwrap().success()
    })
    .await
    .unwrap()
}

async fn start_loss_proxy(backend_origin: String) -> (String, tokio::task::JoinHandle<()>, Arc<AtomicBool>) {
    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", proxy_listener.local_addr().unwrap());
    let lost = Arc::new(AtomicBool::new(false));
    let observed = lost.clone();
    let proxy = tokio::spawn(async move {
        loop {
            let (client, _) = proxy_listener.accept().await.unwrap();
            let backend = backend_origin.clone();
            let lost = lost.clone();
            tokio::spawn(async move {
                forward_or_lose(client, &backend, lost).await.unwrap();
            });
        }
    });
    (origin, proxy, observed)
}

async fn forward_or_lose(
    mut client: tokio::net::TcpStream,
    backend_origin: &str,
    lost: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let mut header = Vec::new();
    while !header.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut buffer = [0_u8; 4096];
        let count = client.read(&mut buffer).await?;
        if count == 0 || header.len() + count > 16 * 1024 {
            return Err(std::io::Error::other("invalid proxy request header"));
        }
        header.extend_from_slice(&buffer[..count]);
    }
    let backend = backend_origin.trim_start_matches("http://");
    let mut upstream = tokio::net::TcpStream::connect(backend).await?;
    upstream.write_all(&header).await?;
    let create = header.starts_with(b"POST /v1/namespaces/rust_sdk_loss/tables ");
    if create && !lost.swap(true, Ordering::SeqCst) {
        let (mut client_read, client_write) = client.into_split();
        let (mut upstream_read, mut upstream_write) = upstream.into_split();
        let forwarding = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut client_read, &mut upstream_write).await;
        });
        let mut response = [0_u8; 4096];
        let count = upstream_read.read(&mut response).await?;
        forwarding.abort();
        drop(client_write);
        if count == 0 || !response.starts_with(b"HTTP/1.1 200") {
            return Err(std::io::Error::other(
                "upstream did not publish the create response",
            ));
        }
        return Ok(());
    }
    tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}
