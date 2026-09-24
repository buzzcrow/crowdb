use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, RoutedCatalogStore},
    file::NativeFileBlocks,
    wire::BearerAuthenticator,
};
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};

use super::fault::{TestBoundary, TestCommitBlocks, TestCommitStore};

pub async fn run() {
    let Some(config) = std::env::var_os("CROWDB_TEST_COMMIT_CHILD") else {
        return;
    };
    let config: serde_json::Value = serde_json::from_str(config.to_str().unwrap()).unwrap();
    let seeds: Vec<String> = serde_json::from_value(config["seeds"].clone()).unwrap();
    let control = Arc::new(CrowdbKvClient::new(KvConfig::new(seeds.clone())));
    let client_config = ClientConfig::default();
    let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(control.clone()));
    let transport = Arc::new(ChunkKvRpcTransport::new(
        client_config.max_owner_connections,
        1,
        2,
    ));
    let client = Arc::new(ChunkKvClient::new(client_config, source, transport).unwrap());
    client.refresh_catalog().await.unwrap();
    let chunks = ChunkIoClient::connect_with_kv(
        ChunkIoClientConfig {
            management_seeds: seeds,
            diskio_connections_per_endpoint: 2,
            diskio_rpc_workers: 2,
            small_write: SmallWritePolicy::default(),
        },
        control,
    )
    .await
    .unwrap();
    let boundary = Arc::new(TestBoundary::new(
        usize::try_from(config["target"].as_u64().unwrap()).unwrap(),
        config["after"].as_bool().unwrap(),
        config["marker"].as_str().unwrap().into(),
    ));
    let store = Arc::new(TestCommitStore {
        inner: Arc::new(RoutedCatalogStore::new(client)),
        boundary: boundary.clone(),
    });
    let blocks = Arc::new(TestCommitBlocks {
        inner: Arc::new(NativeFileBlocks::new(chunks)),
        boundary,
    });
    let repository = Arc::new(
        CatalogRepository::new(
            store.clone(),
            ClearBounds {
                request_ms: 300_000,
                delegated_access_ms: 900_000,
                ..ClearBounds::default()
            },
        )
        .unwrap(),
    );
    let authentication =
        BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32)).unwrap();
    let address = config["address"].as_str().unwrap();
    let service = IcebergHttpService::new(repository, authentication, Duration::from_secs(300))
        .with_namespaces(store.clone())
        .unwrap()
        .with_fileio(store.clone(), blocks.clone(), "us-east-1".into())
        .unwrap()
        .with_tables(store.clone(), blocks)
        .unwrap()
        .with_table_credentials(store, format!("http://{address}"))
        .unwrap();
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    serve(listener, Arc::new(service), std::future::pending())
        .await
        .unwrap();
}

pub struct TestCommitChild {
    child: Child,
    pub address: SocketAddr,
    pub marker: PathBuf,
}

impl TestCommitChild {
    pub async fn start(seeds: &[String], marker: PathBuf, target: usize, after: bool) -> Self {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        let configuration = serde_json::json!({"seeds":seeds,"address":address.to_string(),"marker":marker,"target":target,"after":after});
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_fault_listener_child",
                "--ignored",
                "--nocapture",
            ])
            .env("CROWDB_TEST_COMMIT_CHILD", configuration.to_string())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut process = Self {
            child,
            address,
            marker,
        };
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                assert!(
                    process.child.try_wait().unwrap().is_none(),
                    "fault listener exited"
                );
                if tokio::net::TcpStream::connect(address).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        process
    }

    pub async fn paused(&mut self) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                assert!(
                    self.child.try_wait().unwrap().is_none(),
                    "fault listener exited before boundary"
                );
                if let Ok(bytes) = std::fs::read(&self.marker) {
                    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                        if value["paused"] == true {
                            return value;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
}

impl Drop for TestCommitChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
