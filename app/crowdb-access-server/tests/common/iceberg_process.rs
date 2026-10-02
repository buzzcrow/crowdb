use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crowdb_protocol::port::namespace::RuntimeNamespace;

pub struct TestIcebergProcess {
    child: Child,
    pub address: SocketAddr,
    _ports: RuntimeNamespace,
}

impl TestIcebergProcess {
    pub async fn start(seeds: &[String]) -> Self {
        Self::start_with_gc(seeds, false).await
    }

    pub async fn start_with_gc(seeds: &[String], gc_enabled: bool) -> Self {
        Self::start_with_gc_settings(seeds, gc_enabled, &[]).await
    }

    pub async fn start_with_gc_settings(
        seeds: &[String],
        gc_enabled: bool,
        settings: &[(&str, &str)],
    ) -> Self {
        let mut ports = RuntimeNamespace::ephemeral("iceberg-listener").unwrap();
        let port = ports
            .assign_port(crowdb_protocol::ServicePort::AccessServerIcebergHttp, 0)
            .unwrap();
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let mut launch = command(seeds);
        launch
            .env("CROWDB_ICEBERG_LISTEN", address.to_string())
            .env("CROWDB_ICEBERG_GC_ENABLED", if gc_enabled { "1" } else { "0" })
            .env("CROWDB_ICEBERG_GC_INTERVAL_MS", "100");
        for (name, value) in settings {
            launch.env(name, value);
        }
        let child = launch
            .arg("serve")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut process = Self {
            child,
            address,
            _ports: ports,
        };
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = process.child.try_wait().unwrap() {
                    panic!("Iceberg listener exited before readiness: {status}");
                }
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

    pub fn check_official_client(&self) {
        self.check_client(false);
    }

    pub fn check_official_reads(&self) {
        self.check_client(true);
    }

    fn check_client(&self, read_only: bool) {
        let python = std::env::var_os("CROWDB_ICEBERG_E2E_PYTHON")
            .expect("run pixi run -e iceberg-e2e test-pyiceberg-e2e");
        let mut command = Command::new(python);
        command
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_client.py"
            ))
            .arg(format!("http://{}", self.address));
        if read_only {
            command.arg("--read-only");
        }
        let status = command.status().unwrap();
        assert!(status.success(), "official Iceberg client contract failed");
    }
}

pub fn command(seeds: &[String]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_crowdb-access-server"));
    command
        .arg("iceberg")
        .args([
            "--config",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_single_node.toml"
            ),
        ])
        .env("CROWDB_MANAGEMENT_SEEDS", seeds.join(","))
        .env("CROWDB_ICEBERG_READ_TOKEN", "r".repeat(32))
        .env("CROWDB_ICEBERG_WRITE_TOKEN", "w".repeat(32))
        .env("CROWDB_ICEBERG_MANAGE_TOKEN", "m".repeat(32))
        .env("CROWDB_ICEBERG_CLEAR_TOKEN", "c".repeat(32));
    command
}

impl Drop for TestIcebergProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
