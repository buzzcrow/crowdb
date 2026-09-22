use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

pub struct TestIcebergProcess {
    child: Child,
    pub address: SocketAddr,
}

impl TestIcebergProcess {
    pub async fn start(seeds: &[String]) -> Self {
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        let child = command(seeds)
            .env("CROWDB_ICEBERG_LISTEN", address.to_string())
            .arg("serve")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut process = Self { child, address };
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                assert!(
                    process.child.try_wait().unwrap().is_none(),
                    "Iceberg listener exited"
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

    pub fn check_official_client(&self) {
        let python = std::env::var_os("CROWDB_ICEBERG_E2E_PYTHON")
            .expect("run pixi run -e iceberg-e2e test-pyiceberg-e2e");
        let status = Command::new(python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/common/iceberg_client.py"
            ))
            .arg(format!("http://{}", self.address))
            .status()
            .unwrap();
        assert!(status.success(), "official Iceberg client contract failed");
    }
}

pub fn command(seeds: &[String]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_crowdb-iceberg"));
    command
        .env("CROWDB_MANAGEMENT_SEEDS", seeds.join(","))
        .env("CROWDB_ICEBERG_READ_TOKEN", "r".repeat(32))
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
