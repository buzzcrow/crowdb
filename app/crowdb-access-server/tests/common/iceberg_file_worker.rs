use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    FileIdentity, FileTree, MultipartAdmission, MultipartAdmissionLimits, MultipartLimits, MultipartPart,
    MultipartPhase, MultipartRepository, MultipartSession, TableLocation,
};
use crowdb_access_iceberg::key::{FileId, OperationId};
use sha2::{Digest, Sha256};

use crate::common::TestIcebergStack;

pub async fn verify(stack: &TestIcebergStack, context: CatalogContext, table: TableLocation) {
    let store = stack.store().await;
    let initial = MultipartSession {
        context,
        upload: OperationId::random(),
        owner: FileIdentity {
            table,
            file: FileId::random(),
        },
        location: table.file("multipart/expired.bin").unwrap(),
        principal: [1; 32],
        revision: 1,
        created_ms: 1,
        expires_ms: 1001,
        limits: MultipartLimits {
            max_parts: 1,
            max_part_bytes: 100,
            max_file_bytes: 100,
            max_staged_bytes: 100,
            ttl_ms: 1000,
        },
        phase: MultipartPhase::Open,
        part_count: 0,
        staged_bytes: 0,
        completion: None,
        published: None,
        pending: None,
        credit: None,
    };
    let admission = MultipartAdmission::new(store.clone());
    let policy = admission
        .initialize(
            context,
            MultipartAdmissionLimits {
                max_sessions: 2,
                max_reserved_bytes: 200,
            },
        )
        .await
        .unwrap();
    assert!(admission.reserve(&policy, &initial, 1).await.unwrap());
    let repository = MultipartRepository::new(store);
    let admitted = repository.load(context, initial.upload).await.unwrap().unwrap();
    let part = MultipartPart {
        upload: initial.upload,
        number: 1,
        revision: 1,
        owner: FileIdentity {
            table,
            file: FileId::random(),
        },
        tree: FileTree {
            root: None,
            length: 0,
            digest: Sha256::digest([]).into(),
        },
    };
    assert!(repository.reserve_part(&admitted, &part, 2).await.unwrap());
    let mut worker = TestWorker::start(&stack.cluster.mgmt_endpoints);
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert!(worker.0.try_wait().unwrap().is_none(), "Iceberg worker exited");
            let current = repository.load(context, initial.upload).await.unwrap().unwrap();
            if current.phase == MultipartPhase::Aborted
                && current.credit.is_some_and(|credit| credit.released)
            {
                assert!(current.pending.is_none());
                assert_eq!((current.part_count, current.staged_bytes), (1, 0));
                assert_eq!(repository.part(&current, 1).await.unwrap().as_ref(), Some(&part));
                let policy = admission.load(context).await.unwrap().unwrap();
                if policy.pending.is_some() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                assert_eq!((policy.sessions, policy.reserved_bytes), (0, 0));
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

struct TestWorker(Child);

impl TestWorker {
    fn start(seeds: &[String]) -> Self {
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        Self(
            Command::new(env!("CARGO_BIN_EXE_crowdb-iceberg"))
                .env("CROWDB_MANAGEMENT_SEEDS", seeds.join(","))
                .env("CROWDB_ICEBERG_LISTEN", address.to_string())
                .env("CROWDB_ICEBERG_READ_TOKEN", "r".repeat(32))
                .env("CROWDB_ICEBERG_WRITE_TOKEN", "w".repeat(32))
                .env("CROWDB_ICEBERG_MANAGE_TOKEN", "m".repeat(32))
                .env("CROWDB_ICEBERG_CLEAR_TOKEN", "c".repeat(32))
                .arg("serve")
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        )
    }
}

impl Drop for TestWorker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
