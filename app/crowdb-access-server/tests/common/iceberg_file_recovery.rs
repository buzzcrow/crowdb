use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use crowdb_access_iceberg::{
    catalog::ClearBounds,
    file::{
        FileKind, FileRepository, MultipartAdmission, MultipartPhase, MultipartRepository, MultipartSession,
        TableLocation,
    },
};
use reqwest::Method;
use serde_json::Value;

use super::{
    child::TestCommitChild, common::TestIcebergStack, path, process::TestIcebergProcess, setup_with_bounds,
    TestFileClient,
};

pub async fn run() {
    let (mut stack, process, mut client, table) = setup_with_bounds(ClearBounds {
        request_ms: 300_000,
        delegated_access_ms: 900_000,
        ..ClearBounds::default()
    })
    .await;
    let directory = stack
        .cluster
        .runtime_mut()
        .service_dir("iceberg", "file-faults")
        .unwrap();
    drop(process);
    for multipart in [false, true] {
        let baseline = prepare(&stack, &mut client, table, multipart, "baseline").await;
        let child = TestCommitChild::start(
            &stack.cluster.mgmt_endpoints,
            directory.join(format!("baseline-{multipart}.json")),
            usize::MAX,
            false,
        )
        .await;
        client.address = child.address;
        baseline.execute(&client).await;
        let marker: Value = serde_json::from_slice(&std::fs::read(&child.marker).unwrap()).unwrap();
        let count = usize::try_from(marker["index"].as_u64().unwrap()).unwrap();
        drop(child);
        let mut labels = BTreeSet::new();
        for offset in 1..=count {
            for after in [false, true] {
                let label = interrupt(&stack, &directory, &mut client, table, multipart, offset, after).await;
                labels.insert(label);
            }
        }
        assert!(labels.contains("file-record"));
        assert!(labels.contains("file-mapping"));
        if multipart {
            assert!(labels.contains("multipart-Completing"));
            assert!(labels.contains("multipart-Publishing"));
            assert!(labels.contains("multipart-Published"));
        } else {
            assert!(labels.contains("file-block"));
        }
    }
}

async fn interrupt(
    stack: &TestIcebergStack,
    directory: &Path,
    client: &mut TestFileClient,
    table: TableLocation,
    multipart: bool,
    offset: usize,
    after: bool,
) -> String {
    let case = prepare(stack, client, table, multipart, &format!("{offset}-{after}")).await;
    let mut child = TestCommitChild::start(
        &stack.cluster.mgmt_endpoints,
        directory.join(format!("{multipart}-{offset}-{after}.json")),
        offset,
        after,
    )
    .await;
    client.address = child.address;
    let request = case.request(client);
    let mut request = tokio::spawn(async move { request.send().await?.bytes().await });
    let boundary = tokio::select! {
        boundary = child.paused() => boundary,
        result = &mut request => panic!("file boundary {multipart}/{offset}/{after} returned early: {result:?}"),
    };
    let label = boundary["label"].as_str().unwrap().to_owned();
    println!("kill multipart={multipart} boundary={offset} after={after}: {label}");
    drop(child);
    assert!(
        request.await.unwrap().is_err(),
        "interruption must lose the response"
    );
    let recovery = TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    client.address = recovery.address;
    let first = case.execute(client).await;
    let files = FileRepository::new(stack.store().await);
    let context = client.credentials.grant().context;
    let record = files
        .load(context, &table.file(&case.key).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.kind, FileKind::Unbound);
    assert_eq!(case.execute(client).await, first);
    assert_eq!(
        files.load(context, &record.location).await.unwrap().unwrap(),
        record
    );
    let get = client.send(Method::GET, &case.path, "", b"", false).await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), case.bytes);
    let mut changed = case.bytes.clone();
    changed[4] ^= 1;
    let conflict = client.send(Method::PUT, &case.path, "", &changed, false).await;
    assert_eq!(conflict.status(), 409);
    if let Some(upload) = &case.upload {
        let session = MultipartRepository::new(stack.store().await)
            .load(context, upload.parse().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.phase, MultipartPhase::Published);
        assert_eq!(session.published, Some(record.file));
        released_by_recovery(stack, &session).await;
    }
    label
}

async fn released_by_recovery(stack: &TestIcebergStack, published: &MultipartSession) {
    let store = stack.store().await;
    let sessions = MultipartRepository::new(store.clone());
    let admission = MultipartAdmission::new(store);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = sessions
                .load(published.context, published.upload)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.phase, MultipartPhase::Published);
            assert_eq!(current.published, published.published);
            let policy = admission.load(published.context).await.unwrap().unwrap();
            assert!(policy.sessions <= 1);
            assert!(policy.reserved_bytes <= published.limits.max_staged_bytes);
            if current.credit.unwrap().released && policy.pending.is_none() {
                assert_eq!((policy.sessions, policy.reserved_bytes), (0, 0));
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("background recovery must settle terminal credits without another Complete request");
}

struct TestFileCase {
    key: String,
    path: String,
    bytes: Vec<u8>,
    upload: Option<String>,
    complete: String,
}

impl TestFileCase {
    fn request(&self, client: &TestFileClient) -> reqwest::RequestBuilder {
        match &self.upload {
            Some(upload) => client.request(
                Method::POST,
                &self.path,
                &format!("uploadId={upload}"),
                self.complete.as_bytes(),
                false,
                None,
            ),
            None => client.request(Method::PUT, &self.path, "", &self.bytes, false, None),
        }
    }

    async fn execute(&self, client: &TestFileClient) -> String {
        let response = self.request(client).send().await.unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, 200, "{body}");
        if self.upload.is_some() {
            assert!(body.ends_with("</CompleteMultipartUploadResult>"), "{body}");
            assert!(!body.contains("<Error>"), "{body}");
        }
        body.trim().to_owned()
    }
}

async fn prepare(
    stack: &TestIcebergStack,
    client: &mut TestFileClient,
    table: TableLocation,
    multipart: bool,
    name: &str,
) -> TestFileCase {
    let key = format!("objects/{multipart}-{name}.parquet");
    let mut bytes = b"PAR1".to_vec();
    bytes.resize(65540, b'x');
    bytes.extend_from_slice(b"foot\x04\0\0\0PAR1");
    let mut case = TestFileCase {
        path: path(table, &key),
        key,
        bytes,
        upload: None,
        complete: String::new(),
    };
    if multipart {
        let setup = TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
        client.address = setup.address;
        let create = client
            .send(Method::POST, &case.path, "uploads=", b"", false)
            .await;
        let status = create.status();
        let body = create.text().await.unwrap();
        assert_eq!(status, 200, "{body}");
        let upload = body
            .split_once("<UploadId>")
            .unwrap()
            .1
            .split_once("</UploadId>")
            .unwrap()
            .0
            .to_owned();
        let part = client
            .send(
                Method::PUT,
                &case.path,
                &format!("partNumber=1&uploadId={upload}"),
                &case.bytes,
                false,
            )
            .await;
        assert_eq!(part.status(), 200, "{}", part.text().await.unwrap());
        let etag = part.headers()["etag"].to_str().unwrap();
        case.complete = format!("<CompleteMultipartUpload><Part><ETag>{etag}</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>");
        case.upload = Some(upload);
    }
    case
}
