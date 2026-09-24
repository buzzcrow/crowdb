use std::{sync::Arc, time::Duration};

use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogRepository, ClearBounds, ManagementPrivilege, StoredValue},
    file::{ContentFormat, FileContent, FileKind, FileRecord, FileRepository, TableLocation},
    key::{FileId, IcebergKey, NamespaceId, OperationId, TableId},
    namespace::{
        NamespaceCreateRequest, NamespaceCreator, NamespaceIdentifier, NamespaceProperties,
        NamespaceRepository,
    },
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
    record::StorageRecord,
    table::{head_key, name_key, TableHead, TableLifecycle, TableMapping, TableMappingState},
    wire::BearerAuthenticator,
};
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{blocks::TestFileBlocks, common::TestStore};

pub struct TestTableHttp {
    pub store: Arc<TestStore>,
    pub context: CatalogContext,
    pub namespace: NamespaceId,
    address: std::net::SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

impl TestTableHttp {
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub async fn new() -> Self {
        Self::start(false, false).await
    }

    pub async fn writable() -> Self {
        Self::start(true, false).await
    }

    pub async fn vending() -> Self {
        Self::start(true, true).await
    }

    async fn start(writable: bool, vending: bool) -> Self {
        let store = Arc::new(TestStore::default());
        let repository = Arc::new(
            CatalogRepository::new(
                store.clone(),
                ClearBounds {
                    delegated_access_ms: if vending { 900_000 } else { 0 },
                    ..ClearBounds::default()
                },
            )
            .unwrap(),
        );
        repository
            .execute(
                ManagementRequest {
                    identity: RequestIdentity {
                        operation: OperationId::random(),
                        issued_ms: 100,
                    },
                    principal: "manager".into(),
                    action: ManagementAction::Initialize,
                    expected_epoch: 0,
                    display_name: "catalog".into(),
                    confirmation: None,
                },
                ManagementPrivilege::Manage,
                100,
            )
            .await
            .unwrap();
        let context = repository.status().await.unwrap().0.context;
        let identifier = NamespaceIdentifier::new(vec!["analytics".into()]).unwrap();
        NamespaceCreator::new(store.clone())
            .create(&NamespaceCreateRequest {
                context,
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: 100,
                },
                principal: "writer".into(),
                identifier: identifier.clone(),
                properties: NamespaceProperties::default(),
            })
            .await
            .unwrap();
        let namespace = NamespaceRepository::new(store.clone())
            .load(context, &identifier)
            .await
            .unwrap()
            .unwrap()
            .namespace;
        let auth =
            BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32))
                .unwrap();
        let service = IcebergHttpService::new(repository, auth, Duration::from_secs(2))
            .with_namespaces(store.clone())
            .unwrap();
        let blocks = Arc::new(TestFileBlocks::default());
        let service = if writable {
            service.with_tables(store.clone(), blocks).unwrap()
        } else {
            service.with_table_reads_for_tests(store.clone(), blocks).unwrap()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = Arc::new(if vending {
            service
                .with_table_credentials(store.clone(), format!("http://{address}"))
                .unwrap()
        } else {
            service
        });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            serve(listener, service, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        Self {
            store,
            context,
            namespace,
            address,
            stop,
            server,
        }
    }

    pub async fn install(&self, name: &str) -> (TableHead, Vec<u8>) {
        let location = TableLocation {
            catalog: self.context.catalog,
            table: TableId::random(),
        };
        let snapshots: Vec<_> = [(10,1),(20,2),(30,3)].into_iter().map(|(snapshot, sequence)| json!({
            "snapshot-id":snapshot,"sequence-number":sequence,"timestamp-ms":1000,"schema-id":0,
            "summary":{"operation":"append"},"manifest-list":location.file(&format!("metadata/{snapshot}.avro")).unwrap().to_string()
        })).collect();
        let metadata = json!({
            "format-version":3,"table-uuid":"12345678-1234-1234-1234-123456789abc",
            "location":location.to_string(),"last-updated-ms":1000,"last-column-id":1,
            "schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"long","required":true}]}],
            "current-schema-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,
            "last-partition-id":999,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0,
            "last-sequence-number":3,"next-row-id":0,"current-snapshot-id":20,"snapshots":snapshots,
            "refs":{"main":{"type":"branch","snapshot-id":20},"tag":{"type":"tag","snapshot-id":30}}
        });
        let mut text = serde_json::to_string_pretty(&metadata).unwrap();
        text.pop();
        text.push_str(",\"future-number\":123456789012345678901234567890}");
        let bytes = text.into_bytes();
        let head = TableHead {
            catalog: self.context.catalog,
            table: location.table,
            namespace: self.namespace,
            name: name.into(),
            name_epoch: 1,
            lifecycle: TableLifecycle::Ready,
            generation: 1,
            metadata_file: FileId::random(),
            metadata_location: location.file("metadata/one.json").unwrap(),
            metadata_digest: Sha256::digest(&bytes).into(),
            format_version: 3,
            table_uuid: Some("12345678-1234-1234-1234-123456789abc".parse().unwrap()),
            operation_fence: 1,
            pending_operation: None,
        };
        FileRepository::new(self.store.clone())
            .publish(
                self.context,
                &FileRecord {
                    file: head.metadata_file,
                    location: head.metadata_location.clone(),
                    kind: FileKind::Metadata,
                    format: ContentFormat::Json,
                    length: bytes.len() as u64,
                    digest: head.metadata_digest,
                    content: FileContent::select_inline(FileKind::Metadata, &bytes).unwrap(),
                    hint: None,
                },
            )
            .await
            .unwrap();
        self.put(
            &head_key(head.catalog, head.table),
            &StorageRecord::TableHead(Box::new(head.clone())),
        );
        self.put(
            &name_key(head.catalog, head.namespace, name).unwrap(),
            &StorageRecord::TableMapping(TableMapping {
                catalog: head.catalog,
                namespace: head.namespace,
                name: name.into(),
                table: head.table,
                name_epoch: 1,
                operation: OperationId::random(),
                state: TableMappingState::Published,
            }),
        );
        (head, bytes)
    }

    pub fn put(&self, key: &IcebergKey, record: &StorageRecord) {
        let mut values = (**self.store.values.load()).clone();
        values.insert(
            key.encode().unwrap(),
            StoredValue {
                bytes: record.encode().unwrap(),
                revision: 1,
            },
        );
        self.store.values.store(Arc::new(values));
    }

    pub async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        role: &str,
        etag: Option<&str>,
    ) -> reqwest::Response {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let mut request = client
            .request(method, format!("http://{}{path}", self.address))
            .bearer_auth(role.repeat(32));
        if let Some(etag) = etag {
            request = request.header("If-None-Match", etag);
        }
        request.send().await.unwrap()
    }

    pub async fn finish(self) {
        self.stop.send(()).unwrap();
        self.server.await.unwrap();
    }

    pub async fn post(
        &self,
        path: &str,
        role: &str,
        key: Option<&str>,
        body: &serde_json::Value,
    ) -> reqwest::Response {
        let mut request = reqwest::Client::new()
            .post(format!("http://{}{path}", self.address))
            .bearer_auth(role.repeat(32))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(body).unwrap());
        if let Some(key) = key {
            request = request.header("idempotency-key", key);
        }
        request.send().await.unwrap()
    }
}
