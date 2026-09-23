#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
mod blocks;
#[path = "common/iceberg_upload.rs"]
mod upload;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    ByteRange, FileGrant, FileIdentity, FileOperation, FileOperations, MultipartAdmissionLimits,
    MultipartAdmissionRecord, MultipartCredit, MultipartLimits, MultipartPhase, MultipartSession,
    TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, OperationId, TableId};
use crowdb_access_server::iceberg::{
    FileAdmissionError, FileRequest, FileResponseBudget, FileServiceLimits, FileTransferAdmission,
    FileUploadBudget,
};
use http_body_util::BodyExt;
use hyper::Method;

fn grant(operations: &[FileOperation]) -> FileGrant {
    FileGrant {
        context: CatalogContext {
            catalog: CatalogId::random(),
            activation_epoch: 1,
        },
        table: TableId::random(),
        principal: [5; 32],
        nonce: OperationId::random(),
        issued_ms: 100,
        expires_ms: 200,
        operations: FileOperations::new(operations).unwrap(),
        max_request_bytes: 5,
        max_file_bytes: 20,
    }
}

fn location(grant: &FileGrant) -> crowdb_access_iceberg::file::FileLocation {
    TableLocation {
        catalog: grant.context.catalog,
        table: grant.table,
    }
    .file("data/a.parquet")
    .unwrap()
}

fn request(grant: &FileGrant, method: &Method, suffix: &str) -> FileRequest {
    let uri: hyper::Uri = format!(
        "/{}/{}{}",
        location(grant).table().bucket(),
        location(grant).object_key(),
        suffix
    )
    .parse()
    .unwrap();
    FileRequest::parse(method, &uri).unwrap()
}

fn service() -> FileServiceLimits {
    FileServiceLimits {
        max_request_bytes: 7,
        max_file_bytes: 12,
        max_part_bytes: 4,
        max_staged_bytes: 30,
    }
}

fn session(grant: &FileGrant) -> MultipartSession {
    let table = location(grant).table();
    MultipartSession {
        context: grant.context,
        upload: OperationId::random(),
        owner: FileIdentity {
            table,
            file: FileId::random(),
        },
        location: location(grant),
        principal: grant.principal,
        revision: 1,
        created_ms: 100,
        expires_ms: 200,
        limits: MultipartLimits {
            max_parts: 10,
            max_part_bytes: 4,
            max_file_bytes: 12,
            max_staged_bytes: 20,
            ttl_ms: 100,
        },
        phase: MultipartPhase::Open,
        part_count: 0,
        staged_bytes: 0,
        completion: None,
        published: None,
        pending: None,
        credit: None,
    }
}

fn policy(grant: &FileGrant) -> MultipartAdmissionRecord {
    MultipartAdmissionRecord {
        context: grant.context,
        policy: OperationId::random(),
        revision: 1,
        limits: MultipartAdmissionLimits {
            max_sessions: 2,
            max_reserved_bytes: 25,
        },
        sessions: 0,
        reserved_bytes: 0,
        pending: None,
    }
}

#[test]
fn authorization_intersects_grant_operation_scope_and_service_limits() {
    let grant = grant(&[FileOperation::Get, FileOperation::Put]);
    let get_request = request(&grant, &Method::GET, "");
    let admitted = FileTransferAdmission::authorize(&grant, &get_request, service(), None, 100).unwrap();
    assert!(admitted.check_bytes(5, 12).is_ok());
    assert!(matches!(
        admitted.check_bytes(6, 12),
        Err(FileAdmissionError::Bounds)
    ));
    assert!(matches!(
        admitted.check_bytes(5, 13),
        Err(FileAdmissionError::Bounds)
    ));
    let mut wrong = get_request.clone();
    wrong.location = TableLocation {
        catalog: grant.context.catalog,
        table: TableId::random(),
    }
    .file("data/a.parquet")
    .unwrap();
    assert!(FileTransferAdmission::authorize(&grant, &wrong, service(), None, 100).is_err());
    wrong = get_request;
    wrong.operation = FileOperation::CreateMultipart;
    assert!(FileTransferAdmission::authorize(&grant, &wrong, service(), None, 100).is_err());
    assert!(FileTransferAdmission::authorize(
        &grant,
        &request(&grant, &Method::GET, ""),
        service(),
        None,
        200
    )
    .is_err());
    let mut invalid = service();
    invalid.max_part_bytes = 0;
    assert!(
        FileTransferAdmission::authorize(&grant, &request(&grant, &Method::GET, ""), invalid, None, 100)
            .is_err()
    );
}

#[tokio::test]
async fn authorized_upload_and_range_reads_enforce_declared_and_actual_bytes() {
    let owner = upload::owner();
    let mut grant = grant(&[FileOperation::Put, FileOperation::Get]);
    grant.context.catalog = owner.table.catalog;
    grant.table = owner.table.table;
    let put =
        FileTransferAdmission::authorize(&grant, &request(&grant, &Method::PUT, ""), service(), None, 101)
            .unwrap();
    let store = Arc::new(upload::TestUploadBlocks::default());
    let budget = FileUploadBudget::new(1).unwrap();
    let too_large = upload::TestUploadBody::new(b"123456", 2);
    let polls = too_large.polls.clone();
    assert!(put
        .receive(&budget, too_large, store.clone(), owner, Some(6), None)
        .await
        .is_err());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let too_large = upload::TestUploadBody::new(b"123456", 2);
    assert!(put
        .receive(&budget, too_large, store.clone(), owner, None, None)
        .await
        .is_err());
    let tree = put
        .receive(
            &budget,
            upload::TestUploadBody::new(b"12345", 2),
            store,
            owner,
            Some(5),
            None,
        )
        .await
        .unwrap();
    assert_eq!(tree.length, 5);
    assert_eq!(budget.active(), 0);

    let read_store = Arc::new(blocks::TestFileBlocks {
        bytes: vec![9; 12],
        ..Default::default()
    });
    let mut record = read_store.record();
    record.location = location(&grant);
    let get =
        FileTransferAdmission::authorize(&grant, &request(&grant, &Method::GET, ""), service(), None, 101)
            .unwrap();
    let responses = FileResponseBudget::new(1).unwrap();
    assert!(get
        .read_body(&responses, read_store.clone(), record.clone(), None)
        .is_err());
    let range = Some(ByteRange { start: 0, end: 5 });
    let body = get
        .read_body(&responses, read_store.clone(), record.clone(), range)
        .unwrap();
    assert_eq!(body.collect().await.unwrap().to_bytes().len(), 5);
    assert_eq!(responses.active(), 0);
    let mut larger = record;
    larger.length = 13;
    assert!(matches!(
        get.read_body(&responses, read_store, larger, range),
        Err(FileAdmissionError::Bounds)
    ));
}

#[test]
fn multipart_session_and_global_credit_intersections_fail_closed() {
    let grant = grant(&[
        FileOperation::CreateMultipart,
        FileOperation::UploadPart,
        FileOperation::ListParts,
    ]);
    let create = request(&grant, &Method::POST, "?uploads");
    let admitted = FileTransferAdmission::authorize(&grant, &create, service(), None, 101).unwrap();
    let mut session = session(&grant);
    let mut policy = policy(&grant);
    assert!(admitted.check_create(&session, &policy).is_ok());
    policy.sessions = policy.limits.max_sessions;
    policy.reserved_bytes = policy.limits.max_reserved_bytes;
    assert!(matches!(
        admitted.check_create(&session, &policy),
        Err(FileAdmissionError::Bounds)
    ));
    policy.sessions = 1;
    policy.reserved_bytes = 10;
    assert!(matches!(
        admitted.check_create(&session, &policy),
        Err(FileAdmissionError::Bounds)
    ));
    policy.reserved_bytes = 0;
    policy.sessions = 0;
    session.limits.max_part_bytes = 5;
    assert!(matches!(
        admitted.check_create(&session, &policy),
        Err(FileAdmissionError::Bounds)
    ));
    session.limits.max_part_bytes = 4;
    session.limits.max_file_bytes = 13;
    assert!(matches!(
        admitted.check_create(&session, &policy),
        Err(FileAdmissionError::Bounds)
    ));
    session.limits.max_file_bytes = 12;
    session.limits.max_staged_bytes = 31;
    assert!(matches!(
        admitted.check_create(&session, &policy),
        Err(FileAdmissionError::Bounds)
    ));
    session.limits.max_staged_bytes = 20;

    let part_request = request(
        &grant,
        &Method::PUT,
        &format!("?uploadId={}&partNumber=1", session.upload),
    );
    assert!(FileTransferAdmission::authorize(&grant, &part_request, service(), Some(&session), 101).is_err());
    session.credit = Some(MultipartCredit {
        policy: policy.policy,
        sequence: 2,
        released: false,
    });
    let part =
        FileTransferAdmission::authorize(&grant, &part_request, service(), Some(&session), 101).unwrap();
    assert!(part.check_bytes(4, 12).is_ok());
    assert!(matches!(part.check_bytes(5, 12), Err(FileAdmissionError::Bounds)));
    let list = request(&grant, &Method::GET, &format!("?uploadId={}", session.upload));
    assert!(FileTransferAdmission::authorize(&grant, &list, service(), Some(&session), 101).is_ok());
    session.principal[0] ^= 1;
    assert!(FileTransferAdmission::authorize(&grant, &part_request, service(), Some(&session), 101).is_err());
    session.principal = grant.principal;
    session.credit.as_mut().unwrap().released = true;
    assert!(FileTransferAdmission::authorize(&grant, &part_request, service(), Some(&session), 101).is_err());
}
