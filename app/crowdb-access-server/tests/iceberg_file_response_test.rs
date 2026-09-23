#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
mod blocks;

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    AssemblyProgress, ContentFormat, FileContent, FileIdentity, FileKind, FileTree, FileWriterCheckpoint,
    MultipartCompletion, MultipartLimits, MultipartPart, MultipartPartPage, MultipartPhase, MultipartSession,
    TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, OperationId, TableId};
use crowdb_access_iceberg::operation::PayloadReference;
use crowdb_access_server::iceberg::{FileS3ErrorCode, MultipartResponses};
use hyper::StatusCode;
use sha2::{Digest, Sha256};

fn session() -> MultipartSession {
    let table = TableLocation {
        catalog: CatalogId::random(),
        table: TableId::random(),
    };
    MultipartSession {
        context: CatalogContext {
            catalog: table.catalog,
            activation_epoch: 1,
        },
        upload: OperationId::random(),
        owner: FileIdentity {
            table,
            file: FileId::random(),
        },
        location: table.file("metadata/a&<b>.json").unwrap(),
        principal: [1; 32],
        revision: 1,
        created_ms: 1_704_067_200_000,
        expires_ms: 1_704_067_300_000,
        limits: MultipartLimits {
            max_parts: 10,
            max_part_bytes: 1024,
            max_file_bytes: 1024,
            max_staged_bytes: 2048,
            ttl_ms: 100_000,
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

fn part(session: &MultipartSession, number: u16, modified_ms: u64) -> MultipartPart {
    MultipartPart {
        upload: session.upload,
        number,
        revision: 1,
        modified_ms,
        owner: FileIdentity {
            file: FileId::random(),
            ..session.owner
        },
        tree: FileTree {
            root: None,
            length: 0,
            digest: Sha256::digest([]).into(),
        },
    }
}

#[test]
fn create_list_and_abort_emit_s3_shaped_xml_and_stable_page_markers() {
    let session = session();
    let create = MultipartResponses::create(&session).unwrap();
    assert_eq!(create.status(), StatusCode::OK);
    let body = String::from_utf8(create.into_body()).unwrap();
    assert!(body.contains("<InitiateMultipartUploadResult"));
    assert!(body.contains("<Key>t/"));
    assert!(body.contains("a&amp;&lt;b&gt;.json</Key>"));
    assert!(body.contains(&format!("<UploadId>{}</UploadId>", session.upload)));
    let first = part(&session, 1, session.created_ms);
    let third = part(&session, 3, session.created_ms + 1234);
    let response = MultipartResponses::upload_part(&third).unwrap();
    let tag = response.headers().get("etag").unwrap().to_str().unwrap();
    assert_eq!(tag.len(), 66);
    let page = MultipartPartPage {
        parts: vec![first, third],
        next_marker: Some(3),
    };
    let list = MultipartResponses::list_parts(&session, &page, 0, 1000).unwrap();
    assert_eq!(list.headers().get("content-type").unwrap(), "application/xml");
    let body = String::from_utf8(list.into_body()).unwrap();
    assert!(body.contains("<NextPartNumberMarker>3</NextPartNumberMarker>"));
    assert!(body.contains("<MaxParts>1000</MaxParts>"));
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(body.contains("<LastModified>2024-01-01T00:00:01.234Z</LastModified>"));
    assert_eq!(body.matches("<Part>").count(), 2);
    assert!(body.contains(&format!("<ETag>{}</ETag>", tag.replace('"', "&quot;"))));
    let next = MultipartPartPage {
        parts: vec![],
        next_marker: None,
    };
    let body = String::from_utf8(
        MultipartResponses::list_parts(&session, &next, 3, 1000)
            .unwrap()
            .into_body(),
    )
    .unwrap();
    assert!(body.contains("<IsTruncated>false</IsTruncated>"));
    assert!(!body.contains("NextPartNumberMarker"));
    assert_eq!(MultipartResponses::abort().status(), StatusCode::NO_CONTENT);
}

#[test]
fn malformed_pages_timestamps_and_escape_input_fail_before_success() {
    let session = session();
    let mut page = MultipartPartPage {
        parts: vec![part(&session, 2, session.created_ms)],
        next_marker: Some(1),
    };
    assert!(MultipartResponses::list_parts(&session, &page, 0, 1000).is_err());
    page.next_marker = Some(2);
    assert!(MultipartResponses::list_parts(&session, &page, 2, 1000).is_err());
    page.parts[0].modified_ms = session.expires_ms;
    assert!(MultipartResponses::list_parts(&session, &page, 0, 1000).is_err());
    page.parts[0].modified_ms = u64::MAX;
    assert!(MultipartResponses::list_parts(&session, &page, 0, 1000).is_err());
    let response = MultipartResponses::error(FileS3ErrorCode::InvalidPart, "/x&<y>", "req-1").unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = String::from_utf8(response.into_body()).unwrap();
    assert!(body.contains("<Code>InvalidPart</Code>"));
    assert!(body.contains("<Resource>/x&amp;&lt;y&gt;</Resource>"));
    assert!(MultipartResponses::error(FileS3ErrorCode::InternalError, &"x".repeat(2049), "req").is_err());
    for (code, status) in [
        (FileS3ErrorCode::AccessDenied, StatusCode::FORBIDDEN),
        (FileS3ErrorCode::NoSuchUpload, StatusCode::NOT_FOUND),
        (FileS3ErrorCode::InvalidPart, StatusCode::BAD_REQUEST),
        (FileS3ErrorCode::EntityTooLarge, StatusCode::PAYLOAD_TOO_LARGE),
        (FileS3ErrorCode::InvalidRequest, StatusCode::BAD_REQUEST),
        (FileS3ErrorCode::InternalError, StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        assert_eq!(
            MultipartResponses::error(code, "/file", "id").unwrap().status(),
            status
        );
    }
}

#[test]
fn complete_uses_only_a_published_matching_record() {
    let mut session = session();
    let store = blocks::TestFileBlocks {
        bytes: b"bytes".to_vec(),
        ..Default::default()
    };
    let mut record = store.record();
    record.file = session.owner.file;
    record.location = session.location.clone();
    record.kind = FileKind::Metadata;
    record.format = ContentFormat::Json;
    let candidate = FileTree {
        root: match &record.content {
            FileContent::Chunks { root } => root.clone(),
            FileContent::Inline { .. } => None,
        },
        length: record.length,
        digest: record.digest,
    };
    let selection = PayloadReference {
        catalog: session.context.catalog,
        operation: session.upload,
        digest: [3; 32],
        length: 8,
    };
    session.part_count = 1;
    session.staged_bytes = 5;
    session.phase = MultipartPhase::Published;
    session.published = Some(session.owner.file);
    session.completion = Some(MultipartCompletion {
        selection: selection.clone(),
        selected_parts: 1,
        progress: AssemblyProgress {
            selection: selection.digest,
            next_part: 1,
            part_offset: 0,
            completed_bytes: 5,
            writer: Some(FileWriterCheckpoint {
                root: candidate.root.clone().unwrap(),
            }),
            active: None,
            part_digest: None,
        },
        candidate: Some(candidate.clone()),
        publication: Some(selection),
    });
    let complete = MultipartResponses::complete(&session, &record, "https://storage.example/a&b").unwrap();
    let body = String::from_utf8(complete.into_body()).unwrap();
    assert!(body.contains("<CompleteMultipartUploadResult"));
    assert!(body.contains("<ETag>&quot;"));
    assert!(body.contains("<Location>https://storage.example/a&amp;b</Location>"));
    let mut wrong = record.clone();
    wrong.file = FileId::random();
    assert!(MultipartResponses::complete(&session, &wrong, "https://storage.example/a").is_err());
    assert!(MultipartResponses::complete(&session, &record, "s3://wrong").is_err());
    session.phase = MultipartPhase::Publishing;
    session.published = None;
    assert!(MultipartResponses::complete(&session, &record, "https://storage.example/a").is_err());
}
