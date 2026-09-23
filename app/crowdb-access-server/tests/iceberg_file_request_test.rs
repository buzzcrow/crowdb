#![cfg(feature = "iceberg")]

use crowdb_access_iceberg::file::{FileOperation, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, TableId};
use crowdb_access_server::iceberg::{FileRequest, FileRequestError, MultipartRequest};
use hyper::Method;

fn table() -> TableLocation {
    TableLocation {
        catalog: CatalogId::random(),
        table: TableId::random(),
    }
}

fn path(table: TableLocation, suffix: &str) -> hyper::Uri {
    format!("/{}{suffix}", table.to_string().trim_start_matches("s3://"))
        .parse()
        .unwrap()
}

#[test]
fn native_object_routes_decode_once_and_preserve_plus_percent_and_repeated_slashes() {
    let table = table();
    for (wire, key) in [
        ("a+b", "a+b"),
        ("a%252Fb", "a%2Fb"),
        ("a//b", "a//b"),
        ("%E5%86%B0/a%20b", "冰/a b"),
    ] {
        for (method, operation) in [
            (Method::GET, FileOperation::Get),
            (Method::HEAD, FileOperation::Head),
            (Method::PUT, FileOperation::Put),
        ] {
            let request = FileRequest::parse(&method, &path(table, wire)).unwrap();
            assert_eq!(request.location, table.file(key).unwrap());
            assert_eq!(request.operation, operation);
            assert!(request.multipart.is_none());
        }
    }
}

#[test]
fn native_routes_expose_exact_multipart_operations_but_never_file_delete() {
    let table = table();
    let create = FileRequest::parse(&Method::POST, &path(table, "file?uploads")).unwrap();
    assert_eq!(create.multipart, Some(MultipartRequest::Create));
    let upload = FileRequest::parse(&Method::PUT, &path(table, "file?partNumber=10000&uploadId=id")).unwrap();
    assert_eq!(
        upload.multipart,
        Some(MultipartRequest::Upload {
            upload_id: "id".into(),
            part_number: 10000
        })
    );
    let list = FileRequest::parse(
        &Method::GET,
        &path(table, "file?uploadId=id&max-parts=3&part-number-marker=4"),
    )
    .unwrap();
    assert_eq!(
        list.multipart,
        Some(MultipartRequest::List {
            upload_id: "id".into(),
            marker: 4,
            max_parts: 3
        })
    );
    assert_eq!(
        FileRequest::parse(&Method::POST, &path(table, "file?uploadId=id"))
            .unwrap()
            .operation,
        FileOperation::CompleteMultipart
    );
    assert_eq!(
        FileRequest::parse(&Method::DELETE, &path(table, "file?uploadId=id"))
            .unwrap()
            .operation,
        FileOperation::AbortMultipart
    );
    assert_eq!(
        FileRequest::parse(&Method::DELETE, &path(table, "file")),
        Err(FileRequestError::Unsupported)
    );
}

#[test]
fn native_routes_reject_path_escape_duplicate_parameters_and_general_s3_operations() {
    let table = table();
    for suffix in [
        "",
        "%2e%2e/escape",
        "a/%2e/b",
        "%2Fescape",
        "a%5Cb",
        "a%00b",
        "%ff",
        "%",
        "%2G",
        "file?tagging",
        "file?uploads&uploadId=id",
        "file?partNumber=1",
        "file?uploadId=",
        "file?uploadId=id&uploadId=id",
        "file?uploadId=id&%75ploadId=id",
        "file?uploadId=id&max-parts=1001",
        "file?uploadId=id&part-number-marker=-1",
    ] {
        assert!(
            FileRequest::parse(&Method::GET, &path(table, suffix)).is_err(),
            "{suffix}"
        );
    }
    for part in ["0", "10001", "+1", "-1", "65536", ""] {
        assert!(FileRequest::parse(
            &Method::PUT,
            &path(table, &format!("file?uploadId=id&partNumber={part}"))
        )
        .is_err());
    }
    assert!(FileRequest::parse(&Method::GET, &"/ordinary-bucket/file".parse().unwrap()).is_err());
    assert!(FileRequest::parse(&Method::GET, &"/".parse().unwrap()).is_err());
}

#[test]
fn routing_ignores_only_known_presign_fields_and_bounds_total_input() {
    let table = table();
    let query = "file?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=x&X-Amz-Date=x&X-Amz-Expires=1&X-Amz-Security-Token=x&X-Amz-SignedHeaders=host&X-Amz-Signature=x";
    assert_eq!(
        FileRequest::parse(&Method::GET, &path(table, query))
            .unwrap()
            .operation,
        FileOperation::Get
    );
    assert!(FileRequest::parse(&Method::GET, &path(table, "file?X-Amz-Unknown=x")).is_err());
    assert!(FileRequest::parse(&Method::GET, &path(table, &format!("file?{}", "x".repeat(8192)))).is_err());
}
