use crowdb_access_iceberg::file::{FileOperation, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, TableId};
use crowdb_access_server::iceberg::parse_file_list;
use crowdb_access_server::iceberg::{FileRequest, MultipartRequest};
use hyper::Method;

fn table() -> TableLocation {
    TableLocation {
        catalog: CatalogId::random(),
        table: TableId::random(),
    }
}

#[test]
fn native_list_requests_preserve_table_prefix_and_validate_all_selectors() {
    let table = table();
    let base = format!(
        "/{}?list-type=2&prefix={}data%2F%E9%9B%AA%252B&encoding-type=url&delimiter=%2F&max-keys=3",
        table.bucket(),
        table.object_prefix()
    );
    let list = parse_file_list(&Method::GET, &base.parse().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(list.table, table);
    assert_eq!(list.prefix, format!("{}data/雪%2B", table.object_prefix()));
    assert_eq!(list.delimiter.as_deref(), Some("/"));
    assert!(list.encoding_url);
    assert_eq!(list.max_keys, 3);
    for query in [
        "list-type=2",
        "list-type=1&prefix=t/",
        "list-type=2&prefix=",
        "list-type=2&prefix=t/../",
        "list-type=2&prefix=%ff",
        "list-type=2&prefix=%",
        "list-type=2&prefix=a&list-type=2",
    ] {
        let uri = format!("/{}?{query}", table.bucket()).parse().unwrap();
        assert!(parse_file_list(&Method::GET, &uri).is_err(), "{query}");
    }
    for extra in [
        "&max-keys=1001",
        "&max-keys=-1",
        "&max-keys=1&max-keys=2",
        "&encoding-type=unknown",
        "&delimiter=other",
        "&uploadId=x",
        "&versionId=1",
        "&fetch-owner=true",
        "&continuation-token=x&start-after=x",
    ] {
        let uri = format!(
            "/{}?list-type=2&prefix={}{}",
            table.bucket(),
            table.object_prefix(),
            extra
        )
        .parse()
        .unwrap();
        assert!(parse_file_list(&Method::GET, &uri).is_err(), "{extra}");
    }
    assert!(parse_file_list(&Method::PUT, &base.parse().unwrap()).is_err());
    assert!(parse_file_list(&Method::GET, &path(table, "file"))
        .unwrap()
        .is_none());
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
fn native_routes_distinguish_multipart_abort_from_object_cleanup() {
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
    let cleanup = FileRequest::parse(&Method::DELETE, &path(table, "file")).unwrap();
    assert_eq!(cleanup.operation, FileOperation::DeleteObject);
    assert!(cleanup.multipart.is_none());
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
