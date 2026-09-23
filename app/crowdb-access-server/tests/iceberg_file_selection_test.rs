#![cfg(feature = "iceberg")]

use crowdb_access_server::iceberg::CompleteSelection;

#[test]
fn complete_xml_accepts_only_ordered_sha256_parts() {
    let first = "01".repeat(32);
    let second = "ab".repeat(32);
    let xml = format!("<?xml version=\"1.0\"?><CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{first}\"</ETag></Part><Part><PartNumber>10000</PartNumber><ETag>\"{second}\"</ETag></Part></CompleteMultipartUpload>");
    let selection = CompleteSelection::parse(xml.as_bytes()).unwrap();
    assert_eq!(selection.parts().len(), 2);
    assert_eq!(selection.parts()[0].number, 1);
    assert_eq!(selection.parts()[0].digest, [1; 32]);
    assert_eq!(selection.parts()[1].number, 10_000);
    assert_eq!(selection.parts()[1].digest, [0xab; 32]);
    let sdk_xml = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Part><ETag>\"{first}\"</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>");
    assert_eq!(
        CompleteSelection::parse(sdk_xml.as_bytes()).unwrap().parts()[0].digest,
        [1; 32]
    );
    for quote in ["&quot;", "&#34;", "&#x22;"] {
        let escaped = sdk_xml.replace(&format!("\"{first}\""), &format!("{quote}{first}{quote}"));
        assert_eq!(
            CompleteSelection::parse(escaped.as_bytes()).unwrap().parts()[0].digest,
            [1; 32]
        );
    }
}

#[test]
fn complete_xml_rejects_ambiguous_or_unbounded_inputs() {
    let etag = format!("\"{}\"", "01".repeat(32));
    let part =
        |number: &str, tag: &str| format!("<Part><PartNumber>{number}</PartNumber><ETag>{tag}</ETag></Part>");
    for body in [
        String::new(),
        "<CompleteMultipartUpload/>".into(),
        format!(
            "<CompleteMultipartUpload>{}{}</CompleteMultipartUpload>",
            part("2", &etag),
            part("1", &etag)
        ),
        format!(
            "<CompleteMultipartUpload>{}{}</CompleteMultipartUpload>",
            part("1", &etag),
            part("1", &etag)
        ),
        format!(
            "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
            part("0", &etag)
        ),
        format!(
            "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
            part("1", "bad")
        ),
        format!(
            "<CompleteMultipartUpload extra=\"x\">{}</CompleteMultipartUpload>",
            part("1", &etag)
        ),
        format!(
            "<CompleteMultipartUpload>{}<Other/></CompleteMultipartUpload>",
            part("1", &etag)
        ),
        format!(
            "<!DOCTYPE x><CompleteMultipartUpload>{}</CompleteMultipartUpload>",
            part("1", &etag)
        ),
        "x".repeat(2 * 1024 * 1024 + 1),
        format!(
            "<CompleteMultipartUpload>{}</CompleteMultipartUpload>",
            part("1", &format!("&unknown;{}&unknown;", "01".repeat(32)))
        ),
    ] {
        assert!(CompleteSelection::parse(body.as_bytes()).is_err(), "{body:.100}");
    }
}
