// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::metadata::{BucketNameRecord, MultipartPartPage, MultipartUploadPage, ObjectRecord};
use crate::object::ListObjectsV2Page;

#[must_use]
pub fn list_buckets(tenant: &[u8], buckets: &[BucketNameRecord]) -> String {
    let owner = String::from_utf8_lossy(tenant);
    let mut output = xml_start("ListAllMyBucketsResult");
    output.push_str("<Owner><ID>");
    push_escaped(&mut output, &owner);
    output.push_str("</ID><DisplayName>");
    push_escaped(&mut output, &owner);
    output.push_str("</DisplayName></Owner><Buckets>");
    for bucket in buckets {
        output.push_str("<Bucket><Name>");
        push_escaped(&mut output, &String::from_utf8_lossy(&bucket.name));
        output.push_str("</Name>");
        // Bucket records do not persist creation time; use a stable placeholder.
        element(&mut output, "CreationDate", &iso8601(0));
        output.push_str("</Bucket>");
    }
    output.push_str("</Buckets></ListAllMyBucketsResult>");
    output
}

#[must_use]
pub fn create_multipart_upload(bucket: &[u8], key: &[u8], upload_id: &[u8; 16]) -> String {
    let mut output = xml_start("InitiateMultipartUploadResult");
    element(&mut output, "Bucket", &String::from_utf8_lossy(bucket));
    element(&mut output, "Key", &String::from_utf8_lossy(key));
    element(&mut output, "UploadId", &hex_upload_id(upload_id));
    output.push_str("</InitiateMultipartUploadResult>");
    output
}

#[must_use]
pub fn complete_multipart_upload(location: &str, bucket: &[u8], key: &[u8], etag: &str) -> String {
    let mut output = xml_start("CompleteMultipartUploadResult");
    element(&mut output, "Location", location);
    element(&mut output, "Bucket", &String::from_utf8_lossy(bucket));
    element(&mut output, "Key", &String::from_utf8_lossy(key));
    element(&mut output, "ETag", &format!("\"{etag}\""));
    output.push_str("</CompleteMultipartUploadResult>");
    output
}

#[must_use]
pub fn list_multipart_parts(
    bucket: &[u8],
    key: &[u8],
    upload_id: &[u8; 16],
    marker: u16,
    max_parts: usize,
    page: &MultipartPartPage,
) -> String {
    let mut output = xml_start("ListPartsResult");
    element(&mut output, "Bucket", &String::from_utf8_lossy(bucket));
    element(&mut output, "Key", &String::from_utf8_lossy(key));
    element(&mut output, "UploadId", &hex_upload_id(upload_id));
    element(&mut output, "PartNumberMarker", &marker.to_string());
    if let Some(next) = page.next_part_number_marker {
        element(&mut output, "NextPartNumberMarker", &next.to_string());
    }
    element(&mut output, "MaxParts", &max_parts.to_string());
    element(
        &mut output,
        "IsTruncated",
        if page.next_part_number_marker.is_some() {
            "true"
        } else {
            "false"
        },
    );
    for part in &page.parts {
        output.push_str("<Part>");
        element(&mut output, "PartNumber", &part.number.to_string());
        element(&mut output, "LastModified", &iso8601(part.modified_ms));
        element(
            &mut output,
            "ETag",
            &format!("\"{:x}\"", md5::Digest(part.raw_md5)),
        );
        element(&mut output, "Size", &part.length.to_string());
        output.push_str("</Part>");
    }
    output.push_str("</ListPartsResult>");
    output
}

#[must_use]
pub fn list_multipart_uploads(
    bucket: &[u8],
    prefix: &[u8],
    key_marker: Option<&[u8]>,
    upload_marker: Option<&[u8; 16]>,
    max_uploads: usize,
    owner_id: &str,
    page: &MultipartUploadPage,
) -> String {
    let mut output = xml_start("ListMultipartUploadsResult");
    element(&mut output, "Bucket", &String::from_utf8_lossy(bucket));
    element(
        &mut output,
        "KeyMarker",
        &String::from_utf8_lossy(key_marker.unwrap_or_default()),
    );
    element(
        &mut output,
        "UploadIdMarker",
        &upload_marker.map(hex_upload_id).unwrap_or_default(),
    );
    if let Some((key, id)) = &page.next {
        element(&mut output, "NextKeyMarker", &String::from_utf8_lossy(key));
        element(&mut output, "NextUploadIdMarker", &hex_upload_id(id));
    }
    element(&mut output, "Prefix", &String::from_utf8_lossy(prefix));
    element(&mut output, "MaxUploads", &max_uploads.to_string());
    element(
        &mut output,
        "IsTruncated",
        if page.next.is_some() { "true" } else { "false" },
    );
    for session in &page.uploads {
        output.push_str("<Upload>");
        element(&mut output, "Key", &String::from_utf8_lossy(&session.object_key));
        element(&mut output, "UploadId", &hex_upload_id(&session.upload_id));
        output.push_str("<Initiator>");
        element(&mut output, "ID", owner_id);
        output.push_str("</Initiator><Owner>");
        element(&mut output, "ID", owner_id);
        output.push_str("</Owner>");
        element(&mut output, "StorageClass", "STANDARD");
        element(&mut output, "Initiated", &iso8601(session.created_ms));
        output.push_str("</Upload>");
    }
    output.push_str("</ListMultipartUploadsResult>");
    output
}

fn hex_upload_id(upload_id: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut result = String::with_capacity(32);
    for byte in upload_id {
        write!(&mut result, "{byte:02x}").expect("string write cannot fail");
    }
    result
}

#[must_use]
pub fn list_objects(
    bucket: &[u8],
    prefix: &[u8],
    delimiter: Option<&[u8]>,
    max_keys: usize,
    page: &ListObjectsV2Page,
    url_encoding: bool,
    start_after: Option<&[u8]>,
) -> String {
    let mut output = xml_start("ListBucketResult");
    element(&mut output, "Name", &String::from_utf8_lossy(bucket));
    element(&mut output, "Prefix", &listing_text(prefix, url_encoding));
    if url_encoding {
        element(&mut output, "EncodingType", "url");
    }
    if let Some(start_after) = start_after {
        element(
            &mut output,
            "StartAfter",
            &listing_text(start_after, url_encoding),
        );
    }
    if let Some(delimiter) = delimiter {
        element(&mut output, "Delimiter", &listing_text(delimiter, url_encoding));
    }
    element(&mut output, "MaxKeys", &max_keys.to_string());
    element(
        &mut output,
        "KeyCount",
        &(page.objects.len() + page.common_prefixes.len()).to_string(),
    );
    element(
        &mut output,
        "IsTruncated",
        if page.next_continuation_token.is_some() {
            "true"
        } else {
            "false"
        },
    );
    for object in &page.objects {
        object_entry(&mut output, object, url_encoding);
    }
    for common_prefix in &page.common_prefixes {
        output.push_str("<CommonPrefixes>");
        element(&mut output, "Prefix", &listing_text(common_prefix, url_encoding));
        output.push_str("</CommonPrefixes>");
    }
    if let Some(token) = &page.next_continuation_token {
        element(&mut output, "NextContinuationToken", token);
    }
    output.push_str("</ListBucketResult>");
    output
}

fn object_entry(output: &mut String, object: &ObjectRecord, url_encoding: bool) {
    output.push_str("<Contents>");
    element(output, "Key", &listing_text(&object.key, url_encoding));
    element(output, "LastModified", &iso8601(object.modified_at_ms));
    element(output, "ETag", &format!("\"{}\"", object.etag));
    element(output, "Size", &object.logical_length.to_string());
    element(output, "StorageClass", "STANDARD");
    output.push_str("</Contents>");
}

fn listing_text(bytes: &[u8], url_encoding: bool) -> String {
    const SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    if url_encoding {
        percent_encoding::percent_encode(bytes, SET).to_string()
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }
}

fn xml_start(root: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><{root} xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    )
}

#[must_use]
pub fn copy_result(etag: &str, modified_ms: u64, part: bool) -> String {
    let root = if part {
        "CopyPartResult"
    } else {
        "CopyObjectResult"
    };
    let mut output = xml_start(root);
    element(&mut output, "LastModified", &iso8601(modified_ms));
    element(&mut output, "ETag", &format!("\"{etag}\""));
    output.push_str("</");
    output.push_str(root);
    output.push('>');
    output
}

#[must_use]
pub fn delete_objects(results: &[(Vec<u8>, Result<(), crate::error::S3ErrorCode>)], quiet: bool) -> String {
    let mut output = xml_start("DeleteResult");
    for (key, result) in results {
        match result {
            Ok(()) if quiet => {}
            Ok(()) => {
                output.push_str("<Deleted>");
                element(&mut output, "Key", &String::from_utf8_lossy(key));
                output.push_str("</Deleted>");
            }
            Err(code) => {
                output.push_str("<Error>");
                element(&mut output, "Key", &String::from_utf8_lossy(key));
                element(&mut output, "Code", code.code());
                element(&mut output, "Message", code.message());
                output.push_str("</Error>");
            }
        }
    }
    output.push_str("</DeleteResult>");
    output
}

fn element(output: &mut String, name: &str, value: &str) {
    output.push('<');
    output.push_str(name);
    output.push('>');
    push_escaped(output, value);
    output.push_str("</");
    output.push_str(name);
    output.push('>');
}

fn iso8601(timestamp_ms: u64) -> String {
    let seconds = i64::try_from(timestamp_ms / 1000).unwrap_or(i64::MAX);
    chrono::DateTime::from_timestamp(seconds, 0).map_or_else(
        || "1970-01-01T00:00:00Z".into(),
        |value| value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    )
}

fn push_escaped(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '\r' => output.push_str("&#13;"),
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            _ => output.push(character),
        }
    }
}
