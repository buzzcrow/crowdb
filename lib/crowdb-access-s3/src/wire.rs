// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::metadata::{BucketNameRecord, ObjectRecord};
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
        output.push_str("</Name></Bucket>");
    }
    output.push_str("</Buckets></ListAllMyBucketsResult>");
    output
}

#[must_use]
pub fn list_objects(
    bucket: &[u8],
    prefix: &[u8],
    delimiter: Option<&[u8]>,
    max_keys: usize,
    page: &ListObjectsV2Page,
) -> String {
    let mut output = xml_start("ListBucketResult");
    element(&mut output, "Name", &String::from_utf8_lossy(bucket));
    element(&mut output, "Prefix", &String::from_utf8_lossy(prefix));
    if let Some(delimiter) = delimiter {
        element(&mut output, "Delimiter", &String::from_utf8_lossy(delimiter));
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
        object_entry(&mut output, object);
    }
    for common_prefix in &page.common_prefixes {
        output.push_str("<CommonPrefixes>");
        element(&mut output, "Prefix", &String::from_utf8_lossy(common_prefix));
        output.push_str("</CommonPrefixes>");
    }
    if let Some(token) = &page.next_continuation_token {
        element(&mut output, "NextContinuationToken", token);
    }
    output.push_str("</ListBucketResult>");
    output
}

fn object_entry(output: &mut String, object: &ObjectRecord) {
    output.push_str("<Contents>");
    element(output, "Key", &String::from_utf8_lossy(&object.key));
    element(output, "LastModified", &iso8601(object.modified_at_ms));
    element(output, "ETag", &format!("\"{}\"", object.etag));
    element(output, "Size", &object.logical_length.to_string());
    element(output, "StorageClass", "STANDARD");
    output.push_str("</Contents>");
}

fn xml_start(root: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><{root} xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    )
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
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            _ => output.push(character),
        }
    }
}
