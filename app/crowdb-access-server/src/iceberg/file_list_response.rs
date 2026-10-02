use crowdb_access_iceberg::file::{FileListPage, FileListRequest};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

const KEY_ENCODING: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

pub(super) fn list_files(request: &FileListRequest, page: &FileListPage) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    element(&mut xml, "Name", &request.table.bucket());
    path(&mut xml, "Prefix", &request.prefix, request.encoding_url);
    if let Some(delimiter) = &request.delimiter {
        path(&mut xml, "Delimiter", delimiter, request.encoding_url);
    }
    if request.encoding_url {
        element(&mut xml, "EncodingType", "url");
    }
    element(&mut xml, "MaxKeys", &request.max_keys.to_string());
    element(
        &mut xml,
        "KeyCount",
        &(page.files.len() + page.common_prefixes.len()).to_string(),
    );
    element(
        &mut xml,
        "IsTruncated",
        if page.next_continuation_token.is_some() {
            "true"
        } else {
            "false"
        },
    );
    if let Some(token) = &request.continuation_token {
        element(&mut xml, "ContinuationToken", token);
    }
    if let Some(after) = &request.start_after {
        path(&mut xml, "StartAfter", after, request.encoding_url);
    }
    for file in &page.files {
        xml.push_str("<Contents>");
        path(&mut xml, "Key", &file.key, request.encoding_url);
        // Native file records have no wall-clock publication timestamp.
        element(&mut xml, "LastModified", "1970-01-01T00:00:00Z");
        element(&mut xml, "ETag", &format!("\"{}\"", file.etag));
        element(&mut xml, "Size", &file.length.to_string());
        element(&mut xml, "StorageClass", "STANDARD");
        xml.push_str("</Contents>");
    }
    for prefix in &page.common_prefixes {
        xml.push_str("<CommonPrefixes>");
        path(&mut xml, "Prefix", prefix, request.encoding_url);
        xml.push_str("</CommonPrefixes>");
    }
    if let Some(token) = &page.next_continuation_token {
        element(&mut xml, "NextContinuationToken", token);
    }
    xml.push_str("</ListBucketResult>");
    xml
}

fn path(xml: &mut String, name: &str, value: &str, encoded: bool) {
    if encoded {
        element(xml, name, &utf8_percent_encode(value, KEY_ENCODING).to_string());
    } else {
        element(xml, name, value);
    }
}

fn element(xml: &mut String, name: &str, value: &str) {
    xml.push('<');
    xml.push_str(name);
    xml.push('>');
    for character in value.chars() {
        match character {
            '&' => xml.push_str("&amp;"),
            '<' => xml.push_str("&lt;"),
            '>' => xml.push_str("&gt;"),
            '"' => xml.push_str("&quot;"),
            '\'' => xml.push_str("&apos;"),
            _ => xml.push(character),
        }
    }
    xml.push_str("</");
    xml.push_str(name);
    xml.push('>');
}
