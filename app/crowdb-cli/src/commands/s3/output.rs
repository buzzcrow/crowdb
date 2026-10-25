// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Credential-redacted HTTP diagnostics and structured response formatting.

use quick_xml::{Reader, Writer};
use reqwest::header::{HeaderMap, CONTENT_TYPE};

pub(super) fn print_exchange(exchange: &crowdb_console_shared::ops::s3::S3HttpExchange) {
    eprintln!(
        "\x1b[1;34mHTTP REQUEST\x1b[0m\n> {} {}",
        exchange.request.method, exchange.request.url
    );
    print_headers('>', &exchange.request.headers);
    match exchange.request.body_bytes {
        None => eprintln!("> body: none"),
        Some(bytes) => {
            if let Some(preview) = &exchange.request.body_preview {
                eprintln!("\x1b[35m> body: text ({bytes} bytes)\x1b[0m");
                print_body_preview('>', preview, exchange.request.body_preview_truncated);
            } else {
                eprintln!("\x1b[35m> body: binary ({bytes} bytes, not shown)\x1b[0m");
            }
        }
    }

    eprintln!("\x1b[1;34mHTTP RESPONSE\x1b[0m\n< {}", exchange.status);
    print_headers('<', &exchange.response_headers);
    let body = &exchange.response_body;
    if body.is_empty() {
        eprintln!("\x1b[35m< body: none\x1b[0m");
    } else {
        let kind = body_kind(exchange.response_headers.get(CONTENT_TYPE), body);
        match kind {
            BodyKind::Json => eprintln!(
                "\x1b[35m< body: JSON ({} bytes, formatted on stdout)\x1b[0m",
                body.len()
            ),
            BodyKind::Xml => eprintln!(
                "\x1b[35m< body: XML ({} bytes, formatted on stdout)\x1b[0m",
                body.len()
            ),
            BodyKind::Text => eprintln!(
                "\x1b[35m< body: text ({} bytes, written to stdout)\x1b[0m",
                body.len()
            ),
            BodyKind::Binary => eprintln!(
                "\x1b[35m< body: binary ({} bytes, not shown in trace)\x1b[0m",
                body.len()
            ),
        }
    }
}

pub(super) fn print_body_preview(prefix: char, body: &[u8], truncated: bool) {
    let text = String::from_utf8_lossy(body);
    for line in text.lines() {
        eprintln!("\x1b[35m{prefix} | {line}\x1b[0m");
    }
    if truncated {
        eprintln!("\x1b[35m{prefix} | ... preview truncated at 65536 bytes\x1b[0m");
    }
}

fn print_headers(prefix: char, headers: &HeaderMap) {
    if headers.is_empty() {
        eprintln!("{prefix} headers: none");
        return;
    }
    let mut values = headers
        .iter()
        .map(|(name, value)| {
            let value = if matches!(
                name.as_str(),
                "authorization" | "cookie" | "set-cookie" | "x-amz-security-token"
            ) {
                "<redacted>".to_string()
            } else {
                value
                    .to_str()
                    .map_or_else(|_| "<non-UTF-8>".to_string(), ToOwned::to_owned)
            };
            (name.as_str(), value)
        })
        .collect::<Vec<_>>();
    values.sort_unstable_by(|left, right| left.0.cmp(right.0));
    for (name, value) in values {
        eprintln!("{prefix} {name}: {value}");
    }
}

#[derive(Clone, Copy)]
enum BodyKind {
    Json,
    Xml,
    Text,
    Binary,
}

fn body_kind(content_type: Option<&reqwest::header::HeaderValue>, body: &[u8]) -> BodyKind {
    let media_type = content_type
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if media_type.contains("json") {
        return BodyKind::Json;
    }
    if media_type.contains("xml") {
        return BodyKind::Xml;
    }
    if media_type.starts_with("text/") {
        return BodyKind::Text;
    }
    match body.iter().find(|byte| !byte.is_ascii_whitespace()) {
        Some(b'{' | b'[') => BodyKind::Json,
        Some(b'<') => BodyKind::Xml,
        _ if std::str::from_utf8(body).is_ok() && !media_type.contains("octet-stream") => BodyKind::Text,
        _ => BodyKind::Binary,
    }
}

pub(super) fn format_structured_body(
    content_type: Option<&reqwest::header::HeaderValue>,
    body: Vec<u8>,
) -> Vec<u8> {
    match body_kind(content_type, &body) {
        BodyKind::Json => serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| serde_json::to_vec_pretty(&value).ok())
            .map_or(body, with_trailing_newline),
        BodyKind::Xml => format_xml(&body).map_or(body, with_trailing_newline),
        BodyKind::Text | BodyKind::Binary => body,
    }
}

fn format_xml(body: &[u8]) -> Option<Vec<u8>> {
    let mut reader = Reader::from_reader(body);
    let mut writer = Writer::new_with_indent(Vec::new(), b' ', 2);
    let mut buffer = Vec::new();
    loop {
        match reader.read_event_into(&mut buffer).ok()? {
            quick_xml::events::Event::Eof => break,
            event => writer.write_event(event.into_owned()).ok()?,
        }
        buffer.clear();
    }
    Some(writer.into_inner())
}

fn with_trailing_newline(mut body: Vec<u8>) -> Vec<u8> {
    if !body.ends_with(b"\n") {
        body.push(b'\n');
    }
    body
}
