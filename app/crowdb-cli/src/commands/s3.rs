// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Subcommand;
use quick_xml::{Reader, Writer};
use rand::RngCore;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::Method;

use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum S3Verb {
    Cluster {
        #[command(subcommand)]
        verb: S3ClusterVerb,
    },
    Bucket {
        #[command(subcommand)]
        verb: S3BucketVerb,
    },
    Object {
        #[command(subcommand)]
        verb: S3ObjectVerb,
    },
}

#[derive(Subcommand, Debug)]
pub enum S3ClusterVerb {
    /// Create an empty location or restart an existing mini-cluster.
    Start {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
    },
    Status {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
    },
    /// Stop processes while preserving all data and cluster metadata.
    Stop {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
    },
    /// Stop processes, release ports, and permanently remove the cluster.
    Delete {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
pub enum S3BucketVerb {
    #[command(alias = "add")]
    Put {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
        bucket: String,
    },
    #[command(alias = "remove")]
    Delete {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
        bucket: String,
    },
    List {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
    },
    #[command(alias = "inspect")]
    Get {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
        bucket: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum S3ObjectVerb {
    Put {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
        bucket: String,
        key: String,
        /// Read object content from a file. Reads stdin when no source is set.
        #[arg(long, alias = "input", conflicts_with_all = ["text", "random_size"])]
        file: Option<PathBuf>,
        /// Use the supplied string as UTF-8 object content.
        #[arg(long, value_name = "OBJECT_CONTENT", conflicts_with_all = ["file", "random_size"])]
        text: Option<String>,
        /// Generate a random binary object of exactly BYTES bytes.
        #[arg(long, value_name = "BYTES", conflicts_with_all = ["file", "text"])]
        random_size: Option<usize>,
    },
    Get {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
        bucket: String,
        key: String,
        #[arg(long)]
        output: Option<PathBuf>,
        /// Inclusive byte range formatted as START-END.
        #[arg(long, value_parser = parse_range)]
        range: Option<(u64, u64)>,
    },
    Delete {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
        bucket: String,
        key: String,
    },
    #[command(alias = "inspect")]
    Head {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
        bucket: String,
        key: String,
    },
    List {
        #[arg(long, alias = "data-dir")]
        root: PathBuf,
        bucket: String,
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        continuation: Option<String>,
    },
}

pub async fn run_s3_verb(cli: &Cli, verb: S3Verb) -> ExitCode {
    let result = match verb {
        S3Verb::Cluster { verb } => return run_cluster(cli, verb).await,
        S3Verb::Bucket { verb } => run_bucket(verb).await,
        S3Verb::Object { verb } => run_object(verb).await,
    };
    match result {
        Ok(Some(bytes)) => std::io::stdout().write_all(&bytes).map_or_else(
            |error| {
                eprintln!("error: write output: {error}");
                ExitCode::from(2)
            },
            |()| ExitCode::SUCCESS,
        ),
        Ok(None) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(2)
        }
    }
}

async fn run_cluster(_cli: &Cli, verb: S3ClusterVerb) -> ExitCode {
    let result = match verb {
        S3ClusterVerb::Start { root } => crowdb_console_shared::ops::s3::start(&root).await,
        S3ClusterVerb::Status { root } => crowdb_console_shared::ops::s3::status(&root),
        S3ClusterVerb::Stop { root } => crowdb_console_shared::ops::s3::stop(&root),
        S3ClusterVerb::Delete { root } => crowdb_console_shared::ops::s3::delete(&root),
    };
    match result {
        Ok(status) => {
            println!(
                "S3 mini-cluster: endpoint={} services={}/{} root={}{}",
                status.endpoint,
                status.running_services,
                status.total_services,
                status.data_dir.display(),
                if status.created { " (created)" } else { "" }
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(2)
        }
    }
}

async fn run_bucket(verb: S3BucketVerb) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    let (root, method, bucket) = match verb {
        S3BucketVerb::Put { root, bucket } => (root, Method::PUT, Some(bucket)),
        S3BucketVerb::Delete { root, bucket } => (root, Method::DELETE, Some(bucket)),
        S3BucketVerb::List { root } => (root, Method::GET, None),
        S3BucketVerb::Get { root, bucket } => (root, Method::GET, Some(bucket)),
    };
    let body = send_request(&root, method, bucket.as_deref(), None, &[], None, None).await?;
    Ok((!body.is_empty()).then_some(body))
}

async fn run_object(verb: S3ObjectVerb) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    match verb {
        S3ObjectVerb::Put {
            root,
            bucket,
            key,
            file,
            text,
            random_size,
        } => put_object(&root, &bucket, &key, file, text, random_size).await,
        S3ObjectVerb::Get {
            root,
            bucket,
            key,
            output,
            range,
        } => get_object(&root, &bucket, &key, output, range).await,
        S3ObjectVerb::Delete { root, bucket, key } => {
            send_request(&root, Method::DELETE, Some(&bucket), Some(&key), &[], None, None).await?;
            Ok(None)
        }
        S3ObjectVerb::Head { root, bucket, key } => {
            send_request(&root, Method::HEAD, Some(&bucket), Some(&key), &[], None, None).await?;
            Ok(None)
        }
        S3ObjectVerb::List {
            root,
            bucket,
            prefix,
            limit,
            continuation,
        } => list_objects(&root, &bucket, prefix, limit, continuation).await,
    }
}

async fn put_object(
    root: &Path,
    bucket: &str,
    key: &str,
    file: Option<PathBuf>,
    text: Option<String>,
    random_size: Option<usize>,
) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    let (body, content_type) = match (file, text, random_size) {
        (Some(path), None, None) => (std::fs::read(path)?, "application/octet-stream"),
        (None, Some(text), None) => (text.into_bytes(), "text/plain; charset=utf-8"),
        (None, None, Some(size)) => {
            let mut body = vec![0; size];
            rand::thread_rng().fill_bytes(&mut body);
            (body, "application/octet-stream")
        }
        (None, None, None) => {
            let mut body = Vec::new();
            std::io::stdin().read_to_end(&mut body)?;
            (body, "application/octet-stream")
        }
        _ => unreachable!("clap enforces mutually exclusive object sources"),
    };
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    send_request_with_headers(
        root,
        Method::PUT,
        Some(bucket),
        Some(key),
        &[],
        Some(body),
        None,
        headers,
    )
    .await?;
    Ok(None)
}

async fn get_object(
    root: &Path,
    bucket: &str,
    key: &str,
    output: Option<PathBuf>,
    range: Option<(u64, u64)>,
) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    let body = send_request(root, Method::GET, Some(bucket), Some(key), &[], None, range).await?;
    if let Some(path) = output {
        std::fs::write(path, body)?;
        Ok(None)
    } else {
        Ok(Some(body))
    }
}

fn parse_range(value: &str) -> Result<(u64, u64), String> {
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| "range must use START-END".to_string())?;
    let start = start
        .parse::<u64>()
        .map_err(|_| "invalid range start".to_string())?;
    let end = end.parse::<u64>().map_err(|_| "invalid range end".to_string())?;
    if start > end {
        return Err("range start must not exceed end".into());
    }
    Ok((start, end))
}

async fn list_objects(
    root: &Path,
    bucket: &str,
    prefix: Option<String>,
    limit: Option<usize>,
    continuation: Option<String>,
) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    let mut query = vec![("list-type", "2".to_string())];
    if let Some(value) = prefix {
        query.push(("prefix", value));
    }
    if let Some(value) = limit {
        query.push(("max-keys", value.to_string()));
    }
    if let Some(value) = continuation {
        query.push(("continuation-token", value));
    }
    let body = send_request(root, Method::GET, Some(bucket), None, &query, None, None).await?;
    Ok(Some(body))
}

async fn send_request(
    root: &Path,
    method: Method,
    bucket: Option<&str>,
    object: Option<&str>,
    query: &[(&str, String)],
    body: Option<Vec<u8>>,
    range: Option<(u64, u64)>,
) -> crowdb_console_shared::error::Result<Vec<u8>> {
    send_request_with_headers(root, method, bucket, object, query, body, range, HeaderMap::new()).await
}

#[allow(clippy::too_many_arguments)]
async fn send_request_with_headers(
    root: &Path,
    method: Method,
    bucket: Option<&str>,
    object: Option<&str>,
    query: &[(&str, String)],
    body: Option<Vec<u8>>,
    range: Option<(u64, u64)>,
    headers: HeaderMap,
) -> crowdb_console_shared::error::Result<Vec<u8>> {
    let exchange = crowdb_console_shared::ops::s3::request_exchange_with_headers(
        root, method, bucket, object, query, body, range, headers,
    )
    .await?;
    print_exchange(&exchange);
    let content_type = exchange.response_headers.get(CONTENT_TYPE).cloned();
    if !exchange.status.is_success() {
        let body = format_structured_body(content_type.as_ref(), exchange.response_body);
        if !body.is_empty() {
            print_body_preview('<', &body, false);
        }
        return Err(crowdb_console_shared::error::Error::UpstreamRpc {
            node_id: "s3".into(),
            status: format!("HTTP {}", exchange.status.as_u16()),
        });
    }
    let (_, body) = exchange.into_result()?;
    Ok(format_structured_body(content_type.as_ref(), body))
}

fn print_exchange(exchange: &crowdb_console_shared::ops::s3::S3HttpExchange) {
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

fn print_body_preview(prefix: char, body: &[u8], truncated: bool) {
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
            let value = if matches!(name.as_str(), "authorization" | "cookie" | "set-cookie") {
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

fn format_structured_body(content_type: Option<&reqwest::header::HeaderValue>, body: Vec<u8>) -> Vec<u8> {
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
