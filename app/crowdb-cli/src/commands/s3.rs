// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Subcommand;
use rand::RngCore;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::Method;

use crate::Cli;
use crowdb_console_shared::ops::s3::S3HttpClient;

mod output;
mod target;
use output::{format_structured_body, print_body_preview, print_exchange};
pub(super) use target::client;

#[derive(Subcommand, Debug)]
pub enum S3Verb {
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
pub enum S3BucketVerb {
    #[command(alias = "add")]
    Put {
        bucket: String,
    },
    #[command(alias = "remove")]
    Delete {
        bucket: String,
    },
    List {},
    #[command(alias = "inspect")]
    Get {
        bucket: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum S3ObjectVerb {
    Put {
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
        bucket: String,
        key: String,
        #[arg(long)]
        output: Option<PathBuf>,
        /// Inclusive byte range formatted as START-END.
        #[arg(long, value_parser = parse_range)]
        range: Option<(u64, u64)>,
    },
    Delete {
        bucket: String,
        key: String,
    },
    #[command(alias = "inspect")]
    Head {
        bucket: String,
        key: String,
    },
    List {
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
    let result = match client(cli).await {
        Ok(client) => match verb {
            S3Verb::Bucket { verb } => run_bucket(&client, verb).await,
            S3Verb::Object { verb } => run_object(&client, verb).await,
        },
        Err(error) => Err(error),
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

async fn run_bucket(
    client: &S3HttpClient,
    verb: S3BucketVerb,
) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    let (method, bucket) = match verb {
        S3BucketVerb::Put { bucket } => (Method::PUT, Some(bucket)),
        S3BucketVerb::Delete { bucket } => (Method::DELETE, Some(bucket)),
        S3BucketVerb::List {} => (Method::GET, None),
        S3BucketVerb::Get { bucket } => (Method::GET, Some(bucket)),
    };
    let body = send_request(client, method, bucket.as_deref(), None, &[], None, None).await?;
    Ok((!body.is_empty()).then_some(body))
}

async fn run_object(
    client: &S3HttpClient,
    verb: S3ObjectVerb,
) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    match verb {
        S3ObjectVerb::Put {
            bucket,
            key,
            file,
            text,
            random_size,
        } => put_object(client, &bucket, &key, file, text, random_size).await,
        S3ObjectVerb::Get {
            bucket,
            key,
            output,
            range,
        } => get_object(client, &bucket, &key, output, range).await,
        S3ObjectVerb::Delete { bucket, key } => {
            send_request(client, Method::DELETE, Some(&bucket), Some(&key), &[], None, None).await?;
            Ok(None)
        }
        S3ObjectVerb::Head { bucket, key } => {
            send_request(client, Method::HEAD, Some(&bucket), Some(&key), &[], None, None).await?;
            Ok(None)
        }
        S3ObjectVerb::List {
            bucket,
            prefix,
            limit,
            continuation,
        } => list_objects(client, &bucket, prefix, limit, continuation).await,
    }
}

async fn put_object(
    client: &S3HttpClient,
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
        client,
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
    client: &S3HttpClient,
    bucket: &str,
    key: &str,
    output: Option<PathBuf>,
    range: Option<(u64, u64)>,
) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    let body = send_request(client, Method::GET, Some(bucket), Some(key), &[], None, range).await?;
    if let Some(path) = output {
        std::fs::write(path, body)?;
        Ok(None)
    } else {
        Ok(Some(body))
    }
}

fn parse_range(value: &str) -> std::result::Result<(u64, u64), String> {
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
    client: &S3HttpClient,
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
    let body = send_request(client, Method::GET, Some(bucket), None, &query, None, None).await?;
    Ok(Some(body))
}

async fn send_request(
    client: &S3HttpClient,
    method: Method,
    bucket: Option<&str>,
    object: Option<&str>,
    query: &[(&str, String)],
    body: Option<Vec<u8>>,
    range: Option<(u64, u64)>,
) -> crowdb_console_shared::error::Result<Vec<u8>> {
    send_request_with_headers(
        client,
        method,
        bucket,
        object,
        query,
        body,
        range,
        HeaderMap::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn send_request_with_headers(
    client: &S3HttpClient,
    method: Method,
    bucket: Option<&str>,
    object: Option<&str>,
    query: &[(&str, String)],
    body: Option<Vec<u8>>,
    range: Option<(u64, u64)>,
    headers: HeaderMap,
) -> crowdb_console_shared::error::Result<Vec<u8>> {
    let exchange = client
        .request_exchange_with_headers(method, bucket, object, query, body, range, headers)
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
