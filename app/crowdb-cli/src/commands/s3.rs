// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Subcommand;
use reqwest::Method;

use crate::commands::print_json;
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
        #[arg(long)]
        data_dir: PathBuf,
    },
    Status {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Stop processes while preserving all data and cluster metadata.
    Stop {
        #[arg(long)]
        data_dir: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
pub enum S3BucketVerb {
    Add {
        #[arg(long)]
        data_dir: PathBuf,
        bucket: String,
    },
    Remove {
        #[arg(long)]
        data_dir: PathBuf,
        bucket: String,
    },
    List {
        #[arg(long)]
        data_dir: PathBuf,
    },
    Inspect {
        #[arg(long)]
        data_dir: PathBuf,
        bucket: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum S3ObjectVerb {
    Put {
        #[arg(long)]
        data_dir: PathBuf,
        bucket: String,
        key: String,
        #[arg(long)]
        input: Option<PathBuf>,
    },
    Get {
        #[arg(long)]
        data_dir: PathBuf,
        bucket: String,
        key: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    Delete {
        #[arg(long)]
        data_dir: PathBuf,
        bucket: String,
        key: String,
    },
    Inspect {
        #[arg(long)]
        data_dir: PathBuf,
        bucket: String,
        key: String,
    },
    List {
        #[arg(long)]
        data_dir: PathBuf,
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

async fn run_cluster(cli: &Cli, verb: S3ClusterVerb) -> ExitCode {
    let result = match verb {
        S3ClusterVerb::Start { data_dir } => crowdb_console_shared::ops::s3::start(&data_dir).await,
        S3ClusterVerb::Status { data_dir } => crowdb_console_shared::ops::s3::status(&data_dir),
        S3ClusterVerb::Stop { data_dir } => crowdb_console_shared::ops::s3::stop(&data_dir),
    };
    match result {
        Ok(status) if cli.json => print_json(cli, &status),
        Ok(status) => {
            println!(
                "S3 mini-cluster: endpoint={} services={}/{} data_dir={}{}",
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
    let (data_dir, method, bucket) = match verb {
        S3BucketVerb::Add { data_dir, bucket } => (data_dir, Method::PUT, Some(bucket)),
        S3BucketVerb::Remove { data_dir, bucket } => (data_dir, Method::DELETE, Some(bucket)),
        S3BucketVerb::List { data_dir } => (data_dir, Method::GET, None),
        S3BucketVerb::Inspect { data_dir, bucket } => (data_dir, Method::HEAD, Some(bucket)),
    };
    let (_, body) =
        crowdb_console_shared::ops::s3::request(&data_dir, method, bucket.as_deref(), None, &[], None)
            .await?;
    Ok((!body.is_empty()).then_some(body))
}

async fn run_object(verb: S3ObjectVerb) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    match verb {
        S3ObjectVerb::Put {
            data_dir,
            bucket,
            key,
            input,
        } => put_object(&data_dir, &bucket, &key, input).await,
        S3ObjectVerb::Get {
            data_dir,
            bucket,
            key,
            output,
        } => get_object(&data_dir, &bucket, &key, output).await,
        S3ObjectVerb::Delete {
            data_dir,
            bucket,
            key,
        } => {
            crowdb_console_shared::ops::s3::request(
                &data_dir,
                Method::DELETE,
                Some(&bucket),
                Some(&key),
                &[],
                None,
            )
            .await?;
            Ok(None)
        }
        S3ObjectVerb::Inspect {
            data_dir,
            bucket,
            key,
        } => {
            crowdb_console_shared::ops::s3::request(
                &data_dir,
                Method::HEAD,
                Some(&bucket),
                Some(&key),
                &[],
                None,
            )
            .await?;
            Ok(None)
        }
        S3ObjectVerb::List {
            data_dir,
            bucket,
            prefix,
            limit,
            continuation,
        } => list_objects(&data_dir, &bucket, prefix, limit, continuation).await,
    }
}

async fn put_object(
    data_dir: &Path,
    bucket: &str,
    key: &str,
    input: Option<PathBuf>,
) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    let body = if let Some(path) = input {
        std::fs::read(path)?
    } else {
        let mut body = Vec::new();
        std::io::stdin().read_to_end(&mut body)?;
        body
    };
    crowdb_console_shared::ops::s3::request(data_dir, Method::PUT, Some(bucket), Some(key), &[], Some(body))
        .await?;
    Ok(None)
}

async fn get_object(
    data_dir: &Path,
    bucket: &str,
    key: &str,
    output: Option<PathBuf>,
) -> crowdb_console_shared::error::Result<Option<Vec<u8>>> {
    let (_, body) =
        crowdb_console_shared::ops::s3::request(data_dir, Method::GET, Some(bucket), Some(key), &[], None)
            .await?;
    if let Some(path) = output {
        std::fs::write(path, body)?;
        Ok(None)
    } else {
        Ok(Some(body))
    }
}

async fn list_objects(
    data_dir: &Path,
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
    let (_, body) =
        crowdb_console_shared::ops::s3::request(data_dir, Method::GET, Some(bucket), None, &query, None)
            .await?;
    Ok(Some(body))
}
