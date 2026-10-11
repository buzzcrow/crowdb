// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Access listener termination and coordinated protocol shutdown.

use std::future::Future;
use tokio::sync::watch;

pub type AccessError = Box<dyn std::error::Error>;

/// Stops both S3 listeners when either listener or its storage manager exits.
///
/// # Errors
/// Returns listener errors or an unexpected storage-manager termination.
pub async fn serve_s3<D, H, F>(data: D, health: H, failure: F) -> Result<(), AccessError>
where
    D: Future<Output = std::io::Result<()>>,
    H: Future<Output = std::io::Result<()>>,
    F: Future<Output = ()>,
{
    tokio::select! {
        result = data => result.map_err(Into::into),
        result = health => result.map_err(Into::into),
        () = failure => Err("S3 small-write manager stopped unexpectedly".into()),
    }
}

/// Drains protocol listeners after the first protocol terminates.
///
/// # Errors
/// Preserves the first protocol's error and the original peer-drain ordering.
pub async fn run_pair<S, I>(s3: S, iceberg: I, shutdown_tx: watch::Sender<bool>) -> Result<(), AccessError>
where
    S: Future<Output = Result<(), AccessError>>,
    I: Future<Output = Result<(), AccessError>>,
{
    tokio::pin!(s3, iceberg);
    tokio::select! {
        result = &mut s3 => {
            let _ = shutdown_tx.send(true);
            let other = iceberg.await;
            result?;
            other?;
        }
        result = &mut iceberg => {
            let _ = shutdown_tx.send(true);
            let other = s3.await;
            result?;
            other?;
        }
    }
    Ok(())
}

/// Drains protocol listeners after the first protocol terminates.
///
/// # Errors
/// Preserves the first protocol's error and the original peer-drain ordering.
pub async fn run_three<S, I, D>(
    s3: S,
    iceberg: I,
    dataset: D,
    shutdown_tx: watch::Sender<bool>,
) -> Result<(), AccessError>
where
    S: Future<Output = Result<(), AccessError>>,
    I: Future<Output = Result<(), AccessError>>,
    D: Future<Output = Result<(), AccessError>>,
{
    tokio::pin!(s3, iceberg, dataset);
    tokio::select! {
        result = &mut s3 => {
            let _ = shutdown_tx.send(true);
            let _ = dataset.await;
            let other = iceberg.await;
            result?;
            other?;
        }
        result = &mut iceberg => {
            let _ = shutdown_tx.send(true);
            let _ = dataset.await;
            let other = s3.await;
            result?;
            other?;
        }
        result = &mut dataset => {
            let _ = shutdown_tx.send(true);
            let other_s3 = s3.await;
            let other_iceberg = iceberg.await;
            result?;
            other_s3?;
            other_iceberg?;
        }
    }
    Ok(())
}
