// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP-only application checks, usable without a local container runtime.

use crate::Cli;
use crowdb_console_shared::{deployment::target, ops::cluster::readiness};
use crowdb_protocol::mgmt::cluster::{ClusterPhase, ClusterStatus};
use std::{
    process::ExitCode,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

pub(crate) async fn run(cli: &Cli, json: bool, check: bool, wait: u64) -> ExitCode {
    match observe(cli, check, wait).await {
        Ok(report) => {
            if json {
                match serde_json::to_string_pretty(&report) {
                    Ok(output) => println!("{output}"),
                    Err(error) => return failure(error),
                }
            } else {
                println!(
                    "cluster {}: {:?} (UUID {})",
                    report.observation.cluster,
                    report.phase,
                    report.observation.cluster_id.as_deref().unwrap_or("unbound")
                );
                for reason in &report.reasons {
                    println!("  {}: {}", reason.dependency, reason.code);
                }
            }
            if check && report.phase != ClusterPhase::Ready {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(error) => failure(error),
    }
}

async fn observe(cli: &Cli, check: bool, wait: u64) -> Result<ClusterStatus, String> {
    let binding = cli.cluster.as_deref().map(super::target::load).transpose()?;
    let origins = target::management_origins(if cli.management_endpoints.is_empty() {
        binding
            .as_ref()
            .map_or(&[], |binding| binding.management_endpoints.as_slice())
    } else {
        &cli.management_endpoints
    })
    .map_err(|error| error.to_string())?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| error.to_string())?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(if check && wait > 0 { wait } else { 12 }))
        .ok_or("wait exceeds the supported clock range")?;
    loop {
        let replies =
            futures::future::join_all(origins.iter().map(|origin| read(&client, origin, deadline))).await;
        let mut selected: Option<ClusterStatus> = None;
        for reply in replies {
            let report = reply?;
            if binding.as_ref().is_some_and(|binding| {
                report.observation.cluster_id.as_ref() != Some(&binding.cluster_id)
                    || report.observation.cluster != binding.name
            }) || selected.as_ref().is_some_and(|seen| {
                seen.observation.cluster_id != report.observation.cluster_id
                    || seen.observation.cluster != report.observation.cluster
            }) {
                return Err("selected cluster and management endpoints disagree on identity".into());
            }
            if selected
                .as_ref()
                .map_or(true, |seen| rank(report.phase) > rank(seen.phase))
            {
                selected = Some(report);
            }
        }
        let selected = selected.ok_or("select --cluster or --management-endpoint")?;
        if !check || wait == 0 || selected.phase == ClusterPhase::Ready || Instant::now() >= deadline {
            return Ok(selected);
        }
        tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(250))).await;
        if Instant::now() >= deadline {
            return Ok(selected);
        }
    }
}

async fn read(client: &reqwest::Client, origin: &str, deadline: Instant) -> Result<ClusterStatus, String> {
    tokio::time::timeout_at(deadline.min(Instant::now() + Duration::from_secs(12)), async {
        let mut reply = client
            .get(format!("{origin}/api/cluster/status"))
            .send()
            .await
            .map_err(|_| "cluster status endpoint unavailable")?
            .error_for_status()
            .map_err(|_| "cluster status endpoint rejected observation")?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reply
            .chunk()
            .await
            .map_err(|_| "cluster status response interrupted")?
        {
            if bytes.len() + chunk.len() > 1024 * 1024 {
                return Err("cluster status exceeds its size bound".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let report: ClusterStatus =
            serde_json::from_slice(&bytes).map_err(|_| "invalid cluster status report")?;
        let observation = report.observation;
        if observation.version != 1
            || !matches!(observation.initial_voters, 1 | 3)
            || observation.cluster.is_empty()
            || observation.policy.as_ref().map_or(true, |policy| {
                policy.validate().is_err()
                    || policy.cluster != observation.cluster
                    || policy.initial_voters != observation.initial_voters
            })
            || (observation.bound && observation.cluster_id.is_none())
        {
            return Err("invalid cluster status identity or policy".into());
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX));
        let report = readiness::reduce(observation, now);
        if report
            .reasons
            .iter()
            .any(|reason| reason.code == "invalid-observation")
        {
            return Err("invalid cluster status observation".into());
        }
        Ok(report)
    })
    .await
    .map_err(|_| "cluster status observation deadline elapsed".to_owned())?
}

fn rank(phase: ClusterPhase) -> u8 {
    match phase {
        ClusterPhase::Ready => 0,
        ClusterPhase::Waiting => 1,
        ClusterPhase::Initializing => 2,
        ClusterPhase::Degraded => 3,
        ClusterPhase::Unavailable => 4,
        ClusterPhase::Failed => 5,
    }
}
fn failure(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("error: cluster observation: {error}");
    ExitCode::from(2)
}
