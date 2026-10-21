// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP-only application checks, usable without a local container runtime.

use crate::Cli;
use crowdb_console_shared::{deployment::target, ops::cluster::readiness};
use crowdb_protocol::mgmt::cluster::{
    ClusterPhase, ClusterStatus, DependencyKind, DependencyState, ReadinessReason,
};
use std::{
    process::ExitCode,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

pub(crate) async fn run(cli: &Cli, json: bool, check: bool, wait: u64, timeout: u64) -> ExitCode {
    match observe(cli, check, wait, timeout).await {
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

async fn observe(cli: &Cli, check: bool, wait: u64, timeout: u64) -> Result<ClusterStatus, String> {
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
    let budget = Duration::from_secs(timeout);
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(if check && wait > 0 {
            wait
        } else {
            timeout
        }))
        .ok_or("wait exceeds the supported clock range")?;
    let mut last = None;
    loop {
        let request_deadline = deadline.min(
            Instant::now()
                .checked_add(budget)
                .ok_or("timeout exceeds the supported clock range")?,
        );
        last = round(&client, &origins, binding.as_ref(), request_deadline, last).await?;
        if last
            .as_ref()
            .is_some_and(|report| report.phase == ClusterPhase::Ready)
            || !check
            || wait == 0
            || Instant::now() >= deadline
        {
            return last.ok_or_else(|| "cluster status observation unavailable within its deadline".into());
        }
        tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(250))).await;
        if Instant::now() >= deadline {
            return last.ok_or_else(|| "cluster status observation unavailable within its deadline".into());
        }
    }
}

async fn round(
    client: &reqwest::Client,
    origins: &[String],
    binding: Option<&target::ClusterTargetBinding>,
    deadline: Instant,
    last: Option<ClusterStatus>,
) -> Result<Option<ClusterStatus>, String> {
    let replies =
        futures::future::join_all(origins.iter().map(|origin| read(client, origin, deadline))).await;
    let mut selected: Option<ClusterStatus> = None;
    let mut missing = false;
    for reply in replies {
        let report = match reply {
            Ok(report) => report,
            Err(ReadError::Unavailable) => {
                missing = true;
                continue;
            }
            Err(ReadError::Invalid(reason)) => return Err(reason.into()),
        };
        if binding.is_some_and(|binding| {
            report.observation.cluster_id.as_ref() != Some(&binding.cluster_id)
                || report.observation.cluster != binding.name
        }) || selected.as_ref().or(last.as_ref()).is_some_and(|seen| {
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
    let selected = selected.or(last);
    Ok(if missing {
        selected.map(unavailable)
    } else {
        selected
    })
}

fn unavailable(mut report: ClusterStatus) -> ClusterStatus {
    let now = now_ms();
    for fact in &mut report.observation.dependencies {
        if fact.kind == DependencyKind::Authority {
            fact.state = DependencyState::Unknown;
            fact.observed_at_unix_ms = now;
            fact.reason = Some(ReadinessReason {
                dependency: fact.id.clone(),
                code: "management-origin-unavailable".into(),
                detail: "A selected management origin did not provide a current observation".into(),
            });
        }
    }
    readiness::reduce(report.observation, now)
}

enum ReadError {
    Unavailable,
    Invalid(&'static str),
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
}

async fn read(client: &reqwest::Client, origin: &str, deadline: Instant) -> Result<ClusterStatus, ReadError> {
    tokio::time::timeout_at(deadline, async {
        let mut reply = client
            .get(format!("{origin}/api/cluster/status"))
            .send()
            .await
            .map_err(|_| ReadError::Unavailable)?
            .error_for_status()
            .map_err(|_| ReadError::Unavailable)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reply.chunk().await.map_err(|_| ReadError::Unavailable)? {
            if bytes.len() + chunk.len() > 1024 * 1024 {
                return Err(ReadError::Invalid("cluster status exceeds its size bound"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let report: ClusterStatus = serde_json::from_slice(&bytes)
            .map_err(|_| ReadError::Invalid("invalid cluster status report"))?;
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
            return Err(ReadError::Invalid("invalid cluster status identity or policy"));
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
            return Err(ReadError::Invalid("invalid cluster status observation"));
        }
        Ok(report)
    })
    .await
    .map_err(|_| ReadError::Unavailable)?
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
