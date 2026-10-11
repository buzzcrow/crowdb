// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::PreviewError;
use crate::{DeploymentProfile, MonitorEvent, MonitorEventKind, Supervisor};
use std::future::Future;
use std::time::Duration;
use tokio::time::{sleep, Instant};

pub(super) async fn verify_bootstrap_probe(
    supervisor: &mut Supervisor,
    name: &'static str,
    probe: impl Future<Output = Result<(), PreviewError>>,
) -> Result<(), PreviewError> {
    record_bootstrap_probe(supervisor, name, MonitorEventKind::BootstrapStepStarted).await?;
    let result = probe.await;
    let kind = if result.is_ok() {
        MonitorEventKind::BootstrapStepCompleted
    } else {
        MonitorEventKind::BootstrapFailed
    };
    record_bootstrap_probe(supervisor, name, kind).await?;
    result
}

pub(super) async fn record_bootstrap_probe(
    supervisor: &mut Supervisor,
    name: &'static str,
    kind: MonitorEventKind,
) -> Result<(), PreviewError> {
    supervisor
        .monitor_log_mut()
        .record(&MonitorEvent {
            kind,
            service: Some(name),
            pid: None,
            attempt: None,
        })
        .await?;
    Ok(())
}

pub(super) async fn verify_web_authority(
    supervisor: &mut Supervisor,
    profile: &DeploymentProfile,
) -> Result<(), PreviewError> {
    let web = profile
        .services
        .iter()
        .find(|service| service.id == "web")
        .ok_or(PreviewError::Invalid("Web service is absent"))?;
    let origin = web
        .probe
        .target
        .strip_suffix("/healthz")
        .ok_or(PreviewError::Invalid("Web health endpoint is incompatible"))?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| PreviewError::WebAuthority("cannot construct authority probe"))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_status_refresh = Instant::now();
    loop {
        if last_status_refresh.elapsed() >= Duration::from_secs(5) {
            supervisor.refresh_status()?;
            last_status_refresh = Instant::now();
        }
        let response = client
            .get(format!("{origin}/api/authority"))
            .send()
            .await
            .map_err(|_| PreviewError::WebAuthority("authority endpoint is unavailable"))?;
        let status = response.status();
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| PreviewError::WebAuthority("authority response is invalid"))?;
        if status.is_success() {
            if body.get("source").and_then(serde_json::Value::as_str) == Some("group0")
                && body.get("available").and_then(serde_json::Value::as_bool) == Some(true)
            {
                return Ok(());
            }
            return Err(PreviewError::WebAuthority("Web is not serving Group 0 authority"));
        }
        let reason = body
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unspecified");
        if reason != "group0_unavailable" || Instant::now() >= deadline {
            return Err(PreviewError::WebAuthorityUnavailable(format!(
                "HTTP {status}: {reason}"
            )));
        }
        sleep(Duration::from_millis(200)).await;
    }
}
