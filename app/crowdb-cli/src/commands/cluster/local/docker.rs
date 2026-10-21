// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact local Docker ownership; names alone never authorize an effect.

use super::state::{invalid, DockerIdentity, Launch};
use crowdb_console_shared::error::Result;
use serde_json::Value;
use std::time::Duration;

pub(super) const CLUSTER: &str = "org.crowdb.cluster";
pub(super) const OWNER: &str = "org.crowdb.cluster.owner";
pub(super) struct Docker;

impl Docker {
    pub async fn call(&self, args: &[String]) -> Result<String> {
        let program = std::env::var_os("CROWDB_CONTAINER_RUNTIME").unwrap_or_else(|| "docker".into());
        let output = tokio::time::timeout(
            Duration::from_secs(60),
            tokio::process::Command::new(program)
                .args(args)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| invalid("Docker command exceeded its 60-second bound"))??;
        if !output.status.success() {
            return Err(invalid(&format!(
                "Docker {}: {}",
                args.first().map_or("operation", String::as_str),
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        if output.stdout.len() > 16 * 1024 * 1024 {
            return Err(invalid("Docker observation exceeds its size bound"));
        }
        String::from_utf8(output.stdout)
            .map(|output| output.trim().into())
            .map_err(|_| invalid("invalid Docker output"))
    }
    pub async fn identity(&self) -> Result<DockerIdentity> {
        let context = self.call(&strings(&["context", "show"])).await?;
        let details: Vec<Value> =
            serde_json::from_str(&self.call(&strings(&["context", "inspect", &context])).await?)
                .map_err(|_| invalid("invalid Docker context observation"))?;
        let endpoint = std::env::var("DOCKER_HOST")
            .ok()
            .or_else(|| {
                details
                    .first()?
                    .get("Endpoints")?
                    .get("docker")?
                    .get("Host")?
                    .as_str()
                    .map(String::from)
            })
            .ok_or_else(|| invalid("Docker context has no endpoint"))?;
        let daemon = self.call(&strings(&["info", "--format", "{{.ID}}"])).await?;
        Ok(DockerIdentity {
            daemon,
            context,
            endpoint,
        })
    }
    pub async fn check(&self, record: &Launch) -> Result<()> {
        if self.identity().await? != record.runtime {
            return Err(invalid(
                "Docker endpoint/context differs from accepted launch inputs",
            ));
        }
        Ok(())
    }
    pub async fn objects(&self, kind: &str) -> Result<Vec<Value>> {
        let args = if kind == "container" {
            strings(&["ps", "-aq"])
        } else {
            strings(&[kind, "ls", "-q"])
        };
        let ids = self.call(&args).await?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut args = strings(&[kind, "inspect"]);
        args.extend(ids.lines().map(String::from));
        if args.len() > 4098 {
            return Err(invalid("Docker resource inventory exceeds bound"));
        }
        serde_json::from_str(&self.call(&args).await?)
            .map_err(|_| invalid("invalid Docker resource observation"))
    }
    pub async fn inspect(&self, kind: &str, identity: &str) -> Result<Value> {
        let mut values: Vec<Value> =
            serde_json::from_str(&self.call(&strings(&[kind, "inspect", identity])).await?)
                .map_err(|_| invalid("invalid Docker resource observation"))?;
        if values.len() != 1 {
            return Err(invalid("Docker resource observation is not unique"));
        }
        Ok(values.remove(0))
    }
    pub async fn ensure(&self, record: &Launch, kind: &str, name: &str) -> Result<Value> {
        if let Some(item) = self
            .objects(kind)
            .await?
            .into_iter()
            .find(|item| object_name(item) == name)
        {
            require_owned(&item, record, kind)?;
            return Ok(item);
        }
        self.check(record).await?;
        let mut args = strings(&[kind, "create"]);
        args.extend(labels(record));
        args.push(name.into());
        self.call(&args).await?;
        let item = self.inspect(kind, name).await?;
        require_owned(&item, record, kind)?;
        Ok(item)
    }
}

pub(super) fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).into()).collect()
}
pub(super) fn labels(record: &Launch) -> Vec<String> {
    vec![
        "--label".into(),
        format!("{CLUSTER}={}", record.cluster),
        "--label".into(),
        format!("{OWNER}={}", record.owner),
    ]
}
pub(super) fn object_name(item: &Value) -> &str {
    item["Name"].as_str().unwrap_or_default().trim_start_matches('/')
}
pub(super) fn owned(item: &Value, record: &Launch, kind: &str) -> bool {
    let labels = if kind == "container" {
        &item["Config"]["Labels"]
    } else {
        &item["Labels"]
    };
    labels[CLUSTER] == record.cluster
        && labels[OWNER] == record.owner
        && (if kind == "network" {
            object_name(item) == record.resource_name(kind, 0)
        } else {
            (1..=record.nodes).any(|index| object_name(item) == record.resource_name(kind, index))
        })
}
pub(super) fn require_owned(item: &Value, record: &Launch, kind: &str) -> Result<()> {
    if !owned(item, record, kind) {
        return Err(invalid(
            "Docker resource ownership differs from accepted cluster inputs",
        ));
    }
    Ok(())
}
