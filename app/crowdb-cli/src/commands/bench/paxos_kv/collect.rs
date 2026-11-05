// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{args::PaxosKvArgs, client::fetch, deployment::ServerProcess};
use serde_json::{json, Value};

pub(super) async fn sample(args: &PaxosKvArgs, nodes: &[ServerProcess]) -> Result<Value, String> {
    let start = chrono::Utc::now();
    let metrics = futures::future::try_join_all(args.endpoints.iter().map(|e| fetch(e, "/metrics"))).await?;
    let resources: Vec<_> = nodes
        .iter()
        .map(ServerProcess::sample)
        .collect::<Result<_, _>>()?;
    Ok(json!({"started_at":start,"finished_at":chrono::Utc::now(),"metrics":metrics,"resources":resources}))
}

pub(super) use super::counters::{deltas, sentinel};
