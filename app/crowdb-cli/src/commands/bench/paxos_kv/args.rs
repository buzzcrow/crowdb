// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;

use super::super::workload::LoadOptions;

#[derive(Args, Debug, Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct PaxosKvArgs {
    #[arg(long, default_value = "kv", global = true)]
    pub label: String,
    /// Management origins of all three running nodes; repeat three times.
    #[arg(long = "endpoint", global = true, value_parser = endpoint)]
    pub endpoints: Vec<String>,
    /// Local running containers in the same order as endpoints (host network).
    #[arg(long = "container", global = true, conflicts_with = "server_pids")]
    pub containers: Vec<String>,
    /// Native diagnostic: three existing server PIDs in separate cgroups.
    #[arg(long = "server-pid", global = true, conflicts_with = "containers")]
    pub server_pids: Vec<u32>,
    /// Snapshot success > 0, failure = 0, elections = 0 over the complete load.
    #[arg(long, global = true)]
    pub require_snapshot: bool,
    #[arg(long, global = true)]
    pub expect_window: Option<u64>,
    #[arg(long, global = true)]
    pub expect_coalesce: Option<u64>,
    /// 70% 64B, 20% 1KiB, 10% 16KiB, assigned deterministically by key ID.
    #[arg(long, global = true)]
    pub mixed_values: bool,
    #[arg(long, default_value_t = 0, global = true)]
    pub store: u64,
    /// Existing workload group. Group 0 is reserved for control-plane data.
    #[arg(long, default_value_t = 1, global = true)]
    pub group: u64,
    #[arg(long, default_value_t = 1_000_000, global = true)]
    pub key_space: u64,
    #[arg(long, default_value_t = 64, global = true)]
    pub value_size: usize,
    #[arg(long, default_value_t = 2 * 1024 * 1024 * 1024, global = true)]
    pub max_dataset_bytes: u64,
    /// Upper bound on concurrent materialized scan responses in the host CLI.
    #[arg(long, default_value_t = 512 * 1024 * 1024, global = true)]
    pub max_inflight_bytes: u64,
    #[arg(long, default_value_t = 4, global = true)]
    pub connections: usize,
    #[arg(long, default_value_t = 2, global = true)]
    pub rpc_workers: u32,
    #[arg(long, default_value_t = false, global = true)]
    pub event_write: bool,
    #[arg(long, default_value_t = 4096, global = true)]
    pub send_queue_capacity: u32,
    #[arg(long, default_value_t = 1, global = true)]
    pub seed: u64,
    #[arg(long, value_enum, default_value_t = ReadMode::Linearizable, global = true)]
    pub read_mode: ReadMode,
    #[arg(long, value_enum, default_value_t = ReadEndpoint::Leader, global = true)]
    pub read_endpoint_policy: ReadEndpoint,
    #[arg(long, default_value_t = 100, global = true)]
    pub scan_limit: u32,
    #[arg(long, default_value = "", global = true)]
    pub scan_start_after: String,
    #[arg(long, default_value = "", global = true)]
    pub scan_prefix: String,
    #[arg(long, default_value_t = false, global = true)]
    pub reverse: bool,
    #[command(flatten)]
    pub load: LoadOptions,
    #[command(subcommand)]
    pub operation: Operation,
}

#[derive(Subcommand, Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// Populate and verify the bounded dataset; no performance verdict.
    Prepare,
    Read,
    Write,
    Scan,
}

#[derive(ValueEnum, Debug, Clone, Copy, Serialize)]
pub enum ReadMode {
    Linearizable,
    Minslot,
}

#[derive(ValueEnum, Debug, Clone, Copy, Serialize)]
pub enum ReadEndpoint {
    Leader,
    AnyReplica,
}

fn endpoint(value: &str) -> Result<String, String> {
    let url = reqwest::Url::parse(value).map_err(|error| error.to_string())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.port() == Some(0)
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err("endpoint must be an HTTP(S) management origin without credentials or path".into());
    }
    Ok(value.trim_end_matches('/').to_owned())
}

impl PaxosKvArgs {
    pub(super) fn dataset_bytes(&self) -> Option<u64> {
        let values = if self.mixed_values {
            let complete = (self.key_space / 10).checked_mul(7 * 64 + 2 * 1024 + 16384)?;
            let rest = self.key_space % 10;
            complete.checked_add(rest.min(7) * 64 + rest.saturating_sub(7) * 1024)?
        } else {
            self.key_space.checked_mul(self.value_size as u64)?
        };
        values.checked_add(self.key_space.checked_mul(21)?)
    }
}
