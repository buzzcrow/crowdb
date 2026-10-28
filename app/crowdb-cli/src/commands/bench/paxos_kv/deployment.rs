// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read-only deployment evidence and cgroup v2 resource accounting.
mod memory;
mod native;
use super::args::PaxosKvArgs;
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::PathBuf, time::Duration};

const MEMORY_LIMIT_PERCENT: u128 = 90;

pub(super) struct ServerProcess {
    pub evidence: Value,
    cgroup: PathBuf,
}
impl ServerProcess {
    pub fn sample(&self) -> Result<Value, String> {
        let read =
            |name| std::fs::read_to_string(self.cgroup.join(name)).map_err(|e| format!("cgroup {name}: {e}"));
        Ok(json!({"at":chrono::Utc::now(),"cpu_stat":read("cpu.stat")?,
            "memory_current":read("memory.current")?,"memory_peak":read("memory.peak").ok(),
            "memory_events":read("memory.events")?,"cpu_pressure":read("cpu.pressure").ok(),
            "memory_pressure":read("memory.pressure").ok()}))
    }
}

fn memory(node: &ServerProcess) -> Result<(Option<u64>, u64), String> {
    let read = |file| -> Result<String, String> {
        Ok(std::fs::read_to_string(node.cgroup.join(file))
            .map_err(|e| format!("cgroup {file}: {e}"))?
            .trim()
            .to_owned())
    };
    Ok((
        memory::parse_limit(&read("memory.max")?)?,
        read("memory.current")?
            .parse()
            .map_err(|_| "invalid cgroup memory.current")?,
    ))
}

pub(super) fn validate_budget(args: &PaxosKvArgs, nodes: &[ServerProcess]) -> Result<(), String> {
    let payload = args.dataset_bytes().ok_or("dataset size overflow")?;
    for node in nodes {
        let (limit, current) = memory(node)?;
        let Some(limit) = limit else { continue };
        // Startup estimate for replicas' WAL, Tree and working buffers;
        // the runtime guard remains necessary because write amplification is variable.
        if (u128::from(current) + u128::from(payload) * 2) * 100 > u128::from(limit) * MEMORY_LIMIT_PERCENT {
            return Err(format!("{}: dataset {payload} bytes exceeds memory budget: current={current}, limit={limit}; reduce --key-space or --value-size", node.evidence["name"]));
        }
    }
    Ok(())
}

pub(super) async fn memory_guard(nodes: &[ServerProcess]) -> String {
    loop {
        for node in nodes {
            match memory(node) {
                Ok((None, _)) => {}
                Ok((Some(limit), current))
                    if u128::from(current) * 100 < u128::from(limit) * MEMORY_LIMIT_PERCENT => {}
                Ok((Some(limit), current)) => {
                    return format!(
                        "{}: memory guard stopped load at {current}/{limit} bytes ({MEMORY_LIMIT_PERCENT}% limit)",
                        node.evidence["name"]
                    )
                }
                Err(error) => return error,
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(super) async fn inspect(args: &PaxosKvArgs) -> Result<Vec<ServerProcess>, String> {
    if !args.server_pids.is_empty() {
        return native::inspect(args);
    }
    if args.containers.len() != 3 || args.containers.iter().collect::<BTreeSet<_>>().len() != 3 {
        return Err("provide three distinct --container names in endpoint order".into());
    }
    let mut result = Vec::new();
    for (name, endpoint) in args.containers.iter().zip(&args.endpoints) {
        if name.starts_with('-') {
            return Err("invalid container name".into());
        }
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new("docker")
                // Instrument the load client/server, not the Docker Go process.
                .env_remove("LD_PRELOAD")
                .env_remove("ASAN_OPTIONS")
                .args(["inspect", name])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| "docker inspect timeout")?
        .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned());
        }
        let data: Vec<Value> = serde_json::from_slice(&output.stdout).map_err(|e| e.to_string())?;
        let node = data.first().ok_or("container absent")?;
        if node["State"]["Running"] != true || node["HostConfig"]["NetworkMode"] != "host" {
            return Err(format!(
                "{name}: benchmark requires a running host-network container"
            ));
        }
        let cmd = node["Config"]["Cmd"]
            .as_array()
            .ok_or("container has no command")?;
        let flag = |name: &str| cmd.windows(2).find(|v| v[0] == name).and_then(|v| v[1].as_str());
        if flag("--kv-backend") != Some("mem-block") || flag("--wal-backend") != Some("mem-block") {
            return Err("benchmark requires explicit mem-block KV and WAL backends".into());
        }
        for (expected, name) in [
            (args.expect_window, "--max-inflight"),
            (args.expect_coalesce, "--coalesce-max-keys"),
        ] {
            if let Some(expected) = expected {
                if flag(name).and_then(|s| s.parse::<u64>().ok()) != Some(expected) {
                    return Err(format!("container {name} does not match expected {expected}"));
                }
            }
        }
        let origin = reqwest::Url::parse(endpoint).map_err(|e| e.to_string())?;
        if !matches!(origin.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")) {
            return Err("local host-network benchmark endpoints must use loopback".into());
        }
        if flag("--management-port").and_then(|v| v.parse::<u16>().ok()) != origin.port_or_known_default() {
            return Err(format!(
                "{name}: endpoint port does not match the container command"
            ));
        }
        let pid = node["State"]["Pid"].as_u64().ok_or("missing container PID")?;
        let groups = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).map_err(|e| e.to_string())?;
        let path = groups
            .lines()
            .find_map(|l| l.strip_prefix("0::/"))
            .ok_or("cgroup v2 is required")?;
        let cgroup = PathBuf::from("/sys/fs/cgroup").join(path);
        result.push(ServerProcess {
            cgroup,
            evidence: json!({"kind":"container","name":name,"id":node["Id"],"image":node["Image"],
            "image_labels":node["Config"]["Labels"],"command":cmd,
            "pid":pid,"started_at":node["State"]["StartedAt"],"endpoint":endpoint,
            "network":"host","memory_limit":node["HostConfig"]["Memory"],
            "nano_cpus":node["HostConfig"]["NanoCpus"],"cpu_quota":node["HostConfig"]["CpuQuota"],
            "cpu_period":node["HostConfig"]["CpuPeriod"],"cpuset":node["HostConfig"]["CpusetCpus"]}),
        });
    }
    Ok(result)
}
