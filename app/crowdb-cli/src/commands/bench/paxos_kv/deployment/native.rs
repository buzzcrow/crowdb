// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{json, BTreeSet, PathBuf, PaxosKvArgs, ServerProcess};

pub(super) fn inspect(args: &PaxosKvArgs) -> Result<Vec<ServerProcess>, String> {
    if !args.containers.is_empty()
        || args.server_pids.len() != 3
        || args.endpoints.len() != 3
        || args.server_pids.iter().collect::<BTreeSet<_>>().len() != 3
    {
        return Err("native diagnostic requires three distinct --server-pid values and three endpoints, without --container".into());
    }
    let mut nodes = Vec::new();
    let mut cgroups = BTreeSet::new();
    for (&pid, endpoint) in args.server_pids.iter().zip(&args.endpoints) {
        let node = inspect_process(args, pid, endpoint)?;
        if !cgroups.insert(node.cgroup.clone()) {
            return Err("native server PIDs must have separate cgroups".into());
        }
        nodes.push(node);
    }
    Ok(nodes)
}

fn inspect_process(args: &PaxosKvArgs, pid: u32, endpoint: &str) -> Result<ServerProcess, String> {
    let proc = PathBuf::from(format!("/proc/{pid}"));
    let read = |name| std::fs::read_to_string(proc.join(name)).map_err(|e| format!("PID {pid}/{name}: {e}"));
    let stat = read("stat")?;
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .ok_or("invalid process stat")?
        .1
        .split_whitespace()
        .collect();
    if matches!(fields.first(), Some(&"Z" | &"X")) {
        return Err(format!("PID {pid} has exited"));
    }
    let started = fields.get(19).ok_or("missing process start time")?;
    let exe = std::fs::read_link(proc.join("exe")).map_err(|e| e.to_string())?;
    if exe.file_name().and_then(|n| n.to_str()) != Some("crowdb-paxos-kv-server") {
        return Err(format!("PID {pid} is not crowdb-paxos-kv-server"));
    }
    let command: Vec<String> = read("cmdline")?
        .split('\0')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    validate_command(args, &command, endpoint)?;
    let group = read("cgroup")?;
    let relative = group
        .lines()
        .find_map(|l| l.strip_prefix("0::/"))
        .ok_or("cgroup v2 required")?;
    if relative.is_empty() {
        return Err("native server requires a dedicated cgroup".into());
    }
    let cgroup = PathBuf::from("/sys/fs/cgroup").join(relative);
    let policy = |file| std::fs::read_to_string(cgroup.join(file)).map_err(|e| format!("cgroup {file}: {e}"));
    let pids = policy("cgroup.procs")?;
    if pids.split_whitespace().collect::<Vec<_>>() != [pid.to_string()] {
        return Err(format!("PID {pid}: cgroup must contain only this server process"));
    }
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(|e| e.to_string())?;
    // User services may not have the cpuset controller delegated. The process
    // status still reports the effective CPU affinity inherited from ancestors.
    let status = read("status")?;
    let affinity = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .ok_or("missing process CPU affinity")?
        .trim();
    let evidence = json!({"kind":"native-process","name":format!("pid-{pid}"),
        "id":format!("{}:{pid}:{started}",boot.trim()),"started_at":started,
        "pid":pid,"exe":exe,"command":command,"endpoint":endpoint,"network":"host",
        "cgroup":cgroup,"memory_limit":policy("memory.max")?,"cpu_max":policy("cpu.max")?,
        "cpuset":std::fs::read_to_string(cgroup.join("cpuset.cpus.effective")).ok(),
        "cpu_affinity_list":affinity});
    Ok(ServerProcess { evidence, cgroup })
}

fn validate_command(args: &PaxosKvArgs, command: &[String], endpoint: &str) -> Result<(), String> {
    let flag = |name: &str| command.windows(2).find(|v| v[0] == name).map(|v| v[1].as_str());
    for name in ["--kv-backend", "--wal-backend"] {
        if flag(name) != Some("mem-block") {
            return Err("native benchmark requires explicit mem-block KV and WAL backends".into());
        }
    }
    for (expected, name) in [
        (args.expect_window, "--max-inflight"),
        (args.expect_coalesce, "--coalesce-max-keys"),
    ] {
        if let Some(expected) = expected {
            if flag(name).and_then(|s| s.parse::<u64>().ok()) != Some(expected) {
                return Err(format!("native server {name} does not match expected {expected}"));
            }
        }
    }
    let origin = reqwest::Url::parse(endpoint).map_err(|e| e.to_string())?;
    if !matches!(origin.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"))
        || flag("--management-port").and_then(|p| p.parse::<u16>().ok()) != origin.port_or_known_default()
    {
        return Err("native endpoint must match the server's loopback management port".into());
    }
    Ok(())
}
