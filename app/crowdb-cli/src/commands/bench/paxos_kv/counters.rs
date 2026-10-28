// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use serde_json::{json, Value};

pub fn counter(node: &Value, name: &str) -> Result<f64, String> {
    node["metrics"]
        .as_array()
        .ok_or("metrics response has no points")?
        .iter()
        .find(|v| v["name"] == name)
        .and_then(|v| v["fields"].as_array())
        .and_then(|f| f.iter().find(|v| v["key"] == "total"))
        .and_then(|v| v["value"].as_f64())
        .ok_or_else(|| format!("missing required cumulative counter {name}"))
}

pub fn sentinel(store: u64, group: u64, before: &Value, after: &Value) -> Result<Value, String> {
    let mut sums = [0.0; 3];
    for (i, suffix) in [
        "paxos.elections.c",
        "maintenance.snapshot.success.c",
        "maintenance.snapshot.failure.c",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("s.{store}.g.{group}.{suffix}");
        for node in 0..3 {
            let delta = counter(&after["metrics"][node], &name)? - counter(&before["metrics"][node], &name)?;
            if delta < 0.0 {
                return Err(format!("counter reset: {name}"));
            }
            sums[i] += delta;
        }
    }
    Ok(
        json!({"elections":sums[0],"snapshot_completed":sums[1],"snapshot_failed":sums[2],
        "passed":sums[0]==0.0 && sums[1]>0.0 && sums[2]==0.0,
        "window":"entire load including warmup, tail and drain; preparation excluded"}),
    )
}

fn stat(sample: &Value, file: &str, key: &str) -> Result<u64, String> {
    sample[file]
        .as_str()
        .and_then(|s| {
            s.lines().find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next() == Some(key))
                    .then(|| fields.next()?.parse().ok())
                    .flatten()
            })
        })
        .ok_or_else(|| format!("missing cgroup {file}/{key}"))
}

#[allow(clippy::cast_precision_loss)]
pub fn deltas(before: &Value, after: &Value) -> Result<Value, String> {
    let mut nodes = Vec::new();
    for index in 0..3 {
        let mut counters = serde_json::Map::new();
        let points = before["metrics"][index]["metrics"]
            .as_array()
            .ok_or("metrics missing")?;
        for point in points {
            let Some(name) = point["name"]
                .as_str()
                .filter(|s| s.rsplit('.').next() == Some("c"))
            else {
                continue;
            };
            let delta = counter(&after["metrics"][index], name)? - counter(&before["metrics"][index], name)?;
            if delta < 0.0 {
                return Err(format!("counter reset: {name}"));
            }
            counters.insert(name.into(), json!(delta));
        }
        let b = &before["resources"][index];
        let a = &after["resources"][index];
        let elapsed = (chrono::DateTime::parse_from_rfc3339(a["at"].as_str().ok_or("sample timestamp")?)
            .map_err(|e| e.to_string())?
            - chrono::DateTime::parse_from_rfc3339(b["at"].as_str().ok_or("sample timestamp")?)
                .map_err(|e| e.to_string())?)
        .num_microseconds()
        .ok_or("invalid sample interval")?;
        if elapsed <= 0 {
            return Err("nonpositive resource interval".into());
        }
        let delta = |file, key| {
            stat(a, file, key)?
                .checked_sub(stat(b, file, key)?)
                .ok_or_else(|| format!("cgroup counter reset {file}/{key}"))
        };
        let usage = delta("cpu_stat", "usage_usec")?;
        let oom = delta("memory_events", "oom")?;
        let oom_kill = delta("memory_events", "oom_kill")?;
        if oom > 0 || oom_kill > 0 {
            return Err("container memory exhaustion during load".into());
        }
        nodes.push(json!({"counters":counters,"cpu_usage_usec":usage,"cpu_percent":usage as f64/elapsed as f64*100.0,
            "cpu_throttled_usec":delta("cpu_stat","throttled_usec")?,"memory_current":a["memory_current"],
            "memory_peak":a["memory_peak"],"memory_peak_scope":"container lifetime", "sample_elapsed_us":elapsed}));
    }
    Ok(json!(nodes))
}
