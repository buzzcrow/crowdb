// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workload {
    Append,
    RandomRead,
    Replay,
    Gc,
}

impl FromStr for Workload {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "append" => Ok(Self::Append),
            "random-read" => Ok(Self::RandomRead),
            "replay" => Ok(Self::Replay),
            "gc" => Ok(Self::Gc),
            _ => Err(format!("unknown workload {value}")),
        }
    }
}

impl std::fmt::Display for Workload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Append => "append",
            Self::RandomRead => "random-read",
            Self::Replay => "replay",
            Self::Gc => "gc",
        })
    }
}

#[derive(Clone, Debug)]
pub struct BenchConfig {
    pub workload: Workload,
    pub management_seed: String,
    pub duration_secs: u64,
    pub operations: u64,
    pub object_size: usize,
    pub concurrency: usize,
    pub dataset_bytes: u64,
    pub diskio_connections: usize,
    pub diskio_rpc_workers: u32,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            workload: Workload::Append,
            management_seed: "http://127.0.0.1:10000".into(),
            duration_secs: 10,
            operations: u64::MAX,
            object_size: 4 * 1024,
            concurrency: 1,
            dataset_bytes: 320 * 1024 * 1024,
            diskio_connections: 8,
            diskio_rpc_workers: 4,
        }
    }
}

impl BenchConfig {
    pub fn parse(arguments: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut config = Self::default();
        let mut arguments = arguments;
        while let Some(flag) = arguments.next() {
            if flag == "--bench" {
                continue;
            }
            let value = arguments
                .next()
                .ok_or_else(|| format!("{flag} requires a value"))?;
            match flag.as_str() {
                "--workload" => config.workload = value.parse()?,
                "--management-seed" => config.management_seed = value,
                "--duration-secs" => config.duration_secs = parse(&flag, &value)?,
                "--operations" => config.operations = parse(&flag, &value)?,
                "--object-size" => config.object_size = parse(&flag, &value)?,
                "--concurrency" => config.concurrency = parse(&flag, &value)?,
                "--dataset-bytes" => config.dataset_bytes = parse(&flag, &value)?,
                "--diskio-connections" => config.diskio_connections = parse(&flag, &value)?,
                "--diskio-rpc-workers" => config.diskio_rpc_workers = parse(&flag, &value)?,
                _ => return Err(format!("unknown flag {flag}")),
            }
        }
        if config.duration_secs == 0
            || config.operations == 0
            || config.object_size == 0
            || config.object_size > 64 * 1024 * 1024
            || config.concurrency == 0
            || config.dataset_bytes == 0
            || config.diskio_connections == 0
            || config.diskio_rpc_workers == 0
        {
            return Err("numeric arguments must be nonzero and object-size must not exceed 64 MiB".into());
        }
        Ok(config)
    }
}

fn parse<T: FromStr>(flag: &str, value: &str) -> Result<T, String> {
    value.parse().map_err(|_| format!("invalid {flag} value {value}"))
}
