// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, Subcommand};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as KvClientConfig, CrowdbKvClient};
use futures::{stream, StreamExt};

#[derive(Debug, Parser)]
#[command(name = "crowdb-chunk-kv-cli")]
struct Args {
    #[arg(long = "mgmt-seed", default_value = "http://127.0.0.1:25000")]
    mgmt_seeds: Vec<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Put {
        key: String,
        value: String,
    },
    Get {
        key: String,
    },
    Load {
        #[arg(long, default_value_t = 10_000)]
        operations: u64,
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
        #[arg(long, default_value_t = 256)]
        value_bytes: usize,
        #[arg(long, default_value_t = 10_000)]
        keyspace: u64,
        #[arg(long, default_value = "object")]
        key_prefix: String,
        #[arg(long, default_value_t = 0)]
        key_offset: u64,
        #[arg(long, default_value_t = 0)]
        read_percent: u8,
    },
}

struct LoadConfig {
    operations: u64,
    concurrency: usize,
    value_bytes: usize,
    keyspace: u64,
    key_prefix: String,
    key_offset: u64,
    read_percent: u8,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = match production_client(args.mgmt_seeds) {
        Ok(client) => Arc::new(client),
        Err(error) => exit_error(&error),
    };
    let result = match args.command {
        Command::Put { key, value } => client
            .put(key.into_bytes(), value.into_bytes())
            .await
            .map(|response| println!("{response:?}")),
        Command::Get { key } => client
            .get(key.into_bytes(), None)
            .await
            .map(|response| println!("{response:?}")),
        Command::Load {
            operations,
            concurrency,
            value_bytes,
            keyspace,
            key_prefix,
            key_offset,
            read_percent,
        } => {
            run_load(
                client,
                LoadConfig {
                    operations,
                    concurrency,
                    value_bytes,
                    keyspace,
                    key_prefix,
                    key_offset,
                    read_percent,
                },
            )
            .await
        }
    };
    if let Err(error) = result {
        exit_error(&error.to_string());
    }
}

fn production_client(mgmt_seeds: Vec<String>) -> Result<ChunkKvClient, String> {
    let kv = Arc::new(CrowdbKvClient::new(KvClientConfig::new(mgmt_seeds)));
    let config = ClientConfig::default();
    let catalog = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(kv));
    let transport = Arc::new(ChunkKvRpcTransport::new(config.max_owner_connections, 1, 2));
    ChunkKvClient::new(config, catalog, transport).map_err(|error| error.to_string())
}

async fn run_load(client: Arc<ChunkKvClient>, config: LoadConfig) -> crowdb_chunk_kv_client::Result<()> {
    let LoadConfig {
        operations,
        concurrency,
        value_bytes,
        keyspace,
        key_prefix,
        key_offset,
        read_percent,
    } = config;
    if operations == 0 || concurrency == 0 || value_bytes == 0 || keyspace == 0 || read_percent > 100 {
        return Err(crowdb_chunk_kv_client::ClientError::InvalidRequest(
            "load bounds must be nonzero and read percent must not exceed 100".into(),
        ));
    }
    let largest_index = operations.saturating_sub(1).min(keyspace - 1);
    if key_offset > u64::MAX - largest_index {
        return Err(crowdb_chunk_kv_client::ClientError::InvalidRequest(
            "key offset and keyspace exceed the key index range".into(),
        ));
    }
    let started = Instant::now();
    let mut latencies = stream::iter(0..operations)
        .map(|index| {
            let client = Arc::clone(&client);
            let key_prefix = key_prefix.clone();
            async move {
                let (read, key_index) = load_operation(index, read_percent);
                let key = format!("{key_prefix}/{:020}", key_offset + (key_index % keyspace)).into_bytes();
                let value = vec![u8::try_from(index % 251).unwrap_or_default(); value_bytes];
                let operation_started = Instant::now();
                let success = if read {
                    client
                        .get(key, None)
                        .await
                        .is_ok_and(|response| response.result.is_ok())
                } else {
                    client
                        .put(key, value)
                        .await
                        .is_ok_and(|response| response.result.is_ok())
                };
                let latency = u64::try_from(operation_started.elapsed().as_micros()).unwrap_or(u64::MAX);
                (latency, success)
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    let elapsed = started.elapsed();
    latencies.sort_unstable_by_key(|(latency, _)| *latency);
    let errors = latencies.iter().filter(|(_, success)| !success).count();
    let total_us: u128 = latencies.iter().map(|(latency, _)| u128::from(*latency)).sum();
    let generation = client
        .cached_catalog()
        .as_ref()
        .map_or(0, |catalog| catalog.generation());
    let cached_catalog = client.cached_catalog();
    let partitions = cached_catalog
        .as_ref()
        .map_or(0, |catalog| catalog.entries().len());
    let mut owner_counts = BTreeMap::<u64, usize>::new();
    if let Some(catalog) = &cached_catalog {
        for entry in catalog.entries() {
            *owner_counts.entry(entry.owner.instance_id).or_default() += 1;
        }
    }
    let owner_min_partitions = owner_counts.values().copied().min().unwrap_or(0);
    let owner_max_partitions = owner_counts.values().copied().max().unwrap_or(0);
    let seconds = elapsed.as_secs_f64();
    let operations_per_second = u128::from(operations).saturating_mul(1_000_000) / elapsed.as_micros().max(1);
    let average = if operations == 0 {
        0
    } else {
        total_us / u128::from(operations)
    };
    println!(
        "chunk-kv: workload={} read_percent={read_percent} operations={operations} errors={errors} seconds={seconds:.3} \
         ops_s={:.0} avg_us={average} p50_us={} p99_us={} catalog_generation={generation} \
         partitions={partitions} owner_min_partitions={owner_min_partitions} \
         owner_max_partitions={owner_max_partitions}",
        if read_percent == 0 { "put" } else { "mix" },
        operations_per_second,
        percentile(&latencies, 50),
        percentile(&latencies, 99),
    );
    if errors == 0 {
        Ok(())
    } else {
        Err(crowdb_chunk_kv_client::ClientError::Transport(format!(
            "{errors} load operations failed"
        )))
    }
}

fn load_operation(index: u64, read_percent: u8) -> (bool, u64) {
    let write_percent = 100_u64.saturating_sub(u64::from(read_percent));
    let read = index % 100 >= write_percent;
    (
        read,
        if read {
            index.saturating_sub(write_percent)
        } else {
            index
        },
    )
}

fn percentile(latencies: &[(u64, bool)], percentile: usize) -> u64 {
    if latencies.is_empty() {
        return 0;
    }
    let index = latencies
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(latencies.len() - 1);
    latencies[index].0
}

fn exit_error(error: &str) -> ! {
    eprintln!("ERROR: {error}");
    std::process::exit(1)
}

#[cfg(test)]
mod tests {
    use super::load_operation;

    #[test]
    fn mixed_load_reads_keys_written_earlier_in_each_window() {
        assert_eq!(load_operation(74, 25), (false, 74));
        assert_eq!(load_operation(75, 25), (true, 0));
        assert_eq!(load_operation(99, 25), (true, 24));
        assert_eq!(load_operation(100, 25), (false, 100));
        assert_eq!(load_operation(175, 25), (true, 100));
    }
}
