// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use clap::Parser;
use crowdb_console_shared::ops::cluster::{
    KvDeployTunables, LocalChunkdbDeployConfig, LocalDiskdbDeployConfig,
};

#[derive(Clone, Debug, Parser)]
#[allow(clippy::struct_excessive_bools)] // Independent command-line switches for separate services.
pub struct TestSetup {
    #[arg(long, value_parser = ["kv", "diskdb", "chunkdb", "combined"])]
    pub kind: String,
    #[arg(long, default_value_t = 3)]
    pub nodes: usize,
    #[arg(long)]
    rpc_workers: Option<u32>,
    #[arg(long)]
    peer_pool_size: Option<usize>,
    #[arg(long)]
    max_inflight: Option<usize>,
    #[arg(long)]
    coalesce_max_keys: Option<usize>,
    #[arg(long)]
    event_write: bool,
    #[arg(long)]
    send_queue_capacity: Option<u32>,
    #[arg(long)]
    metrics_interval: Option<u64>,
    #[arg(long)]
    kv_backend: Option<String>,
    #[arg(long)]
    wal_backend: Option<String>,
    #[arg(long)]
    no_fsync: bool,
    #[arg(long, default_value_t = 1)]
    disk_groups_per_node: usize,
    #[arg(long, default_value_t = 4)]
    disks_per_group: usize,
    #[arg(long, default_value_t = 1_099_511_627_776)]
    disk_capacity_bytes: u64,
    #[arg(long, default_value_t = 274_877_906_944)]
    disk_zone_size_bytes: u64,
    #[arg(long, default_value_t = 1_048_576)]
    disk_unit_size_bytes: u32,
    #[arg(long, value_delimiter = ',', default_value = "1")]
    data_groups: Vec<u64>,
    #[arg(long)]
    kv_connections: Option<usize>,
    #[arg(long)]
    kv_client_rpc_workers: Option<u32>,
    #[arg(long)]
    free_batch: bool,
    #[arg(long)]
    free_flush_max_batch: Option<u32>,
    #[arg(long, default_value_t = 3)]
    chunkdb_instances: usize,
    #[arg(long)]
    diskdb_connections: Option<usize>,
    #[arg(long)]
    diskdb_client_rpc_workers: Option<u32>,
    #[arg(long)]
    diskio_rpc_workers: Option<u32>,
    #[arg(long, default_value = "null", value_parser = ["null", "mem"])]
    pub diskio_dummy_disk_type: String,
}

impl TestSetup {
    pub fn kv(&self) -> KvDeployTunables {
        KvDeployTunables {
            rpc_workers: self.rpc_workers,
            peer_pool_size: self.peer_pool_size,
            max_inflight: self.max_inflight,
            coalesce_max_keys: self.coalesce_max_keys,
            event_write: self.event_write.then_some(true),
            send_queue_capacity: self.send_queue_capacity,
            metrics_interval: self.metrics_interval,
            kv_backend: self.kv_backend.clone(),
            wal_backend: self.wal_backend.clone(),
            no_fsync: self.no_fsync.then_some(true),
        }
    }
    pub fn disk(&self) -> LocalDiskdbDeployConfig {
        LocalDiskdbDeployConfig {
            disk_groups_per_node: self.disk_groups_per_node,
            disks_per_group: self.disks_per_group,
            capacity_bytes: self.disk_capacity_bytes,
            zone_size_bytes: self.disk_zone_size_bytes,
            unit_size_bytes: self.disk_unit_size_bytes,
            data_groups: self.data_groups.clone(),
            rpc_workers: self.rpc_workers,
            kv_connections: self.kv_connections,
            kv_client_rpc_workers: self.kv_client_rpc_workers,
            free_batch_enabled: self.free_batch.then_some(true),
            free_flush_max_batch: self.free_flush_max_batch,
            keepalive_interval_secs: None,
        }
    }
    pub fn chunk(&self) -> LocalChunkdbDeployConfig {
        LocalChunkdbDeployConfig {
            instance_count: self.chunkdb_instances,
            storage_groups: vec![1, 2, 3],
            dynamic_ownership: false,
            allow_unsafe_ec: false,
            rpc_workers: self.rpc_workers,
            diskio_rpc_workers: self.diskio_rpc_workers,
            kv_connections: self.kv_connections,
            kv_client_rpc_workers: self.kv_client_rpc_workers,
            diskdb_connections: self.diskdb_connections,
            diskdb_client_rpc_workers: self.diskdb_client_rpc_workers,
            metrics_interval: self.metrics_interval,
        }
    }
}
