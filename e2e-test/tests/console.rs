// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod discovery;
mod fixture;
mod group_publication;
mod hardware;
mod logical_publication;

#[path = "console/common/rpc_response_proxy.rs"]
mod rpc_response_proxy;

#[path = "console/common/bootstrap_authority.rs"]
mod bootstrap_authority;
mod bootstrap_intent;
mod bootstrap_publication;
mod cluster;
mod deployment;
mod logical_delete;
mod logical_fanout;
mod observation;
mod replica_cleanup;
mod server_data;
mod server_management;
mod web_authority;
mod web_catalog;
mod web_isolation;
mod web_kv_lifecycle;
mod web_lifecycle;
mod web_recovery;
mod web_registry;
mod web_slots;
mod web_standalone;

mod web_wal_restart;

mod web_access;

mod web_cluster_recovery;

mod web_lifetime;
mod web_metrics;

mod web_kv;
mod web_legacy;

mod web_management;

mod web_bootstrap;
mod web_replicas;
mod web_sealed_bootstrap;
mod web_voters;

mod native_balance;
mod native_binding;
mod native_browser;
mod native_capacity;
mod native_chunks;
mod native_context;
mod native_disk_lifecycle;
mod native_disk_usage;
mod native_load;
mod native_recovery;
mod native_s3;
mod native_storage;
mod web_diskio_discovery;
mod web_leader_removal;

mod native_journal;

mod native_geometry;

mod browser_dataset;
