// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `kv` domain — server lifecycle, logical concepts (store/group/replica),
//! and data-plane operations. Each sub-module owns one slice of the
//! domain.

pub mod data;
pub mod logical;
pub mod server;

pub(crate) use data::{run_kv_data_verb, KvDataVerb};
pub(crate) use logical::{
    run_group_verb, run_replica_verb, run_store_verb, GroupVerb, ReplicaVerb, StoreVerb,
};
pub(crate) use server::{run_kv_server_verb, KvServerVerb};
