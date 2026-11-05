// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `paxos-kv` domain — logical concepts (store/group/replica),
//! and data-plane operations. Each sub-module owns one slice of the
//! domain.

pub mod data;
pub mod logical;

pub(crate) use data::{run_paxos_kv_data_verb, PaxosKvDataVerb};
pub(crate) use logical::{
    run_group_verb, run_replica_verb, run_store_verb, GroupVerb, ReplicaVerb, StoreVerb,
};
