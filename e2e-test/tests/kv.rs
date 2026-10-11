// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV public management and data-plane acceptance.

mod authority;
mod bootstrap;
mod client_data;
mod client_scan;
mod cluster;
mod discovery;
mod domain_monitor;
mod failover;
mod maintenance;
mod management;
mod membership;
mod membership_cas;
mod operations;
mod ownership;
mod progressive;
mod reconfiguration;
mod routing;
mod snapshot;

mod slot_map;
mod slot_map_validation;

mod handoff_fixture;
mod slot_handoff;
mod slot_publication;
