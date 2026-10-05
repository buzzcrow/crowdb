// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowDB Storage` Console web backend.
//!
//! Key work: two-tree API contract, physical tree lifecycle (A3),
//! per-node primitives (A4), logical store/group planes (A5/A6),
//! logical replica plane with bidirectional wiring + rollback (A7),
//! KV data plane with leader resolution via the monitor cache and
//! `NotLeader` retry (A8), Swagger UI (A9), React SPA shell.

mod access;
mod chunk;
mod chunk_kv;
pub mod corr_id;
pub mod diskdb;
pub mod error;
pub mod expand;
pub mod health;
pub mod kv;
mod launch;
pub mod lifecycle;
mod managed;
mod managed_hardware;
mod managed_logical;
pub mod mgmt;
pub mod owner_assignment;
pub mod physical;
mod services;
pub mod spa;
mod standalone;
pub mod state;

pub use state::AppState;

/// Build the Axum router used by both the binary and integration tests.
#[allow(clippy::too_many_lines)]
pub fn router(state: AppState) -> axum::Router {
    use axum::routing::{any, delete, get, post};

    if state.managed_mode {
        let disk_management =
            axum::middleware::from_fn_with_state(state.clone(), health::require_disk_management);
        let managed = axum::Router::new()
            .route("/healthz", get(health::healthz))
            .route("/api/mode", get(health::mode))
            .route("/api/authority", get(managed::authority))
            .route("/api/preview", get(managed::snapshot))
            .route("/api/chunk-kv/catalog", get(chunk_kv::catalog))
            .route("/api/chunk-kv/runtime", get(chunk_kv::runtime))
            .route("/api/chunks", get(chunk::list))
            .route("/api/stores/:sid/groups/:gid/chunks", get(chunk::pxgroup_list))
            .route("/api/chunk-slots", get(chunk::slots::list))
            .route("/api/chunks/:id", get(chunk::detail))
            .merge(access::read_router())
            .route("/api/diskdb/instances", get(diskdb::http_list_diskdb_instances))
            .route("/api/diskdb/usage", get(diskdb::http_diskdb_usage))
            .route("/api/hardware/capacity", get(diskdb::http_hardware_capacity))
            .route("/api/diskdb/scan-status", get(diskdb::http_diskdb_scan_status))
            .route(
                "/api/diskdb/scan",
                post(diskdb::http_diskdb_scan).route_layer(disk_management.clone()),
            )
            .route(
                "/api/diskdb/recalc",
                post(diskdb::http_diskdb_recalc).route_layer(disk_management.clone()),
            )
            .route(
                "/api/diskdb/compact",
                post(diskdb::http_diskdb_compact).route_layer(disk_management.clone()),
            )
            .route(
                "/api/diskdb/rebuild",
                post(diskdb::http_diskdb_rebuild).route_layer(disk_management.clone()),
            )
            .route("/api/stores/:sid/groups/:gid/kv/get", get(kv::http_kv_get))
            .route("/api/stores/:sid/groups/:gid/kv/scan", get(kv::http_kv_scan))
            .route("/api/stores/:sid/groups/:gid/kv/put", post(kv::http_kv_put))
            .route("/api/stores/:sid/groups/:gid/kv/delete", post(kv::http_kv_delete))
            .route(
                "/api/management/check",
                post(|| async { axum::http::StatusCode::NO_CONTENT }),
            )
            .route(
                "/api/stores",
                get(managed_logical::list_stores).merge(post(managed_logical::add_store)),
            )
            .route(
                "/api/stores/:sid",
                get(managed_logical::get_store).merge(delete(managed_logical::remove_store)),
            )
            .route(
                "/api/stores/:sid/groups",
                get(managed_logical::list_groups).merge(post(managed_logical::add_group)),
            )
            .route(
                "/api/stores/:sid/groups/:gid",
                get(managed_logical::get_group).merge(delete(managed_logical::remove_group)),
            )
            .route(
                "/api/stores/:sid/groups/:gid/replicas",
                get(managed_logical::list_replicas).merge(post(managed_logical::add_replica)),
            )
            .route(
                "/api/stores/:sid/groups/:gid/replicas/:rid",
                get(managed_logical::get_replica).merge(delete(managed_logical::remove_replica)),
            )
            .route("/api/*path", any(health::managed_api_unavailable))
            .route("/internal/reset", any(health::managed_api_unavailable))
            .fallback(spa::spa_fallback);
        let managed = if state.web_mode == Some(crowdb_console_shared::config::web::WebMode::BareMetal) {
            let hardware = axum::Router::new()
                .route("/api/cluster/init", post(mgmt::http_cluster_init))
                .route(
                    "/api/racks",
                    get(managed_hardware::list_racks).merge(post(managed_hardware::add_rack)),
                )
                .route(
                    "/api/racks/:rack_id",
                    get(managed_hardware::get_rack).merge(delete(managed_hardware::remove_rack)),
                )
                .route(
                    "/api/racks/:rack_id/nodes",
                    get(managed_hardware::list_rack_nodes),
                )
                .route(
                    "/api/nodes",
                    get(managed_hardware::list_nodes).merge(post(managed_hardware::add_node)),
                )
                .route(
                    "/api/nodes/:id",
                    get(managed_hardware::get_node).merge(delete(managed_hardware::remove_node)),
                )
                .route(
                    "/api/nodes/:id/disk-groups",
                    get(managed_hardware::list_disk_groups).merge(post(managed_hardware::add_disk_group)),
                )
                .route(
                    "/api/nodes/:id/disk-groups/:dg_id",
                    get(managed_hardware::get_disk_group).merge(delete(managed_hardware::remove_disk_group)),
                )
                .route(
                    "/api/nodes/:id/disk-groups/:dg_id/disks",
                    get(managed_hardware::list_disks).merge(post(managed_hardware::add_disk)),
                )
                .route(
                    "/api/nodes/:id/disk-groups/:dg_id/disks/:disk_id",
                    get(managed_hardware::get_disk).merge(delete(managed_hardware::remove_disk)),
                );
            managed.merge(hardware).merge(launch::routes())
        } else {
            managed
        };
        return managed
            .with_state(state)
            .layer(axum::middleware::from_fn(corr_id::corr_id_layer));
    }

    axum::Router::new()
        .merge(services::routes())
        .route("/api/chunk-kv/catalog", get(chunk_kv::catalog))
        .route("/api/chunk-kv/runtime", get(chunk_kv::runtime))
        .route("/api/chunks", get(chunk::list))
        .route("/api/stores/:sid/groups/:gid/chunks", get(chunk::pxgroup_list))
        .route("/api/chunk-slots", get(chunk::slots::list))
        .route("/api/chunks/:id", get(chunk::detail))
        .merge(access::read_router())
        .route("/api/access/connections", post(access::configure))
        .route("/healthz", get(health::healthz))
        .route("/api/mode", get(health::mode))
        // ── Physical tree (A3): rack + node lifecycle ────────────────
        .route(
            "/api/racks",
            get(lifecycle::http_list_racks).post(lifecycle::http_add_rack),
        )
        .route(
            "/api/racks/:rack_id",
            get(lifecycle::http_get_rack).delete(lifecycle::http_remove_rack),
        )
        .route(
            "/api/racks/:rack_id/nodes",
            get(lifecycle::http_list_rack_nodes).post(lifecycle::http_add_rack_node),
        )
        .route(
            "/api/nodes",
            get(lifecycle::http_list_nodes).post(lifecycle::http_add_node),
        )
        .route(
            "/api/nodes/:id",
            get(lifecycle::http_get_node).delete(lifecycle::http_remove_node),
        )
        .route("/api/nodes/:id/ping", post(lifecycle::http_ping_node))
        // Disk-group lifecycle (R81).
        .route(
            "/api/nodes/:id/disk-groups",
            get(lifecycle::http_list_node_disk_groups).post(lifecycle::http_add_node_disk_group),
        )
        .route(
            "/api/nodes/:id/disk-groups/:dg_id",
            get(lifecycle::http_get_node_disk_group).delete(lifecycle::http_remove_node_disk_group),
        )
        // Disk lifecycle (R81).
        .route(
            "/api/nodes/:id/disk-groups/:dg_id/disks",
            get(lifecycle::http_list_disks_in_group).post(lifecycle::http_add_disk),
        )
        .route(
            "/api/nodes/:id/disk-groups/:dg_id/disks/batch",
            post(lifecycle::http_add_disks_batch),
        )
        .route(
            "/api/nodes/:id/disk-groups/:dg_id/disks/:disk_id",
            get(lifecycle::http_get_disk).delete(lifecycle::http_remove_disk),
        )
        // Disk status set (R77).
        .route(
            "/api/disks/:disk_id/status",
            axum::routing::put(diskdb::http_set_disk_status),
        )
        // Disk-group status set.
        .route(
            "/api/disk-groups/:rack_id/:node_id/:dg_id/status",
            axum::routing::put(diskdb::http_set_disk_group_status),
        )
        // Disk-group ownership/bind assignment (capacity view).
        .route(
            "/api/disk-groups/:rack_id/:node_id/:dg_id/owner",
            axum::routing::put(diskdb::http_set_disk_group_owner),
        )
        .route(
            "/api/disk-groups/:rack_id/:node_id/:dg_id/bind",
            axum::routing::put(diskdb::http_set_disk_group_bind),
        )
        // ── Diskdb runtime proxy (R77): /api/diskdb/* ───────────────
        .route("/api/diskdb/instances", get(diskdb::http_list_diskdb_instances))
        .route("/api/diskdb/usage", get(diskdb::http_diskdb_usage))
        .route("/api/hardware/capacity", get(diskdb::http_hardware_capacity))
        .route("/api/diskdb/scan-status", get(diskdb::http_diskdb_scan_status))
        .route("/api/diskdb/scan", post(diskdb::http_diskdb_scan))
        .route("/api/diskdb/recalc", post(diskdb::http_diskdb_recalc))
        .route("/api/diskdb/compact", post(diskdb::http_diskdb_compact))
        .route("/api/diskdb/rebuild", post(diskdb::http_diskdb_rebuild))
        // ── DiskDB deploy lifecycle (R77) ────────────────────────────
        .route("/api/nodes/:id/diskdb/deploy", post(diskdb::http_deploy_diskdb))
        .route("/api/nodes/:id/diskdb/restart", post(diskdb::http_restart_diskdb))
        .route("/api/nodes/:id/diskdb/stop", post(diskdb::http_stop_diskdb))
        .route(
            "/api/nodes/:id/diskdb",
            axum::routing::delete(diskdb::http_delete_diskdb),
        )
        .route(
            "/api/nodes/:id/server",
            get(lifecycle::http_get_node_server).delete(lifecycle::http_delete_node_server),
        )
        .route(
            "/api/nodes/:id/server/deploy",
            post(lifecycle::http_deploy_node_server),
        )
        .route(
            "/api/nodes/:id/server/restart",
            post(lifecycle::http_restart_node_server),
        )
        .route(
            "/api/nodes/:id/server/stop",
            post(lifecycle::http_stop_node_server),
        )
        // Cluster-wide server list (CLI `server list`), composed from the
        // config + monitor cache.
        .route("/api/servers", get(lifecycle::http_list_servers))
        // ── Physical tree (A4): per-node store/group/remote primitives ─
        .route(
            "/api/nodes/:id/stores",
            get(physical::http_list_node_stores).post(physical::http_add_node_store),
        )
        .route(
            "/api/nodes/:id/stores/:sid",
            get(physical::http_get_node_store).delete(physical::http_remove_node_store),
        )
        .route(
            "/api/nodes/:id/stores/:sid/groups",
            get(physical::http_list_node_groups).post(physical::http_add_node_group),
        )
        .route(
            "/api/nodes/:id/stores/:sid/groups/:gid",
            get(physical::http_get_node_group).delete(physical::http_remove_node_group),
        )
        .route(
            "/api/nodes/:id/stores/:sid/groups/:gid/remotes",
            post(physical::http_add_node_remote),
        )
        .route(
            "/api/nodes/:id/stores/:sid/groups/:gid/remotes/:rid",
            delete(physical::http_remove_node_remote),
        )
        // ── Logical tree (A5/A6): store + group planes ──────────────
        .route(
            "/api/stores",
            get(mgmt::http_list_stores).post(mgmt::http_add_store),
        )
        .route(
            "/api/stores/:sid",
            get(mgmt::http_get_store).delete(mgmt::http_remove_store),
        )
        .route(
            "/api/stores/:sid/groups",
            get(mgmt::http_list_groups).post(mgmt::http_add_group),
        )
        .route(
            "/api/stores/:sid/groups/:gid",
            get(mgmt::http_get_group).delete(mgmt::http_remove_group),
        )
        // ── Logical tree (A7): replica plane ────────────────────────
        .route(
            "/api/stores/:sid/groups/:gid/replicas",
            get(mgmt::http_list_replicas).post(mgmt::http_add_replica),
        )
        .route(
            "/api/stores/:sid/groups/:gid/replicas/:rid",
            get(mgmt::http_get_replica).delete(mgmt::http_remove_replica),
        )
        // Leader crowdb-rpc endpoint resolver (CLI bench dials crowdb-rpc directly).
        .route("/api/stores/:sid/groups/:gid/endpoint", get(kv::http_kv_endpoint))
        // KV data plane: leader resolved via the monitor cache; NotLeader triggers one retry (A8).
        .route("/api/stores/:sid/groups/:gid/kv/get", get(kv::http_kv_get))
        .route("/api/stores/:sid/groups/:gid/kv/scan", get(kv::http_kv_scan))
        .route("/api/stores/:sid/groups/:gid/kv/put", post(kv::http_kv_put))
        .route("/api/stores/:sid/groups/:gid/kv/delete", post(kv::http_kv_delete))
        // ── Metrics proxy (R11): per-node, per-group (leader), per-store (aggregated) ──
        .route("/api/nodes/:id/metrics", get(mgmt::http_node_metrics))
        .route("/api/stores/:sid/metrics", get(mgmt::http_store_metrics))
        .route(
            "/api/stores/:sid/groups/:gid/metrics",
            get(mgmt::http_group_metrics),
        )
        // ── Cluster init (R2): system group bootstrap ────────────────
        .route("/api/cluster/init", post(mgmt::http_cluster_init))
        .route("/api/cluster/destroy", post(lifecycle::http_internal_reset))
        .route("/api/cluster/clean", post(lifecycle::http_cluster_clean))
        // ── Internal: E2E test reset (alias for destroy) ─────────────
        .route("/internal/reset", post(lifecycle::http_internal_reset))
        // React SPA fallback.
        .fallback(spa::spa_fallback)
        .with_state(state)
        // Propagate `x-crowdb-kv-corr-id` through every request: read it
        // (or mint one), open a task-local scope so outbound clients
        // attach it to their own headers, echo it back on the response.
        .layer(axum::middleware::from_fn(corr_id::corr_id_layer))
}

#[cfg(test)]
mod tests {
    use super::{router, AppState};

    #[test]
    fn router_builds() {
        let _ = router(AppState::default());
    }

    #[tokio::test]
    async fn startup_topology_check_no_nodes() {
        let state = AppState::default();
        // No nodes deployed → NoNodes path, should not panic or hang.
        super::mgmt::startup_topology_check(&state).await;
    }
}
