# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Named flow scopes, fresh server profiles and bounded host-client dispatch."""

import concurrent.futures
import json
from dataclasses import dataclass

from fixture import Cluster
from diskdb_fixture import DiskdbCluster
from managed_fixture import ManagedCluster
from diskio_fixture import DiskioCluster, DiskioDiscoveryCluster, DiskioSemanticCluster


@dataclass(frozen=True)
class Case:
    scope: str
    name: str
    stores: tuple = (0,)
    wired: bool = False
    stores_per_node: tuple | None = None
    groups: tuple = (1,)
    restart: bool = False
    groups_per_node: tuple | None = None
    persisted_paths: tuple = ()
    node_args: tuple | None = None
    advertise: bool = False
    service_selection: tuple = ()
    diskio_backend: str | None = None
    diskio_fault_rate: float = 0
    diskio_direct: bool = False
    diskdb_restart_metadata: bool = False


KV_CASES = (
    Case("kv.metadata", "kv::slot_handoff::a_replacement_controller_resumes_partial_fences_without_replacing_the_cohort", groups=(0, 1)),
    Case("kv.metadata", "kv::slot_handoff::competing_cohorts_use_the_service_head_revision_and_reject_stale_maps", groups=(0, 1)),
    Case("kv.metadata", "kv::slot_publication::missing_cached_endpoint_refreshes_dynamic_owner_before_routing", groups=(0, 1)),
    Case("kv.metadata", "kv::slot_publication::stale_epoch_publications_are_conflicts_and_concurrent_regrants_preserve_all_slots", groups=(0, 1)),
    Case("kv.metadata", "kv::slot_publication::complete_epoch_publication_is_atomic_idempotent_and_storage_independent", groups=(0, 1)),
    Case("kv.metadata", "kv::slot_publication::competing_publications_choose_one_complete_epoch_map_without_changing_data_fences", groups=(0, 1)),
    Case("kv.metadata", "kv::slot_map::initialize_is_atomic_idempotent_and_layer_independent", groups=(0,)),
    Case("kv.metadata", "kv::slot_map::concurrent_initializers_cannot_combine_different_assignments", groups=(0,)),
    Case("kv.metadata", "kv::slot_map::paginated_owner_records_preserve_empty_instances", groups=(0,)),
    Case("kv.metadata", "kv::slot_map::service_client_needs_no_storage_map_and_refreshes_same_owner_endpoints", groups=(0,)),
    Case("kv.metadata", "kv::slot_map_validation::legacy_records_are_never_overwritten", groups=(0,)),
    Case("kv.metadata", "kv::slot_map_validation::legacy_chunk_state_without_range_bindings_is_not_reinterpreted", groups=(0,)),
    Case("kv.metadata", "kv::slot_map_validation::corrupt_owner_and_partial_generation_fail_closed", groups=(0,)),
    Case("kv.metadata", "kv::slot_map_validation::orphan_binding_is_preserved_without_creating_a_head", groups=(0,)),
    Case("kv.client", "kv::client_data::put_get_delete_round_trip_via_topology_discovery", (1,)),
    Case("kv.client", "kv::client_data::revision_cas_and_conditional_batch_round_trip", (1,)),
    Case("kv.client", "kv::client_data::batch_write_and_scan", (1,)),
    Case("kv.client", "kv::client_scan::scan_keys_only_and_scan_count", (1,)),
    Case("kv.client", "kv::client_scan::scan_deadline_returns_partial_result", (1,)),
    Case("kv.client", "kv::domain_monitor::concurrent_domain_monitor_ensure_reconciles_cas_busy", groups=(0,)),
    Case("kv.group0", "kv::membership_cas::independent_clients_cannot_submit_two_changes_or_skip_installation", groups=(0,)),
    Case("kv.membership", "kv::authority::incarnation_change_revokes_old_writes_and_preserves_record_cas", (1,)),
    Case("kv.membership", "kv::authority::ordinary_and_malformed_writes_cannot_change_slot_authority", (1,)),
    Case("kv.membership", "kv::authority::raw_owner_rpc_rejects_malformed_authority_and_cross_namespace_mutation", (1,)),
    Case("kv.group0", "kv::discovery::paused_bootstrap_admits_no_registration_writes",
         stores_per_node=((),), groups=(), node_args=(("--node-id", "2", "--keepalive-interval", "1"),), advertise=True),
    Case("kv.group0", "kv::discovery::nonmember_recovers_discovery_and_registration_identity",
         stores_per_node=((), ()), groups=(), node_args=(
             ("--instance-id", "9001", "--node-id", "1", "--keepalive-interval", "1"),
             ("--node-id", "2", "--keepalive-interval", "1")), advertise=True),
    Case("kv.recovery", "kv::failover::graceful_leader_shutdown", (0, 1, 2), True),
    Case("kv.membership", "kv::failover::remove_leader_and_continue", (0, 1, 2), True),
    Case("kv.membership", "kv::failover::unavailable_voter_and_follower_removal",
         stores_per_node=((0,), (1,), (2,), ()), groups_per_node=((1,), (1,), (1,), ())),
    Case("kv.group0", "kv::bootstrap::initialize_and_restore", stores_per_node=((),), groups=(), restart=True,
         persisted_paths=("kv/waldata/store0/group0",)),
    Case("kv.group0", "kv::bootstrap::custom_replica", stores_per_node=((),), groups=()),
    Case("kv.group0", "kv::ownership::prepared_owner_survives_restart", stores_per_node=((),), groups=(), restart=True),
    Case("kv.group0", "kv::ownership::competing_prepares", stores_per_node=((),), groups=()),
    Case("kv.group0", "kv::ownership::cleanup_fences_delayed_requests", stores_per_node=((),), groups=(), restart=True),
    Case("kv.recovery", "kv::snapshot::snapshot_import_precedes_wiring_and_catches_up_tail",
         (0, 1, 2), groups_per_node=((1,), (1,), ())),
    Case("kv.membership", "kv::reconfiguration::quorum_expands_and_shrinks_without_losing_writes",
         (0, 1, 2, 3, 4)),
    Case("kv.routing", "kv::cluster::topology_and_follower_hint", (10, 20, 30), True),
    Case("kv.routing", "kv::cluster::dynamic_groups_isolate_data", (0, 1, 2), True),
    Case("kv.management", "kv::management::management_lifecycle"),
    Case("kv.membership", "kv::membership::replica_lifecycle"),
    Case("kv.membership", "kv::membership::epoch_replay_and_authority_bootstrap"),
    Case("kv.management", "kv::progressive::topology_wiring_and_progressive_setup", (0, 0)),
    Case("kv.management", "kv::maintenance::synchronous_step_down_fence"),
    Case("kv.management", "kv::maintenance::flush_and_wipe_preserve_group_and_resume_writes"),
    Case("kv.management", "kv::operations::async_and_sync_leader_lifecycle", (0, 1, 2), True),
    Case("kv.routing", "kv::routing::multi_store_isolation_and_recovery", wired=True,
         stores_per_node=((1, 2),) * 3, groups=(10,), restart=True),
)


DISKDB_CASES = (
    Case("diskdb.recovery", "diskdb::recovery::diskdb_restart_preserves_acknowledged_records", restart=True),
    Case("diskdb.recovery", "diskdb::recovery::metadata_restart_preserves_acknowledged_records", restart=True, diskdb_restart_metadata=True),
    Case("diskdb.allocation", "diskdb::allocation::allocate_commit_and_free_through_client"),
    Case("diskdb.ownership", "diskdb::ownership::wrong_owner_free_is_ignored_by_compaction"),
    Case("diskdb.reuse", "diskdb::space_reuse::exhaust_free_compact_and_reuse_exact_space"),
)

DEPLOYMENT_CASES = (
    Case("deployment.selection", "deployment::selected_kv_services_recover_acknowledged_data", restart=True),
    Case("deployment.selection", "deployment::selected_storage_services_recover_acknowledged_data", restart=True,
         service_selection=("kv", "diskdb")),
    Case("deployment.selection", "deployment::complete_release_services_recover_acknowledged_data", restart=True,
         service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
)

DISKIO_CASES = (
    Case("diskio.direct", "diskio::durability::disk_io_e2e_durability", restart=True, diskio_direct=True),
    Case("diskio.durability", "diskio::durability::disk_io_e2e_durability", restart=True),
    Case("diskio.io.mem", "diskio::io::mem_disk_full_flow", diskio_backend="mem"),
    Case("diskio.io.null", "diskio::io::null_disk_full_flow", diskio_backend="null"),
    Case("diskio.discovery", "diskio::group0::disk_io_e2e_group0_sync", diskio_backend="mem"),
    Case("diskio.errors", "diskio::errors::disk_io_e2e_error_paths", diskio_backend="mem"),
    Case("diskio.semantic", "diskio::semantic::semantic_client_owns_route_transport_payload_and_durability", diskio_backend="semantic"),
    Case("diskio.faults", "diskio::faults::disk_io_fault_errors", diskio_backend="mem", diskio_fault_rate=1.0),
)
ALL_CASES = KV_CASES + DISKDB_CASES + DEPLOYMENT_CASES + DISKIO_CASES

def case_nodes(case):
    if case.diskio_backend == "semantic":
        return 4
    if case.diskio_backend:
        return 2
    if len(case.service_selection) > 2:
        return 4
    return 2 if case.scope.startswith("diskdb.") else len(case.stores_per_node or case.stores)

def run_cases(image, artifacts, bundle, slots, data_root=None, scope="all", case_name=None, layer="all"):
    selected = tuple(case for case in ALL_CASES if (layer == "all" or case.scope.startswith(layer + ".")) and (scope == "all" or case.scope == scope)
                     and (not case_name or case.name == case_name))
    if not selected:
        raise ValueError(f"no migrated cases for scope {scope}")

    def run(case):
        count = len(case.stores_per_node or case.stores)
        cluster = ManagedCluster(image, artifacts, services=case.service_selection or ("kv",), data_root=data_root) if case.scope.startswith("deployment.") else DiskioSemanticCluster(image, artifacts, bundle, data_root=data_root) if case.diskio_backend == "semantic" else DiskioDiscoveryCluster(image, artifacts, bundle, case.diskio_backend, fault_rate=case.diskio_fault_rate, data_root=data_root) if case.diskio_backend else DiskioCluster(image, artifacts, direct=case.diskio_direct, data_root=data_root) if case.scope.startswith("diskio.") else DiskdbCluster(image, artifacts, bundle, restart_metadata=case.diskdb_restart_metadata, data_root=data_root) if case.scope.startswith("diskdb.") else Cluster(image, artifacts, data_root=data_root, node_count=count,
                          store_ids=case.stores, wire_replicas=case.wired,
                          stores_per_node=case.stores_per_node, groups=case.groups,
                          groups_per_node=case.groups_per_node, node_args=case.node_args,
                          advertise=case.advertise)
        (cluster.artifacts / "case.json").write_text(json.dumps({
            "scope": case.scope, "case": case.name}, indent=2))
        with cluster:
            if case.restart:
                cluster.client(bundle, "write", case.name)
                for data in cluster.data_paths:
                    for path in case.persisted_paths:
                        if not (data / path).is_dir():
                            raise AssertionError(f"acknowledged state did not persist at {data / path}")
                cluster.crash_restart()
                cluster.client(bundle, "verify", case.name)
            else:
                cluster.client(bundle, case.name.rsplit("::", 1)[-1], case.name)

    with concurrent.futures.ThreadPoolExecutor(max_workers=slots) as pool:
        for _ in pool.map(run, selected):
            pass
