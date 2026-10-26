# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Named flow scopes, fresh server profiles and bounded host-client dispatch."""

import json
import threading
import time
from dataclasses import dataclass
from contextlib import nullcontext

from browser_batch import run_browser_batches
from resource_dispatch import batch_slots, run_with_resources

from fixture import Cluster
from diskdb_fixture import DiskdbCluster
from chunkdb_fixture import ChunkdbCluster, ChunkdbStorageCluster
from chunk_kv_fixture import ChunkKvCluster
from native_s3_fixture import NativeS3Cluster
from access_s3_fixture import AccessS3Cluster
from s3_node_fixture import S3NodeCluster
from access_protocol_fixture import AccessProtocolCluster
from access_iceberg_fixture import AccessIcebergCluster
from access_dataset_fixture import AccessDatasetCluster
from s3_recovery import SERVICE_NODES, run_recovery
from managed_fixture import ManagedCluster
from deployment_bootstrap_fixture import InterruptedBootstrapCluster
from deployment_validation_fixture import RejectedBootstrapCluster
from deployment_clients_fixture import DeploymentClientsCluster
from deployment_failure_fixture import DeploymentFailureCluster
from deployment_anonymous_fixture import AnonymousReleaseCluster
from deployment_browser_fixture import DeploymentBrowserCluster
from deployment_listener_fixture import FixedListenerAccessCluster
from console_fixture import ConsoleCluster
from console_access_fixture import AccessDeploymentCluster
from console_lifetime_fixture import DisposableWebCluster
from console_recovery_fixture import RecoveringWebCluster
from console_web_fixture import ConsoleWebCluster
from console_bootstrap_fixture import BootstrapWebCluster
from console_shell_fixture import ShellBrowserCluster
from console_discovery_browser_fixture import DiscoveryBrowserCluster
from console_cli_fixture import CliContainerCluster
from console_cli_target import CliAttachedCluster
from oci_reference_fixture import OciReferenceCluster
from console_native_fixture import NativeConsoleCluster
from console_standalone_fixture import StandaloneWebCluster
from console_registry_fixture import RegistryWebCluster
from console_typed_fixture import TypedWebCluster
from console_wal_fixture import WalWebCluster
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
    chunkdb_server: bool = False
    chunkdb_storage: bool = False
    diskio_process_exit: bool = False
    chunkdb_multi: bool = False
    storage_backend: str = "mem"
    chunk_kv_empty_catalog: bool = False
    sdk_client: bool = False
    dataset_sdk: bool = False
    s3_recovery: str | None = None
    s3_script: str | None = None
    s3_language: str | None = None
    s3_node_fault: bool = False
    s3_policy: bool = False
    access_protocol: bool = False
    iceberg_native_files: bool = False
    iceberg_catalog_lifecycle: bool = False
    iceberg_gc: bool = False
    iceberg_gc_page_items: int | None = None
    iceberg_gc_interval_ms: int | None = 100
    iceberg_gc_catalogs: tuple = ()
    iceberg_deferred_access: bool = False
    console_web: bool = False
    preserve_network: bool = False
    native_client: bool = False
    native_disk_gib: tuple = (80, 8, 8)
    native_zone_gib: tuple = (1, 1, 1)
    native_rack_ids: tuple | None = None
    native_admission_count: int | None = None
    native_diskless_nodes: tuple = ()
    native_spare_disk_gib: int = 0
    native_supplied_candidates: bool = False
    native_bootstrap: bool = True
    native_auto_bootstrap: bool = False
    native_observations: bool = False
    native_browser: str | None = None
    shell_browser: str | None = None
    shell_observations: bool = False
    console_browser: str | None = None
    cli_replay: str | None = None
    oci_reference: str | None = None
    oci_editable_ui: bool = False
    oci_address_recovery: bool = False
    oci_shortcuts: bool = False
    cli_workload: bool = False
    client_timeout: int = 120


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
    Case("deployment.reference", "deployment::reference::direct_docker_one_node", oci_reference="docker-one", oci_shortcuts=True),
    Case("deployment.reference", "deployment::reference::editable_ui_one_node", oci_reference="docker-one", oci_editable_ui=True),
    Case("deployment.reference", "deployment::reference::editable_ui_three_nodes", oci_reference="docker-three", oci_editable_ui=True),
    Case("deployment.reference", "deployment::reference::direct_docker_three_nodes", oci_reference="docker-three", oci_address_recovery=True),
    Case("deployment.reference", "deployment::reference::compose_one_node", oci_reference="compose-one"),
    Case("deployment.reference", "deployment::reference::compose_three_nodes", oci_reference="compose-three"),
    Case("deployment.balance", "deployment::balance::single_node_preserves_default_chunk_kv_balance_policy", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
    Case("deployment.listeners", "deployment::listeners::fixed_s3_listener_recovers_after_explicit_termination", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
    Case("deployment.listeners", "deployment::listeners::fixed_iceberg_listener_recovers_after_explicit_termination", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
    Case("deployment.browser", "deployment::browser::readonly_release_ui_preserves_domains_and_live_authority", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
    Case("deployment.anonymous", "deployment::anonymous::default_anonymous_volume_is_ready_and_monitor_death_exits_container", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
    Case("deployment.failures", "deployment::failures::restart_exhaustion_exits_the_container", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=540),
    Case("deployment.failures", "deployment::failures::recovery_rejects_changed_durable_credentials", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=540),
    Case("deployment.clients", "deployment::clients::official_clients_and_protocol_layout_survive_restart", restart=True, service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=420),
    Case("deployment.kv", "deployment::kv_bootstrap::monitor_creates_real_kv_groups_then_validates_after_restart", restart=True, service_selection=("kv",)),
    Case("deployment.validation", "deployment::validation::invalid_profile_is_rejected", service_selection=("kv",)),
    Case("deployment.validation", "deployment::validation::invalid_node_policy_is_rejected", service_selection=("kv",)),
    Case("deployment.storage", "deployment::storage::chunk_services_validate_live_registration_and_logical_topology", restart=True, service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
    Case("deployment.storage", "deployment::storage::real_iceberg_catalog_and_listener_survive_restart", restart=True, service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
    Case("deployment.bootstrap", "deployment::bootstrap::interrupted_setup_preserves_completed_steps", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
    Case("deployment.hardware", "deployment::hardware_bootstrap::writes_and_validates_hardware_with_real_group_zero", (0, 0, 0), True, groups=(0,)),
    Case("deployment.hardware", "deployment::hardware_bootstrap::conflicting_group_zero_record_rejects_without_creating_hardware", (0, 0, 0), True, groups=(0,)),
    Case("deployment.selection", "deployment::selected_kv_services_recover_acknowledged_data", restart=True),
    Case("deployment.selection", "deployment::selected_storage_services_recover_acknowledged_data", restart=True,
         service_selection=("kv", "diskdb")),
    Case("deployment.selection", "deployment::complete_release_services_recover_acknowledged_data", restart=True,
         service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web")),
    Case("deployment.supervision", "deployment::supervision::kv_crash", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::kv_hang", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::diskdb_crash", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::diskdb_hang", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::diskio_crash", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::diskio_hang", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::chunkdb_crash", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::chunkdb_hang", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::chunk_kv_crash", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::chunk_kv_hang", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::access_crash", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::access_hang", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::web_crash", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
    Case("deployment.supervision", "deployment::supervision::web_hang", service_selection=("kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access", "web"), client_timeout=180),
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
CHUNKDB_CASES = (
    Case("chunkdb.handoff", "chunkdb::handoff::relocation_journal_copies_publishes_then_frees_exact_source", chunkdb_storage=True),
    Case("chunkdb.repair", "chunkdb::repair::topology::degraded_ec_recovers_protection_and_exact_bytes_after_node_returns", chunkdb_storage=True),
    Case("chunkdb.conversion", "chunkdb::conversion::foreground::expired_client_claim_preserves_dirty_destination_tail_and_completes_once", chunkdb_storage=True),
    Case("chunkdb.slots", "chunkdb::slots::three_service_owners_preserve_all_purposes_across_paxos_failover", chunkdb_multi=True),
    Case("chunkdb.repair", "chunkdb::repair::four_missing_shards_repair_partial_data_and_parity", chunkdb_storage=True),
    Case("chunkdb.conversion", "chunkdb::conversion::manual_partial_mirror_to_ec_preserves_exact_shards", chunkdb_storage=True),
    Case("chunkdb.io", "chunkdb::io::all_failure_domains_route_exact_segment_bytes", chunkdb_storage=True),
    Case("chunkdb.placement", "chunkdb::placement::mirror_and_ec_matrix_publish_protected_geometry", chunkdb_server=True),
    Case("chunkdb.recovery", "chunkdb::recovery::crash_recovery_preserves_exact_chunk_records_and_placement", chunkdb_server=True, restart=True),
    Case("chunkdb.lifecycle", "chunkdb::lifecycle::allocate_seal_delete_preserves_physical_placement_and_free_facts", chunkdb_server=True),
    Case("chunkdb.allocation", "chunkdb::allocation::allocation_connect_failure_reroutes_but_lost_reply_does_not"),
)
CHUNK_STREAM_CASES = (
    Case("chunk-stream.objects", "chunk_stream::production_released_idle_writer_stops_after_owner_transfer", chunkdb_storage=True),
    Case("chunk-stream.recovery", "chunk_stream::production_stream_recovers_exact_bytes_after_service_restarts", chunkdb_storage=True, storage_backend="file", restart=True),
    Case("chunk-stream.errors", "chunk_stream::production_stream_write_returns_after_diskio_failure", chunkdb_storage=True, diskio_process_exit=True),
    Case("chunk-stream.errors", "chunk_stream::production_stream_initialization_returns_after_live_diskio_errors", chunkdb_storage=True, diskio_fault_rate=1.0),
)
CHUNK_CLIENT_CASES = (
    Case("chunk-client.reader", "chunk_client::reader::data::ec_range_read_recovers_only_the_requested_bytes", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.reader", "chunk_client::reader::data_loss::ec_read_reports_data_loss_beyond_parity_tolerance", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.reader", "chunk_client::reader::data_loss::partial_read_preserves_healthy_ec_shard_ranges_around_data_loss", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.reader", "chunk_client::reader::data_loss::mirror_read_succeeds_and_reports_replica_loss", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.reader", "chunk_client::reader::recovery::transient_mirror_read_failure_does_not_mark_segment_unavailable", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.reader", "chunk_client::reader::recovery::chunkdb_restart_admits_verified_corruption_and_repairs_full_shard", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.reader", "chunk_client::reader::recovery::routed_full_fragment_requests_share_one_durable_repair", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.large_repair", "chunk_client::large_repair::mirror::large_mirror_replaces_failed_replica_before_ordered_completion", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.large_repair", "chunk_client::large_repair::mirror_repair::large_mirror_retains_later_framed_buffers_until_failed_first_strip_is_repaired", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.large_repair", "chunk_client::large_repair::mirror_repair::large_mirror_rotates_after_repair_exhaustion_and_replays_ordered_data", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.large_repair", "chunk_client::large_repair::mirror::large_mirror_rotates_failed_partial_final_strip", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.large_repair", "chunk_client::large_repair::mirror::large_mirror_abort_drains_delayed_submitted_write_before_delete", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.large_repair", "chunk_client::large_repair::mirror_repair::large_mirror_repair_exhaustion_deletes_unsealed_chunk", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.large_repair", "chunk_client::large_repair::ec::large_write_replaces_failed_data_and_parity_segments_end_to_end", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.large_repair", "chunk_client::large_repair::ec_repair::large_write_repair_exhaustion_deletes_unsealed_chunk", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::batching::small_write_batches_concurrent_objects_and_reads_them_back", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::batching::eight_closed_mirror_strips_become_one_durable_ec_strip_without_reread", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::conversion::consecutive_conversion_groups_keep_one_mibibyte_shard_geometry", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::conversion::chunkdb_takes_over_durable_conversion_task_after_client_io_failure", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::conversion::manual_chunkdb_trigger_converts_closed_active_range_end_to_end", chunkdb_storage=True),
    Case("chunk-client.small_flow", "chunk_client::small_flow::background_conversion::automatic_chunkdb_scan_converts_three_groups_and_preserves_tail", chunkdb_storage=True),
    Case("chunk-client.small_flow", "chunk_client::small_flow::background_conversion::chunkdb_restart_recovers_an_inflight_conversion_claim", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::replica_repair::small_write_repairs_failed_replica_through_real_chunkdb_and_diskio", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::replica_repair::protected_small_write_reports_real_diskio_write_failure", chunkdb_storage=True, storage_backend="mem", diskio_fault_rate=1.0),
    Case("chunk-client.small_flow", "chunk_client::small_flow::replica_repair::protected_read_reports_diskio_process_failure", chunkdb_storage=True, storage_backend="mem", diskio_process_exit=True),
    Case("chunk-client.small_flow", "chunk_client::small_flow::repair_exhaustion::small_write_repair_preserves_acknowledged_prefix_in_open_block", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::repair_exhaustion::small_write_new_chunk_repair_exhaustion_preserves_sealed_predecessor", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::batching::small_write_scales_out_on_queued_bytes_then_scales_in_when_empty", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::queue_pressure::small_write_scales_out_on_queued_object_count", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.small_flow", "chunk_client::small_flow::queue_pressure::small_write_carry_does_not_underflow_queue_pressure", chunkdb_storage=True, storage_backend="mem"),
    Case("chunk-client.benchmark", "chunk_client::benchmark::small_write_benchmark_uses_real_metadata_and_null_disk", chunkdb_storage=True, storage_backend="null"),
    Case("chunk-client.benchmark", "chunk_client::benchmark::mixed_read_benchmark_prepares_and_verifies_real_frames", chunkdb_storage=True, storage_backend="mem"),

    Case("chunk-client.small", "chunk_client::small::shared_object_spans_mirror_strips_with_one_location", chunkdb_storage=True),
    Case("chunk-client.small", "chunk_client::small::cancelled_shared_object_keeps_pipeline_available", chunkdb_storage=True),
    Case("chunk-client.small", "chunk_client::small::small_write_rotates_strips_and_chunks_without_splitting_objects", chunkdb_storage=True),

    Case("chunk-client.large", "chunk_client::large::large_write_multi_strip_persists_data_metadata_and_parity", chunkdb_storage=True),
    Case("chunk-client.large", "chunk_client::large::large_two_copy_mirror_reads_across_strips", chunkdb_storage=True),
    Case("chunk-client.large", "chunk_client::large::large_raw_buffers_submit_complete_mirror_strips_concurrently", chunkdb_storage=True),
    Case("chunk-client.large", "chunk_client::large::large_write_rotates_chunks_without_losing_data", chunkdb_storage=True),
    Case("chunk-client.large", "chunk_client::large::large_write_unknown_size_partial_tail_is_durable", chunkdb_storage=True),
)
CHUNK_KV_CASES = (
    Case("chunk-kv.protocol", "chunk_kv::production_transport_calls_direct_owner_rpc", chunk_kv_empty_catalog=True),
    Case("chunk-kv.recovery", "chunk_kv::recovery::routed_native_records_and_tombstones_survive_complete_service_restart", restart=True),
)
S3_CASES = (
    Case("s3.protocol", "s3.protocol::test_aws_chunked_trailers_are_verified_before_publication", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_aws_cli_workflow", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_basic_bucket_object_matrix", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_batch_delete_preserves_exact_keys_and_quiet", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_batch_delete_rejects_entire_invalid_request", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_batch_delete_thousand_keys_and_unversioned_retry", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_concurrent_overwrite_delete_and_get_are_portable", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_copy_captures_source_before_overwrite_and_delete", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_default_boto3_checksums_and_multipart", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_fragmentation_and_storage_boundaries", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_independent_frontends_share_one_namespace", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_invalid_user_metadata_preserves_objects_and_sessions", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_multipart_copy_selects_ranges_and_replaces_parts", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_multipart_replaces_parts_and_publishes_selected_bytes", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_ordinary_put_size_matrix", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_presigned_transfers_tamper_and_expiry", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_rclone_workflow", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_server_side_copy_preserves_bytes_and_supported_metadata", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_signed_raw_http_wire_contract", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_slow_response_reader_keeps_full_object_consistent", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_slow_signed_upload_releases_native_buffers", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_truncated_signed_upload_does_not_publish_and_releases_credit", sdk_client=True),
    Case("s3.protocol", "s3.protocol::test_user_metadata_publication_copy_and_multipart", sdk_client=True),
)
S3_RECOVERY_CASES = tuple(Case("s3.recovery", "s3.recovery::" + service, sdk_client=True, s3_recovery=service)
                          for service in SERVICE_NODES)
S3_SPECIAL_CASES = tuple(Case("s3.nodes", f"access_s3::node_failure::node_{node}_outage_preserves_existing_and_new_objects", s3_node_fault=True)
                        for node in ("one", "two", "three")) + (
                    Case("s3.recovery", "access_s3::node_failure::protected_restart_preserves_objects_and_fixed_endpoints", s3_node_fault=True),
                    Case("s3.policy", "access_s3::protocol_http::combined_http_listeners_keep_protocol_chunk_policies_separate", access_protocol=True),
                    Case("s3.policy", "access_s3::policy::s3_and_iceberg_keep_distinct_policies_on_protected_storage", s3_policy=True),
                    Case("s3.policy", "access_s3::policy_failure::protected_two_copy_write_stops_after_repair_and_chunk_rotation_fail", s3_policy=True, diskio_process_exit=True),
                    Case("s3.credentials", "s3.credentials::release_cli_issue_ensure_lookup", sdk_client=True, s3_script="admin"),
                    Case("s3.benchmark", "access_s3::benchmark::direct_chunk_path"),
                    Case("s3.responses", "s3.responses::lost_put_and_multipart_replies_are_idempotent",
                        sdk_client=True, s3_script="lost_reply"),
                    Case("s3.benchmark", "s3.benchmark::s3_request_path", sdk_client=True, s3_script="benchmark"))
S3_LANGUAGE_CASES = tuple(Case("s3.sdk", "s3.sdk::" + language, sdk_client=True, s3_language=language)
                          for language in ("java", "js", "go"))
S3_SPECIAL_CASES += (
    Case("s3.native", "access_s3::native::complete_corpus_survives_storage_restart_without_access_or_credentials", restart=True),
    Case("s3.native.parity", "access_s3::native::parity::http_and_native_share_bytes_metadata_conditions_copy_and_multipart", access_protocol=True),
)
ICEBERG_CASES = (
    Case("iceberg.files.listing", "access_iceberg::file_http::listing::native_list_objects_v2_serves_selected_files_and_rejects_foreign_scope", iceberg_deferred_access=True),
    Case("iceberg.files.listing", "access_iceberg::file_http::listing::official_pyarrow_native_fileio_creates_exact_files_and_lists_table_prefixes", iceberg_deferred_access=True),
    Case("iceberg.sdk.rck", "access_iceberg::rck::apache_rest_compatibility_kit_supported_catalog_surface", client_timeout=960),
    Case("iceberg.benchmark", "access_iceberg::file_http::profiles::large::five_mib_profile", iceberg_deferred_access=True),
    Case("iceberg.benchmark", "access_iceberg::file_http::profiles::small_file_profiles", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::capacity::full_simulated_disk_preserves_file_authority_then_recovers_after_compaction", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::concurrency::thirty_two_concurrent_direct_puts", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::lifecycle::native_file_credentials_refresh_expire_and_follow_lifecycle_fences", iceberg_deferred_access=True),
    Case("iceberg.sdk.java", "access_iceberg::file_http::java_sdk::official_java_s3_fileio_uploads_and_reads_native_files", iceberg_deferred_access=True, client_timeout=600),
    Case("iceberg.sdk.java", "access_iceberg::file_http::java_sdk::official_java_catalog_commits_native_parquet_snapshots_and_staged_tables", iceberg_deferred_access=True, client_timeout=1200),
    Case("iceberg.sdk.java", "access_iceberg::file_http::java_sdk::official_java_opaque_metadata_publication_preserves_valid_data_and_delete_reads", iceberg_deferred_access=True, client_timeout=600),

    Case("iceberg.files", "access_iceberg::file_http::publication::signed_standard_put_get_and_multipart_publish_unbound_files", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::direct_profile::repeated_100_mib_put_streams_without_client_payload_copy", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::multipart_profile::repeated_100_mib_multipart_upload_profile", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::restart::multipart_100_mib_survives_restart_and_complete_replay", iceberg_deferred_access=True),

    Case("iceberg.files", "access_iceberg::file_http::streaming::ordinary_put_size_matrix_streams_and_reads_ranges", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::streaming::slow_socket_resumes_upload_after_writer_drains", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::routing::small_routing_is_strict_at_the_strip_threshold", iceberg_deferred_access=True),

    Case("iceberg.files", "access_iceberg::file_http::cleanup::concurrent_multipart_creates_use_independent_sessions", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::cleanup::explicit_cleanup_supports_single_batch_and_same_path_reupload", iceberg_deferred_access=True),
    Case("iceberg.files", "access_iceberg::file_http::cleanup::explicit_cleanup_rejects_a_retained_metadata_file", iceberg_deferred_access=True),

    Case("iceberg.sdk.rust", "access_iceberg::rust_sdk::official_rust_client_namespace_and_table_lifecycle", iceberg_deferred_access=True, client_timeout=600),
    Case("iceberg.gc", "access_iceberg::gc::foreground::official_sdk_foreground_progresses_under_gc_backlog", iceberg_gc=True, iceberg_gc_page_items=1, iceberg_gc_interval_ms=None, client_timeout=180),
    Case("iceberg.gc", "access_iceberg::gc::convergence::expired_inline_backlog_completes_with_foreground_progress", iceberg_gc=True, iceberg_gc_page_items=1, iceberg_gc_interval_ms=None, client_timeout=3600),
    Case("iceberg.gc", "access_iceberg::gc::notifications::mixed_external_and_tree_notifications_complete_without_reclaiming_live_ranges", iceberg_gc=True, iceberg_gc_page_items=1, iceberg_gc_interval_ms=None, client_timeout=600),
    Case("iceberg.gc", "access_iceberg::gc::notifications::matrix::restarted_cleaners_finish_large_and_sparse_backlogs_beside_an_invalid_catalog", iceberg_gc=True, iceberg_gc_page_items=1, iceberg_gc_interval_ms=None, iceberg_gc_catalogs=("01010101010101010101010101010101",), client_timeout=900),
    Case("iceberg.sdk.rust", "access_iceberg::rust_sdk::official_rust_client_lost_reply_survives_native_storage_restart", iceberg_deferred_access=True, client_timeout=1200),
    Case("iceberg.sdk.rust", "access_iceberg::rust_retired::official_rust_client_rejects_retired_native_catalog_after_full_grace", iceberg_deferred_access=True, client_timeout=1500),
    Case("iceberg.gc", "access_iceberg::gc::authenticated_gc_controls_survive_separate_processes", iceberg_gc=True),
    Case("iceberg.gc", "access_iceberg::gc::enabled_scheduler_admits_durable_purge_markers_once", iceberg_gc=True),
    Case("iceberg.gc", "access_iceberg::gc::enabled_scheduler_admits_and_advances_completed_clear", iceberg_gc=True, iceberg_gc_page_items=1),
    Case("iceberg.catalog", "access_iceberg::catalog::catalog_recovery_survives_real_chunk_kv_restart", iceberg_catalog_lifecycle=True),
    Case("iceberg.files", "access_iceberg::native_files::native_file_tree_publication_and_ranges_survive_catalog_storage_restart", iceberg_native_files=True),
    Case("iceberg.metadata", "access_iceberg::metadata::metadata_create_and_commit_stay_within_operation_budget"),
    Case("iceberg.namespace", "access_iceberg::namespace::namespace_functional_crud_survives_native_storage_and_listener_restart"),
)
CONSOLE_OPERATION_CASES = (
    Case("console.native.browser.dataset", "console::browser_dataset::browse", native_client=True, client_timeout=360),
    Case("console.native.browser.dataset", "console::browser_dataset::import", native_client=True, client_timeout=360),
    Case("console.native.browser.dataset", "console::browser_dataset::split_revisions", native_client=True, client_timeout=360),
    Case("console.native.browser.dataset", "console::browser_dataset::recovery", native_client=True, client_timeout=360),
    Case("console.native.browser.dataset", "console::browser_dataset::capabilities", native_client=True, client_timeout=360),

    Case("console.native.browser.six-node", "console::browser_data::six_node_round_trips", native_browser="UI creates three storage nodes, adds three diskless nodes, then KV, S3 and Iceberg persist real data", native_disk_gib=(4096, 4096, 4096, 8, 8, 8), native_zone_gib=(32, 32, 32, 1, 1, 1), native_admission_count=3, native_diskless_nodes=(4, 5, 6), native_bootstrap=False, native_observations=True, client_timeout=600),
    Case("console.native.browser.topology", "console::browser_topology::multi_rack_leaders", native_browser="native topology: three racks grow one replica and retain three complete leaders", native_rack_ids=(1, 2, 3)),
    Case("console.native.browser.topology", "console::browser_topology::independent_subsets", native_browser="native topology: overlapping three-voter subsets retain independent keys and scans", native_disk_gib=(80, 8, 8, 8, 8), native_zone_gib=(1, 1, 1, 1, 1)),
    Case("console.native.browser.topology", "console::browser_topology::simple_and_complex", native_browser="native topology: SIMPLE and COMPLEX store layouts route every populated group"),
    Case("console.native.browser.retry", "console::browser_plan::fixed_ports_and_failed_retry", native_browser="native plans: fixed ports isolate three nodes and failed DiskDB retries preserve identity", native_observations=True),
    Case("console.browser.lifecycle", "console::browser_lifecycle::standalone_kv_and_cascade", shell_browser="standalone release KV lifecycle preserves node records and cascades owned processes", shell_observations=True),
    Case("console.browser.registry", "console::browser_registry::selected_services_before_group0", shell_browser="persists all six selected services but starts only PKV before Group 0", shell_observations=True),
    Case("console.browser.discovery", "console::browser_discovery::stale_provider_observations", shell_browser="discovery: release monitor observations remain visible and stale after provider loss"),
    Case("console.native.geometry", "console::native_geometry::mixed_units_reject_consumers_without_starting_services", native_client=True),
    Case("console.native.browser.hardware", "console::native_browser::hardware_capacity_and_dashed_status", native_client=True, native_auto_bootstrap=True, native_supplied_candidates=True, client_timeout=360),
    Case("console.native.browser.maintenance", "console::native_browser::disk_maintenance_and_status_badges", native_client=True, client_timeout=360),
    Case("console.native.browser.disk", "console::native_browser::disk_inspector_and_selection", native_client=True, native_disk_gib=(4096, 8, 8), native_zone_gib=(32, 1, 1), native_spare_disk_gib=4096, client_timeout=360),
    Case("console.native.browser.binding", "console::browser_capacity::binding_recommendations", native_browser="KV binding recommends the least loaded group and initialization creates Group 1", native_bootstrap=False),
    Case("console.native.browser.diskdb", "console::browser_lifecycle::diskdb_and_kv_independence", native_browser="full deploy flow: deploy diskdb via UI, restart, stop, delete via context menu", native_observations=True),
    Case("console.native.browser.crud", "console::browser_capacity::mounted_crud_and_owner_binding", native_browser="native capacity: disk-group CRUD and explicit owner binding use mounted storage", native_spare_disk_gib=4096),
    Case("console.native.browser.shell", "console::browser_shell::defaults_and_tree", native_browser="shell defaults and tree interactions distinguish live and stopped release nodes", native_disk_gib=(80, 8, 8, 8), native_zone_gib=(1, 1, 1, 1)),
    Case("console.native.browser.transition", "console::native_browser::production_split_and_transfer_journals", native_client=True, native_disk_gib=(256, 256, 256), client_timeout=1020),
    Case("console.native.browser.lifecycle", "console::native_browser::auxiliary_process_menus", native_client=True, client_timeout=360),
    Case("console.native.browser.defaults", "console::native_browser::auxiliary_dialog_defaults", native_client=True, client_timeout=360),
    Case("console.native.browser.plan", "console::native_browser::interrupted_plan_reconciliation", native_client=True, client_timeout=360),
    Case("console.native.browser.prerequisites", "console::native_browser::waiting_plan_prerequisites", native_client=True, client_timeout=360),
    Case("console.native.browser.storage-windows", "console::native_browser::storage_windows_and_access_recovery", native_client=True, native_disk_gib=(80, 256, 256), client_timeout=360),
    Case("console.native.browser.scanner", "console::native_browser::scanner_and_scoped_recalc", native_client=True, client_timeout=360),
    Case("console.native.browser.journal-windows", "console::native_browser::large_journal_records_and_restart", native_client=True, client_timeout=480),
    Case("console.native.recovery", "console::native_recovery::all_typed_restarts_preserve_objects_and_keep_one_live_process", native_client=True),
    Case("console.native.load", "console::native_load::production_load_keeps_heartbeats_and_durable_bytes_across_owner_restart", native_client=True),
    Case("console.native.inspection", "console::native_browser::page_layout_and_iceberg_inspection", native_client=True, native_disk_gib=(80, 256, 256), client_timeout=3840),
    Case("console.native.browser.ownership", "console::native_browser::chunk_ownership_scope", native_client=True, client_timeout=360),
    Case("console.native.browser.journal", "console::native_browser::journal_identity_and_owner_recovery", native_client=True, client_timeout=360),
    Case("console.native.browser.s3", "console::native_browser::multipart_upload_and_physical_navigation", native_client=True, client_timeout=360),
    Case("console.native.s3", "console::native_s3::multipart_locations_bind_actual_chunks_and_reject_stale_cursors", native_client=True),
    Case("console.native.binding", "console::native_binding::prebound_groups_acquire_live_owners_and_require_explicit_binding_repair", native_client=True),
    Case("console.browser.registry", "console::browser_registry::duplicate_ids", shell_browser="rejects duplicate rack and node IDs from the add dialogs"),
    Case("console.browser.node", "console::browser_node::replica_navigation", native_browser="cross-jumps between views and shows local + remote replicas"),
    Case("console.browser.node", "console::browser_node::store_navigation", native_browser="physical node with store shows cross-jump to logical store"),
    Case("console.browser.logical", "console::browser_logical::create", native_browser="creates stores, groups and replicas through the UI against a real deployed server"),
    Case("console.browser.logical", "console::browser_logical::delete", native_browser="deletes a replica and a group through the UI and verifies the real backend"),
    Case("console.browser.inspector", "console::browser_inspector::activity", shell_browser="records mutations and async operations, and clear empties the log"),
    Case("console.browser.inspector", "console::browser_inspector::selection", shell_browser="clears stale properties when the domain changes or the cluster resets"),
    Case("console.cli.container", "console::cli_container::pending_free_accounting", oci_reference="docker-one", cli_workload=True),
    Case("console.cli.container", "console::cli_container::confirmed_group_zero_replay", cli_replay="bound"),
    Case("console.cli.container", "console::cli_container::confirmed_storage_replay", cli_replay="storage"),
    Case("console.cli.container", "console::cli_container::terminal_chunk_writer_recovery", cli_replay="terminal-writer"),
    Case("console.cli.container", "console::cli_container::plain_docker_target_attachment", cli_replay="attachment"),
    Case("console.cli.container", "console::cli_container::observation_round_trip", cli_replay="observation"),
    Case("console.cli.container", "console::cli_container::hardware_round_trip", cli_replay="hardware"),
    Case("console.cli.container", "console::cli_container::logical_round_trip", cli_replay="logical"),
    Case("console.cli.container", "console::cli_container::data_round_trip", cli_replay="data"),
    Case("console.managed", "console::managed::authority_outage", console_browser="Node authority outage rejects mutations and marks retained observations stale"),
    Case("console.managed", "console::managed::hardware_and_user_flows", console_browser="Managed native KV, Iceberg, S3 and Chunk operations retain hardware boundaries"),
    Case("console.browser.kv", "console::browser_kv::pagination", shell_browser="Group selection opens data directly; pages replace rows and expose complete values"),
    Case("console.browser.kv", "console::browser_kv::binary_bytes", shell_browser="binary byte runs retain printable fields and use distinct uppercase hex without a prefix"),
    Case("console.browser.kv", "console::browser_kv::auto_scan", shell_browser="auto-scan fires on initial group selection (G-0 shows data without manual Scan)"),
    Case("console.browser.canvas", "console::browser_canvas::fit", shell_browser="Fit All is available in every view and autofit centers nodes on load"),
    Case("console.browser.canvas", "console::browser_canvas::collapse", shell_browser="parent cards collapse children, retain nested state and keep context menus"),
    Case("console.browser.canvas", "console::browser_canvas::pan", shell_browser="clicking Fit All resets the viewport after panning"),
    Case("console.browser.canvas", "console::browser_canvas::navigation", shell_browser="switching views always fits to window, not the stale viewport"),
    Case("console.browser.registry", "console::browser_registry::empty", shell_browser="renders the SPA shell against a real empty backend"),
    Case("console.browser.registry", "console::browser_registry::datacenter", shell_browser="datacenter root wraps racks; Add Rack from datacenter context menu"),
    Case("console.browser.registry", "console::browser_registry::ownership_scope", shell_browser="keeps Cluster and unprovisioned Chunk selections free of ownership probes"),
    Case("console.browser.registry", "console::browser_registry::access_plan", shell_browser="persists an Access-only plan with four independent ports across reload"),
    Case("console.browser.registry", "console::browser_registry::slot_warning", shell_browser="fixed-slot warning stays healthy and persists across reload"),
    Case("console.browser.shell", "console::shell::embedding", shell_browser="embedding honors apiPrefix, readonly, and module opt-out"),
    Case("console.browser.shell", "console::shell::backend_unavailable", shell_browser="shows an alert when backend API requests fail"),
    Case("console.browser.shell", "console::shell::domain_toggle", shell_browser="domain toggle switches between seven domains and shared Cluster views"),
    Case("console.web.standalone", "console::web_diskio_discovery::diskio_discovers_disk_groups_created_after_web_deployment"),
    Case("console.web.wal", "console::web_leader_removal::remove_leader_from_five_node_group_elects_new_leader", stores=(0, 0, 0, 0, 0)),
    Case("console.web.wal", "console::web_leader_removal::remove_unreachable_leader_retains_group0_membership", stores=(0, 0, 0, 0, 0), preserve_network=True),
    Case("console.web.wal", "console::web_replicas::replica_add_remove_wires_peers_bidirectionally", stores=(0, 0)),
    Case("console.web.bootstrap", "console::web_sealed_bootstrap::bare_metal_bootstrap_consumes_sealed_topology_without_local_topology"),
    Case("console.web.standalone", "console::web_bootstrap::bootstrap_replays_data_group_without_changing_replica_or_publishing_invalid_bindings"),
    Case("console.web.standalone", "console::web_management::store_and_group_management_cycle_preserves_identity"),
    Case("console.web.standalone", "console::web_kv::put_get_delete_and_binary_values_through_web_routes"),
    Case("console.web.standalone", "console::web_metrics::structured_metrics_preserve_node_group_and_store_scopes"),
    Case("console.web.disposable", "console::web_lifetime::test_mode_termination_reaps_children_and_preserves_persistent_runtime"),
    Case("console.web.disposable", "console::web_lifetime::test_mode_forced_exit_recovers_owned_children_without_removing_persistent_data"),
    Case("console.web.recovery", "console::web_cluster_recovery::standalone_recovers_three_member_group0_and_diskdb"),
    Case("console.web.access", "console::web_access::native_access_deployment_provisions_private_credentials_and_reuses_them"),
    Case("console.web.wal", "console::web_wal_restart::restart_3node_1group", stores=(0, 0, 0)),
    Case("console.web.wal", "console::web_wal_restart::restart_5node_1group", stores=(0, 0, 0, 0, 0)),
    Case("console.web.wal", "console::web_wal_restart::restart_5node_2group", stores=(0, 0, 0, 0, 0)),
    Case("console.web.wal", "console::web_wal_restart::restart_6node_2group_overlap", stores=(0, 0, 0, 0, 0, 0)),
    Case("console.web.standalone", "console::web_wal_restart::restart_1node_1group"),
    Case("console.web.standalone", "console::web_kv_lifecycle::deploy_restart_and_stop_retains_fixed_ports_and_workspace"),
    Case("console.web.standalone", "console::web_isolation::survives_peer_fixture_retirement"),
    Case("console.web.lifecycle", "console::web_lifecycle::deploys_real_chunkdb_with_fixed_cluster_seeds_and_retained_launch"),
    Case("console.web.standalone", "console::web_recovery::standalone_recovers_kv_before_and_after_group0_init"),
    Case("console.web.registry", "console::web_registry::bare_metal_web_loads_launch_policy_without_restoring_topology"),
    Case("console.web.standalone", "console::web_standalone::empty_ui_persists_rack_and_node_across_restart"),
    Case("console.web.standalone", "console::web_standalone::malformed_standalone_config_is_preserved_and_rejected"),
    Case("console.web.catalog", "console::web_catalog::published_owner_change_invalidates_saved_catalog_and_runtime_cursors", console_web=True),
    Case("console.clients", "console::observation::formal_server_appears_healthy_in_aggregate_snapshot", stores_per_node=((),), groups=()),
    Case("console.web.slots", "console::web_slots::owner_windows_keep_disjoint_slots_generations_and_exact_ids", console_web=True),
    Case("console.discovery", "console::discovery::discover_registered_diskdb_instance", (0, 0, 0), True, groups=(0,)),
    Case("console.discovery", "console::discovery::chunk_kv_raw_observation_preserves_load_and_partition_state", (0, 0, 0), True, groups=(0,)),
    Case("console.discovery", "console::discovery::cache_hit_within_ttl", (0, 0, 0), True, groups=(0,)),
    Case("console.discovery", "console::discovery::invalidate_forces_refresh", (0, 0, 0), True, groups=(0,)),
    Case("console.discovery", "console::discovery::round_robin_selection", (0, 0, 0), True, groups=(0,)),
    Case("console.discovery", "console::discovery::empty_registry_returns_error", (0, 0, 0), True, groups=(0,)),
    Case("console.discovery", "console::discovery::discover_by_endpoint_found", (0, 0, 0), True, groups=(0,)),
    Case("console.discovery", "console::discovery::discover_by_endpoint_not_found", (0, 0, 0), True, groups=(0,)),
    Case("console.discovery", "console::discovery::zero_ttl_disables_caching", (0, 0, 0), True, groups=(0,)),
    Case("console.discovery", "console::discovery::expired_instance_filtered", (0, 0, 0), True, groups=(0,)),
    Case("console.hardware", "console::hardware::separate_consoles_confirm_matching_hardware_and_reject_conflicts", (0, 0, 0), True, groups=(0,)),
    Case("console.hardware", "console::hardware::concurrent_node_creates_preserve_both_rack_memberships", (0, 0, 0), True, groups=(0,)),
    Case("console.hardware", "console::hardware::committed_rack_survives_a_lost_conditional_write_response", (0, 0, 0), True, groups=(0,)),
    Case("console.hardware", "console::hardware::two_consoles_share_disk_groups_and_disks_without_local_topology", (0, 0, 0), True, groups=(0,)),
    Case("console.hardware", "console::hardware::committed_disk_group_survives_a_lost_response", (0, 0, 0), True, groups=(0,)),
    Case("console.logical", "console::group_publication::group_and_initial_replica_share_one_committed_revision", (0, 0, 0), True, groups=(0,)),
    Case("console.logical", "console::group_publication::lost_batch_response_confirms_complete_group_membership", (0, 0, 0), True, groups=(0,)),
    Case("console.logical", "console::group_publication::group_creation_prepares_a_local_store_on_new_nodes", (0, 0, 0), True, groups=(0,)),
    Case("console.logical", "console::group_publication::orphan_membership_is_not_overwritten_by_group_creation", (0, 0, 0), True, groups=(0,)),
    Case("console.logical", "console::logical_publication::concurrent_conflicting_store_publication_is_preserved", (0, 0, 0), True, groups=(0,)),
    Case("console.logical", "console::logical_publication::concurrent_matching_store_publication_is_confirmed", (0, 0, 0), True, groups=(0,)),
    Case("console.logical", "console::logical_publication::committed_store_is_confirmed_after_its_write_response_is_lost", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::bootstrap_publication::bootstrap_rejects_conflicting_hardware_without_overwriting_authority", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::bootstrap_publication::bootstrap_retry_preserves_known_rack_membership_and_node_runtime_fields", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::bootstrap_publication::bootstrap_preflights_logical_conflicts_before_publishing_missing_hardware", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::bootstrap_publication::bootstrap_rejects_a_different_rack_name_even_when_membership_matches", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::bootstrap_publication::bootstrap_publishes_shared_hardware_labels_and_credential_reference", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::bootstrap_publication::bootstrap_resumes_missing_records_and_preserves_committed_revisions", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::replica_cleanup::failed_group_creation_removes_new_store", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::replica_cleanup::failed_rollback_is_reported", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::replica_cleanup::automatic_replica_identity_exhaustion_is_validation_error", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::replica_cleanup::existing_replica_host_is_rejected_before_local_mutation", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_delete::store_deletion_cleans_descendants_and_reaches_later_replica_hosts", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_delete::group_deletion_cleans_replicas_and_preserves_siblings", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_delete::failed_store_deletion_preserves_all_authority_records", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_delete::failed_group_deletion_preserves_all_authority_records", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_delete::store_deletion_reconciles_a_lost_metadata_response", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_delete::group_deletion_reconciles_a_lost_metadata_response", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_fanout::peer_wiring_failure_rolls_back_without_publishing_membership", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_fanout::missing_peer_endpoint_rolls_back_without_publishing_membership", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_fanout::replica_creation_requires_every_existing_peer_registration", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_fanout::replica_creation_requires_every_existing_peer_endpoint", (0, 0, 0), True, groups=(0,)),
    Case("console.cleanup", "console::logical_fanout::replica_creation_rolls_back_when_new_endpoint_is_missing", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::bootstrap_intent::interrupted_bootstrap_restores_identity_then_clears_verified_intent", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::bootstrap_intent::committed_group_zero_is_verified_before_stale_intent_is_removed", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::bootstrap_intent::changed_bootstrap_topology_is_rejected_before_group_zero_mutation", (0, 0, 0), True, groups=(0,)),
    Case("console.deployment", "console::deployment::concurrent_admissions_allocate_unique_stable_ids_through_group_zero", (0, 0, 0), True, groups=(0,)),
    Case("console.deployment", "console::deployment::service_retries_retain_identity_and_other_nodes_cannot_claim_the_same_service", (0, 0, 0), True, groups=(0,)),
    Case("console.deployment", "console::deployment::authenticated_node_update_preserves_ids_and_converges_from_another_console", (0, 0, 0), True, groups=(0,)),
    Case("console.bootstrap", "console::cluster::clean_requires_confirmed_group_replicas", (0, 0, 0), True, groups=(0,)),



    Case("console.clients", "console::server_management::full_store_group_remote_cycle", stores_per_node=((),), groups=()),
    Case("console.clients", "console::server_data::put_get_delete_cycle", stores_per_node=((),), groups=()),

    Case("console.web.authority", "console::web_authority::bare_metal_hardware_routes_share_confirmed_group_zero_state", console_web=True),
    Case("console.web.authority", "console::web_authority::bare_metal_snapshot_requires_live_authority_without_a_docker_monitor", console_web=True),
    Case("console.web.authority", "console::web_authority::snapshot_validates_later_replica_hosts_as_well_as_original_store_hosts", console_web=True),
)

DATASET_CASES = (
    Case("dataset.sdk", "dataset::sdk_real_payload_restart",dataset_sdk=True,client_timeout=600),
    Case("dataset.sdk", "dataset::sdk_split_revision_round_trip",dataset_sdk=True,client_timeout=600),
    Case("dataset.http", "access_dataset_split::split_commit_restart_preserves_candidates_receipts_and_historical_members",client_timeout=600),
    Case("dataset.http", "access_dataset::listener_authenticates_and_serves_create_open"),
    Case("dataset.http", "access_dataset::publish_persist_restart_and_scan_round_trip"),
)

ALL_CASES = CONSOLE_OPERATION_CASES + DATASET_CASES + ICEBERG_CASES + S3_LANGUAGE_CASES + S3_SPECIAL_CASES + S3_RECOVERY_CASES + S3_CASES + CHUNK_KV_CASES + CHUNK_CLIENT_CASES + CHUNK_STREAM_CASES + CHUNKDB_CASES + KV_CASES + DISKDB_CASES + DEPLOYMENT_CASES + DISKIO_CASES

MANUAL_CASES = (
    Case("iceberg.manual.tpc", "access_iceberg::tpc::parallel_loader_stress", client_timeout=3600),
)

def select_cases(layer="all", scope="all", case_name=None, partition=None):
    from ci_partitions import partition_for
    candidates = MANUAL_CASES if scope == "iceberg.manual.tpc" else ALL_CASES
    return tuple(case for case in candidates
                 if (layer == "all" or case.scope.startswith(layer + "."))
                 and (scope == "all" or case.scope == scope or case.scope.startswith(scope + ".")
                      or (scope == "console.operations" and case in CONSOLE_OPERATION_CASES))
                 and (not case_name or case.name == case_name)
                 and (not partition or partition_for(case) == partition))


def case_nodes(case):
    if case.oci_reference:
        return 3 if case.oci_shortcuts else 4 if case.oci_address_recovery else 1 if case.oci_reference.endswith("-one") else 3
    if case.scope == "console.browser.discovery":
        return 2
    if case.native_browser or case.native_client:
        return len(case.native_disk_gib)
    if case.cli_replay:
        return 3
    if case.shell_browser:
        return 1
    if case.scope == "console.web.bootstrap":
        return 2
    if case.scope == "console.web.disposable":
        return 1
    if case.scope == "console.web.recovery":
        return 4
    if case.scope == "console.web.access":
        return 13
    if case.s3_node_fault:
        return 14
    if case.scope == "console.web.wal":
        return len(case.stores) + 1
    if case.scope == "console.web.lifecycle":
        return 4
    if case.scope in ("console.web.standalone", "console.web.registry"):
        return 1
    if case.scope == "deployment.listeners":
        return 2
    if case.console_web:
        return 5
    if case.console_browser or case.service_selection or (case.scope.startswith("deployment.") and case.scope != "deployment.hardware"):
        return 1
    if case.access_protocol or case.scope.startswith("dataset."):
        return 13
    if case.scope.startswith("iceberg."):
        return 14
    if case.scope == "s3.native":
        return 12
    if case.scope.startswith("s3."):
        return 14
    if case.scope.startswith("chunk-kv."):
        return 12
    if case.chunkdb_multi:
        return 7
    if case.chunkdb_storage:
        return 11
    if case.chunkdb_server:
        return 5
    if case.diskio_backend == "semantic":
        return 4
    if case.diskio_backend:
        return 2
    return 2 if case.scope.startswith(("diskdb.", "chunkdb.")) else len(case.stores_per_node or case.stores)

def case_resources(case, node_cpus):
    if case.oci_reference:
        count = 3 if case.oci_shortcuts else 1 if case.oci_reference.endswith("-one") else 3
        return count + 0.5 + (0.25 if case.oci_address_recovery else 0), count * 1536 + 512 + (768 if case.oci_address_recovery else 0)
    # CPU shares allow nodes to borrow idle capacity; memory remains a hard bound.
    if case.scope == "console.browser.discovery":
        return 1.5, 3072
    if case.cli_replay or case.native_browser or case.native_client:
        count = case_nodes(case)
        return 1.5, 2048 * count + 512
    if case.shell_browser:
        return 1.5, 2560
    if case.scope == "iceberg.manual.tpc":
        return 1.5, (case_nodes(case) + 1) * 512 + 8192
    if case.scope == "deployment.listeners":
        return 1.5, 3072
    if case.scope == "iceberg.sdk.rck":
        return 1.5, case_nodes(case) * 512 + 4096
    if case.console_browser or len(case.service_selection) > 2:
        return 1.5, 2560
    nodes = case_nodes(case)
    retained_networks = (len(case.stores) if case.preserve_network else
                         12 if case.s3_node_fault else 6 if case.diskio_process_exit else 0)
    return 1.5, (nodes + 1) * 512 + retained_networks * 16


def allocate_cluster(case, image, artifacts, bundle, data_root):
    if case.oci_reference:
        return OciReferenceCluster(image, artifacts, bundle, reference=case.oci_reference,
                                   editable_ui=case.oci_editable_ui, cli_workload=case.cli_workload,
                                   address_recovery=case.oci_address_recovery, shortcuts=case.oci_shortcuts, data_root=data_root)
    if case.scope == "console.browser.discovery":
        return DiscoveryBrowserCluster(image, artifacts, bundle, data_root=data_root)
    if case.native_browser or case.native_client:
        return NativeConsoleCluster(image, artifacts, bundle, disk_gib=case.native_disk_gib,
                                    zone_gib=case.native_zone_gib,
                                    spare_disk_gib=case.native_spare_disk_gib,
                                    supplied_candidates=case.native_supplied_candidates,
                                    bootstrap=case.native_bootstrap,
                                    auto_bootstrap=case.native_auto_bootstrap,
                                    rack_ids=case.native_rack_ids,
                                    admission_count=case.native_admission_count,
                                    diskless_nodes=case.native_diskless_nodes,
                                    data_root=data_root)
    if case.cli_replay:
        if case.cli_replay in ("attachment", "observation", "hardware", "logical", "data"):
            return CliAttachedCluster(image, artifacts, bundle, data_root=data_root)
        return CliContainerCluster(image, artifacts, bundle, checkpoint=case.cli_replay, data_root=data_root)
    if case.shell_browser:
        return ShellBrowserCluster(image, artifacts, bundle, data_root=data_root)
    if case.scope == "console.web.bootstrap":
        return BootstrapWebCluster(image, artifacts, data_root=data_root)
    if case.scope == "console.web.disposable":
        return DisposableWebCluster(image, artifacts, data_root=data_root)
    if case.scope == "console.web.recovery":
        return RecoveringWebCluster(image, artifacts, bundle, data_root=data_root)
    if case.scope == "console.web.access":
        return AccessDeploymentCluster(image, artifacts, bundle, data_root=data_root)
    if case.scope == "console.web.wal":
        return WalWebCluster(image, artifacts, len(case.stores), data_root=data_root,
                             preserve_network=case.preserve_network)
    if case.scope == "console.web.lifecycle":
        return TypedWebCluster(image, artifacts, data_root=data_root)
    if case.scope == "console.web.registry":
        return RegistryWebCluster(image, artifacts, data_root=data_root)
    if case.scope == "console.web.standalone":
        return StandaloneWebCluster(image, artifacts, rejected="malformed" in case.name, data_root=data_root)
    if case.scope == "deployment.listeners":
        protocol = "iceberg" if "fixed_iceberg" in case.name else "s3"
        return FixedListenerAccessCluster(image, artifacts, protocol, services=case.service_selection, data_root=data_root)
    if case.scope == "deployment.browser":
        return DeploymentBrowserCluster(image, artifacts, bundle, services=case.service_selection, data_root=data_root)
    if case.scope == "deployment.anonymous":
        return AnonymousReleaseCluster(image, artifacts, services=case.service_selection, data_root=data_root)
    if case.scope == "deployment.failures":
        return DeploymentFailureCluster(image, artifacts, services=case.service_selection, data_root=data_root)
    if case.scope == "deployment.clients":
        return DeploymentClientsCluster(image, artifacts, services=case.service_selection, data_root=data_root)
    if case.scope == "deployment.validation":
        kind = "profile" if "invalid_profile" in case.name else "policy"
        return RejectedBootstrapCluster(image, artifacts, kind, data_root=data_root)
    if case.scope == "deployment.bootstrap":
        return InterruptedBootstrapCluster(image, artifacts, services=case.service_selection, data_root=data_root)
    if case.console_web:
        return ConsoleWebCluster(image, artifacts, data_root=data_root)
    if case.scope.startswith("dataset."):
        return AccessDatasetCluster(image, artifacts, bundle, data_root=data_root)
    if case.console_browser:
        return ConsoleCluster(image, artifacts, bundle, data_root=data_root)
    if case.scope.startswith("iceberg."):
        return AccessIcebergCluster(image, artifacts, bundle, native_files=case.iceberg_native_files, catalog_lifecycle=case.iceberg_catalog_lifecycle,
                                    gc_worker=case.iceberg_gc, gc_page_items=case.iceberg_gc_page_items,
                                    gc_interval_ms=case.iceberg_gc_interval_ms,
                                    gc_catalogs=case.iceberg_gc_catalogs,
                                    deferred_access=case.iceberg_deferred_access,
                                    data_root=data_root)
    if case.access_protocol:
        return AccessProtocolCluster(image, artifacts, bundle, data_root=data_root)
    if case.s3_node_fault:
        return S3NodeCluster(image, artifacts, bundle, data_root=data_root)
    if case.s3_policy:
        return ChunkKvCluster(image, artifacts, bundle, service_instances=(11, 12, 13),
                              storage_groups=(1, 2, 3), diskio_process_exit=case.diskio_process_exit,
                              data_root=data_root)
    if case.scope == "s3.native":
        return NativeS3Cluster(image, artifacts, bundle, data_root=data_root)
    if case.scope.startswith("s3."):
        return AccessS3Cluster(image, artifacts, bundle, data_root=data_root)
    if case.scope.startswith("chunk-kv."):
        return ChunkKvCluster(image, artifacts, bundle, empty_catalog=case.chunk_kv_empty_catalog,
                              data_root=data_root)
    if case.chunkdb_multi:
        return ChunkdbCluster(image, artifacts, bundle, service_instances=(11, 12, 13),
                              storage_groups=(1, 2, 3), data_root=data_root)
    if case.chunkdb_storage:
        return ChunkdbStorageCluster(image, artifacts, bundle, storage_backend=case.storage_backend,
                                     fault_rate=case.diskio_fault_rate,
                                     diskio_process_exit=case.diskio_process_exit, data_root=data_root)
    if case.chunkdb_server:
        return ChunkdbCluster(image, artifacts, bundle, data_root=data_root)
    if case.scope.startswith("deployment.") and case.scope != "deployment.hardware":
        return ManagedCluster(image, artifacts, services=case.service_selection or ("kv",), data_root=data_root)
    if case.diskio_backend == "semantic":
        return DiskioSemanticCluster(image, artifacts, bundle, data_root=data_root)
    if case.diskio_backend:
        return DiskioDiscoveryCluster(image, artifacts, bundle, case.diskio_backend,
                                      fault_rate=case.diskio_fault_rate, data_root=data_root)
    if case.scope.startswith("diskio."):
        return DiskioCluster(image, artifacts, direct=case.diskio_direct, data_root=data_root)
    if case.scope.startswith(("diskdb.", "chunkdb.")):
        return DiskdbCluster(image, artifacts, bundle, restart_metadata=case.diskdb_restart_metadata,
                             data_root=data_root)
    return Cluster(image, artifacts, data_root=data_root,
                   node_count=len(case.stores_per_node or case.stores),
                   store_ids=case.stores, wire_replicas=case.wired,
                   stores_per_node=case.stores_per_node, groups=case.groups,
                   groups_per_node=case.groups_per_node, node_args=case.node_args,
                   advertise=case.advertise)


def run_cases(image, artifacts, bundle, slots, data_root=None, scope="all", case_name=None, layer="all", node_cpus=1.0, partition=None, fail_fast=False, resource_budget=None):
    selected = select_cases(layer, scope, case_name, partition)
    if not selected:
        raise ValueError(f"no migrated cases for scope {scope}")

    def prepare(case):
        cluster = allocate_cluster(case, image, artifacts, bundle, data_root)
        cluster.case_name = case.name
        cluster.client_timeout = case.client_timeout
        if case.native_browser:
            cluster.browser_timeout = case.client_timeout
        cluster.node_cpus = node_cpus
        (cluster.artifacts / "case.json").write_text(json.dumps({
            "scope": case.scope, "case": case.name,
            "scheduled_at_unix_ms": time.time_ns() // 1_000_000,
            "resources": dict(zip(("cpus", "memory_mib"), case_resources(case, node_cpus)))}, indent=2))
        return cluster

    stopped = threading.Event()

    def run(case, owned=None):
        if owned is None and stopped.is_set():
            return
        try:
            execute(case, owned)
        except Exception:
            if fail_fast:
                stopped.set()
            raise

    def execute(case, owned=None):
        cluster = owned if owned is not None else prepare(case)
        with nullcontext(cluster) if owned is not None else cluster:
            if case.oci_reference:
                cluster.verify_reference()
            elif case.cli_replay:
                cluster.replay(case.cli_replay)
            elif case.native_browser:
                config = "native.config.ts" if case.scope.startswith("console.native.") else "shell.config.ts"
                if case.native_observations:
                    cluster.observed_browser(case.native_browser, config=config)
                else:
                    cluster.browser(case.native_browser, config=config)
            elif case.shell_browser:
                if case.shell_observations:
                    cluster.observed_browser(case.shell_browser)
                else:
                    cluster.browser(case.shell_browser)
            elif case.scope == "deployment.browser":
                cluster.browser()
            elif case.console_browser:
                cluster.browser(case.console_browser)
            elif case.s3_recovery:
                run_recovery(cluster, case.s3_recovery)
            elif case.s3_language:
                cluster.language_client(case.s3_language)
            elif case.s3_script:
                cluster.script_client(case.s3_script)
            elif case.dataset_sdk:
                if case.name == "dataset::sdk_split_revision_round_trip":
                    cluster.sdk_split_roundtrip()
                else:
                    cluster.sdk_roundtrip()
            elif case.sdk_client:
                cluster.sdk_client(case.name.rsplit("::", 1)[-1])
            elif case.restart:
                cluster.client(bundle, "write", case.name)
                for data in cluster.data_paths:
                    for path in case.persisted_paths:
                        if not (data / path).is_dir():
                            raise AssertionError(f"acknowledged state did not persist at {data / path}")
                cluster.crash_restart()
                cluster.client(bundle, "verify", case.name)
            else:
                cluster.client(bundle, case.name.rsplit("::", 1)[-1], case.name)

    def stable_network(case):
        # CLI fixtures run immediately after startup in the earlier resource
        # phase, then finish before any browser batch creates its networks.
        return (case.native_client or case.native_browser or case.shell_browser
                or case.console_browser or case.oci_editable_ui or case.scope == "deployment.browser")

    budget = resource_budget or (float("inf"), float("inf"))
    resources = lambda case: case_resources(case, node_cpus)
    errors = run_with_resources([case for case in selected if not stable_network(case)],
                                slots, run, resources, budget, stopped)
    if not stopped.is_set():
        browsers = [case for case in selected if stable_network(case)]
        browser_slots = batch_slots(browsers, slots, resources, budget) if resource_budget else slots
        errors.extend(run_browser_batches(browsers, browser_slots, prepare, run, fail_fast=fail_fast))
    if errors:
        for error in errors[1:]:
            errors[0].add_note(f"another independent fixture failed: {error}")
        raise errors[0]
