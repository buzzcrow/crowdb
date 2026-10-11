# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Named flow scopes, fresh server profiles and bounded host-client dispatch."""

import concurrent.futures
import json
from dataclasses import dataclass

from fixture import Cluster
from diskdb_fixture import DiskdbCluster
from chunkdb_fixture import ChunkdbCluster, ChunkdbStorageCluster
from chunk_kv_fixture import ChunkKvCluster
from access_s3_fixture import AccessS3Cluster
from access_protocol_fixture import AccessProtocolCluster
from access_iceberg_fixture import AccessIcebergCluster
from access_dataset_fixture import AccessDatasetCluster
from s3_recovery import SERVICE_NODES, run_recovery
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
    chunkdb_server: bool = False
    chunkdb_storage: bool = False
    chunkdb_multi: bool = False
    storage_backend: str = "mem"
    chunk_kv_empty_catalog: bool = False
    sdk_client: bool = False
    s3_recovery: str | None = None
    s3_script: str | None = None
    s3_language: str | None = None
    s3_policy: bool = False
    access_protocol: bool = False
    iceberg_native_files: bool = False
    iceberg_catalog_lifecycle: bool = False
    iceberg_gc: bool = False
    iceberg_gc_page_items: int | None = None
    iceberg_deferred_access: bool = False
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
    Case("chunk-stream.objects", "chunk_stream::production_idle_writer_stops_after_ownership_takeover", chunkdb_storage=True),
    Case("chunk-stream.recovery", "chunk_stream::production_stream_recovers_exact_bytes_after_service_restarts", chunkdb_storage=True, storage_backend="file", restart=True),
    Case("chunk-stream.errors", "chunk_stream::production_stream_write_returns_after_diskio_failure", chunkdb_storage=True),
    Case("chunk-stream.errors", "chunk_stream::production_stream_write_returns_after_live_diskio_errors", chunkdb_storage=True, diskio_fault_rate=1.0),
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
    Case("chunk-client.small_flow", "chunk_client::small_flow::replica_repair::protected_read_reports_diskio_process_failure", chunkdb_storage=True, storage_backend="mem"),
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
S3_SPECIAL_CASES = (Case("s3.policy", "access_s3::protocol_http::combined_http_listeners_keep_protocol_chunk_policies_separate", access_protocol=True),
                    Case("s3.policy", "access_s3::policy::s3_and_iceberg_keep_distinct_policies_on_protected_storage", s3_policy=True),
                    Case("s3.credentials", "s3.credentials::release_cli_issue_ensure_lookup", sdk_client=True, s3_script="admin"),
                    Case("s3.benchmark", "access_s3::benchmark::direct_chunk_path"),
                    Case("s3.responses", "s3.responses::lost_put_and_multipart_replies_are_idempotent",
                        sdk_client=True, s3_script="lost_reply"),
                    Case("s3.benchmark", "s3.benchmark::s3_request_path", sdk_client=True, s3_script="benchmark"))
S3_LANGUAGE_CASES = tuple(Case("s3.sdk", "s3.sdk::" + language, sdk_client=True, s3_language=language)
                          for language in ("java", "js", "go"))
ICEBERG_CASES = (
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
    Case("iceberg.gc", "access_iceberg::gc::foreground::official_sdk_foreground_progresses_under_gc_backlog", iceberg_gc=True, iceberg_gc_page_items=1, client_timeout=180),
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
DATASET_CASES = (
    Case("dataset.http", "access_dataset::listener_authenticates_and_serves_create_open"),
    Case("dataset.http", "access_dataset::publish_persist_restart_and_scan_round_trip"),
)

ALL_CASES = DATASET_CASES + ICEBERG_CASES + S3_LANGUAGE_CASES + S3_SPECIAL_CASES + S3_RECOVERY_CASES + S3_CASES + CHUNK_KV_CASES + CHUNK_CLIENT_CASES + CHUNK_STREAM_CASES + CHUNKDB_CASES + KV_CASES + DISKDB_CASES + DEPLOYMENT_CASES + DISKIO_CASES

def case_nodes(case):
    if case.service_selection:
        return 1
    if case.access_protocol or case.scope.startswith("dataset."):
        return 13
    if case.scope.startswith("iceberg."):
        return 14
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
    if len(case.service_selection) > 2:
        return max(2, node_cpus) + 0.5, 2560
    nodes = case_nodes(case)
    return nodes * node_cpus + 0.5, (nodes + 1) * 512


def allocate_cluster(case, image, artifacts, bundle, data_root):
    if case.scope.startswith("dataset."):
        return AccessDatasetCluster(image, artifacts, bundle, data_root=data_root)
    if case.scope.startswith("iceberg."):
        return AccessIcebergCluster(image, artifacts, bundle, native_files=case.iceberg_native_files, catalog_lifecycle=case.iceberg_catalog_lifecycle,
                                    gc_worker=case.iceberg_gc, gc_page_items=case.iceberg_gc_page_items,
                                    deferred_access=case.iceberg_deferred_access,
                                    data_root=data_root)
    if case.access_protocol:
        return AccessProtocolCluster(image, artifacts, bundle, data_root=data_root)
    if case.s3_policy:
        return ChunkKvCluster(image, artifacts, bundle, service_instances=(11, 12, 13),
                              storage_groups=(1, 2, 3), data_root=data_root)
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
                                     fault_rate=case.diskio_fault_rate, data_root=data_root)
    if case.chunkdb_server:
        return ChunkdbCluster(image, artifacts, bundle, data_root=data_root)
    if case.scope.startswith("deployment."):
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


def run_cases(image, artifacts, bundle, slots, data_root=None, scope="all", case_name=None, layer="all", node_cpus=0.5):
    selected = tuple(case for case in ALL_CASES if (layer == "all" or case.scope.startswith(layer + ".")) and (scope == "all" or case.scope == scope or case.scope.startswith(scope + "."))
                     and (not case_name or case.name == case_name))
    if not selected:
        raise ValueError(f"no migrated cases for scope {scope}")

    def run(case):
        cluster = allocate_cluster(case, image, artifacts, bundle, data_root)
        cluster.case_name = case.name
        cluster.client_timeout = case.client_timeout
        cluster.node_cpus = max(2, node_cpus) if len(case.service_selection) > 2 else node_cpus
        (cluster.artifacts / "case.json").write_text(json.dumps({
            "scope": case.scope, "case": case.name}, indent=2))
        with cluster:
            if case.s3_recovery:
                run_recovery(cluster, case.s3_recovery)
            elif case.s3_language:
                cluster.language_client(case.s3_language)
            elif case.s3_script:
                cluster.script_client(case.s3_script)
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=slots) as pool:
        futures = [pool.submit(run, case) for case in selected]
        errors = []
        try:
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as error:
                    errors.append(error)
        except BaseException:
            for future in futures:
                future.cancel()
            raise
        if errors:
            for error in errors[1:]:
                errors[0].add_note(f"another independent fixture failed: {error}")
            raise errors[0]
