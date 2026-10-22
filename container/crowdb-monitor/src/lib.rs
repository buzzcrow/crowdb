// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod bootstrap;
mod crash;
mod credentials;
mod layout;
mod liveness;
mod manifest;
mod monitor_log;
mod node;
#[cfg(feature = "test-util")]
mod preview;
mod probe;
mod process;
mod profile;
mod render;
mod status;
mod supervisor;

pub use bootstrap::{
    disk_step_names, ensure_disk_files, hardware_step_names, iceberg_step_names, kv_step_names,
    logical_step_names, s3_step_names, verify_chunk_services, verify_diskio_disks, ChunkBootstrapError,
    DiskBootstrapError, HardwareBootstrap, HardwareBootstrapError, IcebergBootstrap, IcebergBootstrapError,
    KvBootstrap, KvBootstrapError, LogicalBootstrap, LogicalBootstrapError, S3Bootstrap, S3BootstrapError,
    StorageProbeError,
};
pub use crash::CrashRetention;
pub use credentials::{show_client_credentials, ClientCredentials, CredentialError, ServerCredentials};
pub use liveness::{probe_liveness, LivenessError, LivenessServer};
pub use manifest::{BootstrapManifest, BootstrapSession, ManifestError, ManifestState};
pub use monitor_log::{MonitorEvent, MonitorEventKind, MonitorLog, MonitorLogError};
#[cfg(feature = "test-util")]
pub use node::unique_member_endpoints_for_tests;
#[cfg(feature = "test-util")]
pub use node::{
    accept_file_for_tests, accept_member_trust_for_tests, accept_startup_mode_for_tests,
    check_member_trust_for_tests, check_retained_medium_for_tests, file_operations_for_tests,
    member_trust_ready_for_tests, prepare_file_for_tests, prepare_supplied_disk_for_tests,
    publish_file_for_tests, runtime_observation_for_tests, verify_blank_medium_for_tests,
    InitializationForTests, NodeServiceForTests, NodeSetupForTests, SuppliedOperationsForTests,
};
pub use node::{
    accept_node_policy, control_node, discover_disks, read_node_policy, run_node, run_node_with_inputs,
    serve_node_management, CandidateCache, DiscoveryConfig, DiscoveryError, NodeDiscovery, NodeIdentity,
    NodeIdentityError, NodeManagementError, NodeRuntimeConfig, NODE_SERVICE_TYPE,
};
#[cfg(feature = "test-util")]
pub use preview::{preview_step_names_for_tests, run_preview_for_tests, PreviewError};
pub use probe::{ProbeError, ProbeExecutor};
pub use process::{ProcessError, ProcessManager};
pub use profile::{
    DeploymentProfile, DiskProfile, GroupProfile, GroupRole, LogProfile, NodeProfile, PathProfile, ProbeKind,
    ProbeProfile, ProfileError, PublicEndpoint, RestartProfile, ServiceProfile,
};
pub use render::{render_configs, RenderError, RenderedConfig};
pub use status::{MonitorPhase, MonitorStatus, ServiceStatus, StatusError, StatusStore};
pub use supervisor::{Supervisor, SupervisorError};

#[cfg(feature = "test-util")]
pub use node::rebind_recovery_services_for_tests;

pub use node::read_recovery_identity;
