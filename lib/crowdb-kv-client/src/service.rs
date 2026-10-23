// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 service discovery, registry, and watch/notify.

pub mod discovery;
pub mod domain_monitor;
pub mod registry;
mod stream_lease;
pub mod watch_notify;

pub use discovery::ServiceDiscoveryClient;
pub use domain_monitor::DomainMonitorClient;
pub use registry::ServiceRegistryClient;
pub use stream_lease::{StreamProcessLeaseClient, StreamProcessLeaseError};
pub use watch_notify::{WatchNotify, WatchNotifyClient, WatchSubscription};
