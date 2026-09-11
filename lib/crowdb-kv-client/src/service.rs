// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 service discovery, registry, and watch/notify.

pub mod discovery;
pub mod registry;
pub mod watch_notify;

pub use discovery::ServiceDiscoveryClient;
pub use registry::ServiceRegistryClient;
pub use watch_notify::{WatchNotify, WatchNotifyClient, WatchSubscription};
