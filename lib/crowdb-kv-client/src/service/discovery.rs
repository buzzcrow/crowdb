// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

//! [`ServiceDiscoveryClient`]: group-0 service discovery with caching.
//!
//! Wraps [`ServiceRegistryClient`] with a per-service `DashMap` cache
//! and TTL-based refresh. Clients call `discover_all` / `discover_one`
//! to find living service instances by service name without hardcoding
//! addresses. The cache is poll-on-demand: the first call queries
//! group-0, subsequent calls within the TTL return the cached result.
//!
//! See `doc/design/kv/design-crowdb-kv-group0.md` §4.4.

use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use dashmap::DashMap;

use crowdb_protocol::common::InstanceValue;
use crowdb_protocol::common_type::InstanceId;

use crate::error::{Error, Result};
use crate::{CrowdbKvClient, ServiceRegistryClient};

/// Default cache TTL: 5 seconds. Matches the service heartbeat
/// interval — the discovery client sees new registrations within one
/// heartbeat cycle.
const DEFAULT_CACHE_TTL_MS: u64 = 5_000;

#[allow(clippy::cast_possible_truncation)]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[allow(clippy::cast_possible_truncation)]
fn duration_to_ms(ttl: Duration) -> u64 {
    ttl.as_millis() as u64
}

/// Cached discovery result for a single service.
struct CachedEntry {
    instances: Vec<(InstanceId, InstanceValue)>,
    refreshed_at_ms: u64,
}

#[derive(Default)]
struct ServiceState {
    cache: ArcSwapOption<CachedEntry>,
    rr_cursor: AtomicUsize,
    refresh: tokio::sync::Mutex<()>,
}

/// Client for discovering living service instances via the group-0
/// service registry. Caches results per service with a configurable
/// TTL. Round-robin selection for `discover_one`.
///
/// All reads target store 0, group 0 (group-0 sysdata). The wrapped
/// `ServiceRegistryClient` shares the same `Arc<CrowdbKvClient>` as
/// the rest of the application, so topology re-seeding and connection
/// pooling are shared.
#[derive(Clone)]
pub struct ServiceDiscoveryClient {
    svc: ServiceRegistryClient,
    /// Per-service cache, atomic cursor, and refresh single-flight state.
    services: Arc<DashMap<String, Arc<ServiceState>>>,
    /// Cache TTL in milliseconds.
    cache_ttl_ms: Arc<AtomicU64>,
}

impl ServiceDiscoveryClient {
    /// Wrap a `ServiceRegistryClient` for cached discovery.
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self {
            svc,
            services: Arc::new(DashMap::new()),
            cache_ttl_ms: Arc::new(AtomicU64::new(DEFAULT_CACHE_TTL_MS)),
        }
    }

    /// Construct from a shared `CrowdbKvClient`. The discovery client
    /// wraps a new `ServiceRegistryClient` that shares the same
    /// underlying KV client + connection pool.
    #[must_use]
    pub fn from_shared_kv(kv: Arc<CrowdbKvClient>) -> Self {
        Self::new(ServiceRegistryClient::from_shared(kv))
    }

    /// Set a custom cache TTL. Default is 5 seconds. A TTL of 0
    /// disables caching (every call queries group-0).
    #[must_use]
    pub fn with_cache_ttl(self, ttl: Duration) -> Self {
        self.cache_ttl_ms.store(duration_to_ms(ttl), Ordering::Relaxed);
        self
    }

    /// Discover all living instances of `service`. Returns from cache
    /// if fresh; otherwise queries group-0 and updates the cache.
    ///
    /// On group-0 unreachable, the cache is **not** invalidated — a
    /// stale cache is returned if available, otherwise the error
    /// propagates.
    pub async fn discover_all(&self, service: &str) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.discover_all_with(service, || self.svc.read_all_instances(service))
            .await
    }

    async fn discover_all_with<F, Fut>(
        &self,
        service: &str,
        refresh: F,
    ) -> Result<Vec<(InstanceId, InstanceValue)>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<(InstanceId, InstanceValue)>>>,
    {
        let state = self.service_state(service);
        let ttl_ms = self.cache_ttl_ms.load(Ordering::Relaxed);
        let now = now_ms();

        // Fast path: cache hit within TTL.
        if let Some(entry) = state.cache.load_full() {
            if now.saturating_sub(entry.refreshed_at_ms) < ttl_ms {
                return Ok(entry.instances.clone());
            }
        }

        // Slow path: one in-flight group-0 query per service. No DashMap
        // guard survives the `service_state` lookup above.
        let _refresh_guard = state.refresh.lock().await;
        let now = now_ms();
        if let Some(entry) = state.cache.load_full() {
            if now.saturating_sub(entry.refreshed_at_ms) < ttl_ms {
                return Ok(entry.instances.clone());
            }
        }

        let instances = match refresh().await {
            Ok(v) => v,
            Err(e) => {
                // On failure, return stale cache if available.
                if let Some(entry) = state.cache.load_full() {
                    return Ok(entry.instances.clone());
                }
                return Err(Error::DiscoveryUnreachable {
                    service: service.to_string(),
                    source: Box::new(e),
                });
            }
        };

        state.cache.store(Some(Arc::new(CachedEntry {
            instances: instances.clone(),
            refreshed_at_ms: now_ms(),
        })));

        Ok(instances)
    }

    /// Discover one living instance of `service` (round-robin among
    /// the cached/refreshed set). Returns `Error::NoLivingInstances`
    /// if the registry has zero live entries for the service.
    pub async fn discover_one(&self, service: &str) -> Result<InstanceValue> {
        let instances = self.discover_all(service).await?;
        if instances.is_empty() {
            return Err(Error::NoLivingInstances {
                service: service.to_string(),
            });
        }

        // Round-robin: atomically increment the cursor and pick the
        // instance at `cursor % len`.
        let idx = self
            .service_state(service)
            .rr_cursor
            .fetch_add(1, Ordering::Relaxed)
            % instances.len();

        Ok(instances[idx].1.clone())
    }

    /// Discover the instance whose `rpc_endpoint` matches `endpoint`.
    /// Returns `Ok(None)` if no living instance has that endpoint.
    /// Used by callers that have an explicit override address and want
    /// to verify liveness — if the endpoint is not in the registry,
    /// the caller may still connect directly (override semantics).
    pub async fn discover_by_endpoint(&self, service: &str, endpoint: &str) -> Result<Option<InstanceValue>> {
        let instances = self.discover_all(service).await?;
        Ok(instances
            .into_iter()
            .find(|(_, v)| v.rpc_endpoint == endpoint)
            .map(|(_, v)| v))
    }

    /// Invalidate the cache for `service` (or all services if
    /// `None`). The next `discover_*` call re-queries group-0.
    /// Called after a known topology change (e.g. `cluster_init`,
    /// `deploy_diskdb`) so the next read picks up new registrations
    /// immediately.
    pub fn invalidate(&self, service: Option<&str>) {
        match service {
            Some(s) => {
                if let Some(state) = self.services.get(s) {
                    state.cache.store(None);
                }
            }
            None => {
                self.services.iter().for_each(|state| state.cache.store(None));
            }
        }
    }

    /// Access the underlying `ServiceRegistryClient` for direct
    /// registry operations (register, heartbeat, unregister).
    #[must_use]
    pub fn registry(&self) -> &ServiceRegistryClient {
        &self.svc
    }

    fn service_state(&self, service: &str) -> Arc<ServiceState> {
        self.services
            .entry(service.to_string())
            .or_insert_with(|| Arc::new(ServiceState::default()))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientConfig;

    #[test]
    fn default_cache_ttl_is_5s() {
        let client = ServiceDiscoveryClient::new(ServiceRegistryClient::new(CrowdbKvClient::new(
            ClientConfig::new(vec!["http://127.0.0.1:10000".into()]),
        )));
        assert_eq!(client.cache_ttl_ms.load(Ordering::Relaxed), 5_000);
    }

    #[test]
    fn with_cache_ttl_sets_ttl() {
        let client = ServiceDiscoveryClient::new(ServiceRegistryClient::new(CrowdbKvClient::new(
            ClientConfig::new(vec!["http://127.0.0.1:10000".into()]),
        )))
        .with_cache_ttl(Duration::from_millis(500));
        assert_eq!(client.cache_ttl_ms.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn invalidate_removes_single_service() {
        let client = ServiceDiscoveryClient::new(ServiceRegistryClient::new(CrowdbKvClient::new(
            ClientConfig::new(vec!["http://127.0.0.1:10000".into()]),
        )));
        let diskdb = client.service_state("diskdb");
        diskdb.cache.store(Some(Arc::new(CachedEntry {
            instances: vec![],
            refreshed_at_ms: 0,
        })));
        let chunkdb = client.service_state("chunkdb");
        chunkdb.cache.store(Some(Arc::new(CachedEntry {
            instances: vec![],
            refreshed_at_ms: 0,
        })));
        client.invalidate(Some("diskdb"));
        assert!(diskdb.cache.load().is_none());
        assert!(chunkdb.cache.load().is_some());
    }

    #[test]
    fn invalidate_all_clears_cache() {
        let client = ServiceDiscoveryClient::new(ServiceRegistryClient::new(CrowdbKvClient::new(
            ClientConfig::new(vec!["http://127.0.0.1:10000".into()]),
        )));
        let diskdb = client.service_state("diskdb");
        diskdb.cache.store(Some(Arc::new(CachedEntry {
            instances: vec![],
            refreshed_at_ms: 0,
        })));
        client.invalidate(None);
        assert!(diskdb.cache.load().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn simultaneous_expiry_uses_one_refresh() {
        let client = ServiceDiscoveryClient::new(ServiceRegistryClient::new(CrowdbKvClient::new(
            ClientConfig::new(vec!["http://127.0.0.1:10000".into()]),
        )))
        .with_cache_ttl(Duration::from_secs(1));
        let calls = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(tokio::sync::Barrier::new(16));
        let tasks: Vec<_> = (0..16)
            .map(|_| {
                let client = client.clone();
                let calls = Arc::clone(&calls);
                let start = Arc::clone(&start);
                tokio::spawn(async move {
                    start.wait().await;
                    client
                        .discover_all_with("diskdb", || async move {
                            calls.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            Ok(vec![(1, InstanceValue::default())])
                        })
                        .await
                })
            })
            .collect();

        for task in tasks {
            let instances = task.await.expect("discovery task").expect("discovery result");
            assert_eq!(instances.len(), 1);
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}
