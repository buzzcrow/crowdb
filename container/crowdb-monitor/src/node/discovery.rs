// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::net::{IpAddr, SocketAddr};

use crowdb_protocol::mgmt::node::{CandidateNode, NodeAdvertisement, NODE_PROTOCOL_VERSION};
use mdns_sd::{DaemonEvent, IfKind, Receiver, ServiceDaemon, ServiceEvent, ServiceInfo};
use thiserror::Error;
use uuid::Uuid;

use super::{CandidateCache, NodeIdentity};

pub const NODE_SERVICE_TYPE: &str = "_crowdb-node._tcp.local.";

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("discovery state I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("node discovery failed: {0}")]
    Mdns(#[from] mdns_sd::Error),
    #[error("node discovery configuration is invalid: {0}")]
    Invalid(&'static str),
}

/// Explicit management-interface advertisement; no storage interfaces are enabled.
#[derive(Clone, Debug)]
pub struct DiscoveryConfig {
    pub interfaces: Vec<String>,
    pub addresses: Vec<IpAddr>,
    pub monitor_port: u16,
    pub cluster_id: Option<Uuid>,
    pub seeds: Vec<String>,
}

pub struct NodeDiscovery {
    daemon: ServiceDaemon,
    events: Receiver<ServiceEvent>,
    diagnostics: Receiver<DaemonEvent>,
    fullname: String,
    local: NodeAdvertisement,
    cache: CandidateCache,
}

impl NodeDiscovery {
    pub(super) fn observe_seed(&mut self, seed: &str, advertisement: Option<NodeAdvertisement>) {
        let instance = format!("seed:{seed}");
        if let Some(advertisement) = advertisement {
            if advertisement == self.local {
                return;
            }
            self.cache.observe(instance, advertisement);
        } else {
            self.cache.remove(&instance);
        }
    }

    pub(super) fn bind_cluster(&mut self, cluster: Option<String>) -> Result<(), DiscoveryError> {
        if cluster == self.local.cluster_id {
            return Ok(());
        }
        let instance = self
            .fullname
            .strip_suffix(NODE_SERVICE_TYPE)
            .ok_or(DiscoveryError::Invalid("invalid local service name"))?
            .trim_end_matches('.');
        let mut properties = vec![
            ("id".to_owned(), self.local.discovery_id.clone()),
            ("version".to_owned(), self.local.protocol_version.to_string()),
        ];
        if let Some(cluster) = &cluster {
            properties.push(("cluster".to_owned(), cluster.clone()));
        }
        let mut addresses = Vec::new();
        let mut port = 0;
        for endpoint in &self.local.monitor_endpoints {
            let endpoint = reqwest::Url::parse(endpoint)
                .map_err(|_| DiscoveryError::Invalid("invalid local endpoint"))?;
            addresses.push(
                endpoint
                    .host_str()
                    .ok_or(DiscoveryError::Invalid("missing host"))?
                    .trim_matches(['[', ']'])
                    .parse::<IpAddr>()
                    .map_err(|_| DiscoveryError::Invalid("invalid IP address"))?,
            );
            port = endpoint.port().ok_or(DiscoveryError::Invalid("missing port"))?;
        }
        self.daemon.register(ServiceInfo::new(
            NODE_SERVICE_TYPE,
            instance,
            &format!("crowdb-{instance}.local."),
            addresses.as_slice(),
            port,
            properties.as_slice(),
        )?)?;
        self.local.cluster_id = cluster;
        Ok(())
    }
    /// # Errors
    /// Rejects empty management scope, wildcard addresses and unusable metadata.
    pub fn start(identity: NodeIdentity, config: &DiscoveryConfig) -> Result<Self, DiscoveryError> {
        if config.interfaces.is_empty()
            || config
                .interfaces
                .iter()
                .any(|name| name.is_empty() || name.len() > 64)
            || config.addresses.is_empty()
            || config.addresses.len() > 16
            || config
                .addresses
                .iter()
                .any(|address| address.is_unspecified() || address.is_multicast())
            || config.monitor_port == 0
            || config.cluster_id.is_some_and(|id| id.is_nil())
        {
            return Err(DiscoveryError::Invalid(
                "explicit interfaces, addresses and port are required",
            ));
        }
        let daemon = ServiceDaemon::new()?;
        let result = Self::configure(daemon.clone(), identity, config);
        if result.is_err() {
            let _ = daemon.shutdown();
        }
        result
    }

    fn configure(
        daemon: ServiceDaemon,
        identity: NodeIdentity,
        config: &DiscoveryConfig,
    ) -> Result<Self, DiscoveryError> {
        daemon.disable_interface(IfKind::All)?;
        for name in &config.interfaces {
            daemon.enable_interface(IfKind::Name(name.clone()))?;
        }
        let diagnostics = daemon.monitor()?;
        let mut properties = vec![
            ("id".to_owned(), identity.uuid().to_string()),
            ("version".to_owned(), NODE_PROTOCOL_VERSION.to_string()),
        ];
        if let Some(cluster) = config.cluster_id {
            properties.push(("cluster".to_owned(), cluster.to_string()));
        }
        // A boot-specific instance exposes cloned persistent identities rather
        // than allowing DNS name collision handling to hide a second claimant.
        let instance = format!("{}-{}", identity.uuid(), &Uuid::new_v4().to_string()[..8]);
        let host = format!("crowdb-{instance}.local.");
        let service = ServiceInfo::new(
            NODE_SERVICE_TYPE,
            &instance,
            &host,
            config.addresses.as_slice(),
            config.monitor_port,
            properties.as_slice(),
        )?;
        let fullname = service.get_fullname().to_owned();
        daemon.register(service)?;
        let events = daemon.browse(NODE_SERVICE_TYPE)?;
        let mut endpoints: Vec<_> = config
            .addresses
            .iter()
            .map(|address| format!("http://{}", SocketAddr::new(*address, config.monitor_port)))
            .collect();
        endpoints.sort();
        endpoints.dedup();
        Ok(Self {
            daemon,
            events,
            diagnostics,
            fullname,
            local: NodeAdvertisement {
                discovery_id: identity.uuid().to_string(),
                protocol_version: NODE_PROTOCOL_VERSION,
                monitor_endpoints: endpoints,
                cluster_id: config.cluster_id.map(|id| id.to_string()),
            },
            cache: CandidateCache::default(),
        })
    }

    #[must_use]
    pub fn local(&self) -> &NodeAdvertisement {
        &self.local
    }

    /// Drain bounded batches; callers keep serving management requests between polls.
    pub fn refresh(&mut self) -> Vec<CandidateNode> {
        for _ in 0..1024 {
            let Ok(event) = self.events.try_recv() else {
                break;
            };
            match event {
                ServiceEvent::ServiceResolved(service) => {
                    if service.get_fullname() == self.fullname {
                        continue;
                    }
                    let Some(identity) = service.get_property_val_str("id") else {
                        continue;
                    };
                    let Some(version) = service
                        .get_property_val_str("version")
                        .and_then(|text| text.parse().ok())
                    else {
                        continue;
                    };
                    let advertisement = NodeAdvertisement {
                        discovery_id: identity.to_owned(),
                        protocol_version: version,
                        monitor_endpoints: service
                            .get_addresses()
                            .iter()
                            .map(|address| {
                                format!(
                                    "http://{}",
                                    SocketAddr::new(address.to_ip_addr(), service.get_port())
                                )
                            })
                            .collect(),
                        cluster_id: service.get_property_val_str("cluster").map(str::to_owned),
                    };
                    self.cache
                        .observe(service.get_fullname().to_owned(), advertisement);
                }
                ServiceEvent::ServiceRemoved(_, fullname) => self.cache.remove(&fullname),
                _ => {}
            }
        }
        self.cache.snapshot(&self.local)
    }

    #[must_use]
    pub fn diagnostics(&self) -> Vec<String> {
        self.diagnostics
            .try_iter()
            .take(64)
            .map(|event| format!("{event:?}"))
            .collect()
    }

    /// # Errors
    /// Sends standard goodbye announcements before shutting down discovery.
    pub async fn shutdown(self) -> Result<(), DiscoveryError> {
        self.daemon
            .unregister(&self.fullname)?
            .recv_async()
            .await
            .map_err(|_| DiscoveryError::Invalid("goodbye channel closed"))?;
        self.daemon
            .shutdown()?
            .recv_async()
            .await
            .map_err(|_| DiscoveryError::Invalid("shutdown channel closed"))?;
        Ok(())
    }
}

impl Drop for NodeDiscovery {
    fn drop(&mut self) {
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
    }
}
