use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs, FBMsgType};
use crowdb_rpc_ffi::{Buffer, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::{ProbeKind, ProbeProfile, ServiceProfile};

#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("probe target is invalid")]
    InvalidTarget,
    #[error("probe timed out")]
    Timeout,
    #[error("probe endpoint is unavailable")]
    Unavailable,
    #[error("probe credential is absent")]
    MissingCredential,
}

pub struct ProbeExecutor {
    client: reqwest::Client,
    rpc: Option<RpcProbe>,
}

struct RpcProbe {
    client: RpcClient,
    server: RpcServer,
    request_id: AtomicU64,
}

impl RpcProbe {
    fn new() -> Result<Self, ProbeError> {
        let server = RpcServer::new(None);
        server
            .listen("127.0.0.1", 0)
            .map_err(|_| ProbeError::Unavailable)?;
        server.start();
        let client = RpcClient::new();
        client.set_completion_pool_size(128);
        client.start_reaper(2_000_000_000, 100_000_000);
        Ok(Self {
            client,
            server,
            request_id: AtomicU64::new(1),
        })
    }

    async fn ping(&self, address: SocketAddr, duration: Duration) -> Result<(), ProbeError> {
        let connection = self
            .server
            .connect(&address.ip().to_string(), i32::from(address.port()))
            .map_err(|_| ProbeError::Unavailable)?;
        self.client.attach(&connection);
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let mut builder = FlatBufferBuilder::new();
        let request = ConnectionPingRequest::create(
            &mut builder,
            &ConnectionPingRequestArgs {
                id: request_id,
                rpc_create_nano: 0,
            },
        );
        builder.finish(request, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let response = self
            .client
            .call(
                &self.server,
                &connection,
                request_id,
                control,
                None,
                FBMsgType::EConnectionPingRequest.0 as u16,
            )
            .map_err(|_| ProbeError::Unavailable)?;
        let response = timeout(duration, response)
            .await
            .map_err(|_| ProbeError::Timeout)?
            .map_err(|_| ProbeError::Unavailable)?;
        if response.request_id == request_id {
            Ok(())
        } else {
            Err(ProbeError::Unavailable)
        }
    }
}

impl ProbeExecutor {
    /// # Errors
    /// Rejects invalid HTTP client configuration.
    pub fn new(enable_rpc: bool) -> Result<Self, ProbeError> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ProbeError::InvalidTarget)?;
        let rpc = enable_rpc.then(RpcProbe::new).transpose()?;
        Ok(Self { client, rpc })
    }

    /// # Errors
    /// Rejects malformed, timed-out, non-success, and unreachable endpoints.
    pub async fn probe_service(
        &self,
        service: &ServiceProfile,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), ProbeError> {
        self.probe_one(&service.probe, environment).await?;
        for probe in &service.additional_probes {
            self.probe_one(probe, environment).await?;
        }
        Ok(())
    }

    async fn probe_one(
        &self,
        probe: &ProbeProfile,
        environment: &BTreeMap<String, String>,
    ) -> Result<(), ProbeError> {
        let duration = Duration::from_millis(probe.timeout_ms);
        match probe.kind {
            ProbeKind::Tcp => {
                let address: SocketAddr = probe.target.parse().map_err(|_| ProbeError::InvalidTarget)?;
                timeout(duration, TcpStream::connect(address))
                    .await
                    .map_err(|_| ProbeError::Timeout)?
                    .map_err(|_| ProbeError::Unavailable)?;
                Ok(())
            }
            ProbeKind::RpcPing => {
                let address = probe.target.parse().map_err(|_| ProbeError::InvalidTarget)?;
                self.rpc
                    .as_ref()
                    .ok_or(ProbeError::Unavailable)?
                    .ping(address, duration)
                    .await
            }
            ProbeKind::Http => {
                let mut request = self.client.get(&probe.target).timeout(duration);
                if let Some(name) = &probe.bearer_env {
                    let token = environment
                        .get(name)
                        .filter(|token| !token.is_empty())
                        .ok_or(ProbeError::MissingCredential)?;
                    request = request.bearer_auth(token);
                }
                let response = request.send().await.map_err(|error| {
                    if error.is_timeout() {
                        ProbeError::Timeout
                    } else {
                        ProbeError::Unavailable
                    }
                })?;
                if response.status().is_success() {
                    Ok(())
                } else {
                    Err(ProbeError::Unavailable)
                }
            }
        }
    }
}
