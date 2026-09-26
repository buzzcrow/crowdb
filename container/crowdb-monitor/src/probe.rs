use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::{ProbeKind, ServiceProfile};

#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("probe target is invalid")]
    InvalidTarget,
    #[error("probe timed out")]
    Timeout,
    #[error("probe endpoint is unavailable")]
    Unavailable,
}

pub struct ProbeExecutor {
    client: reqwest::Client,
}

impl ProbeExecutor {
    /// # Errors
    /// Rejects invalid HTTP client configuration.
    pub fn new() -> Result<Self, ProbeError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ProbeError::InvalidTarget)?;
        Ok(Self { client })
    }

    /// # Errors
    /// Rejects malformed, timed-out, non-success, and unreachable endpoints.
    pub async fn probe_service(&self, service: &ServiceProfile) -> Result<(), ProbeError> {
        let duration = Duration::from_millis(service.probe.timeout_ms);
        match service.probe.kind {
            ProbeKind::Tcp => {
                let address: SocketAddr = service
                    .probe
                    .target
                    .parse()
                    .map_err(|_| ProbeError::InvalidTarget)?;
                timeout(duration, TcpStream::connect(address))
                    .await
                    .map_err(|_| ProbeError::Timeout)?
                    .map_err(|_| ProbeError::Unavailable)?;
                Ok(())
            }
            ProbeKind::Http => {
                let response = self
                    .client
                    .get(&service.probe.target)
                    .timeout(duration)
                    .send()
                    .await
                    .map_err(|error| {
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
