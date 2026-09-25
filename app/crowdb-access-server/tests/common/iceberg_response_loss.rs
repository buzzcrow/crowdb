use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct TestResponseLossProxy {
    pub origin: String,
    dropped: Arc<AtomicU8>,
    task: tokio::task::JoinHandle<()>,
}

impl TestResponseLossProxy {
    pub async fn start(backend_origin: String, path: &'static str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let dropped = Arc::new(AtomicU8::new(0));
        let observed = dropped.clone();
        let target = format!("POST {path} ");
        let task = tokio::spawn(async move {
            loop {
                let (client, _) = listener.accept().await.unwrap();
                let backend = backend_origin.clone();
                let target = target.clone();
                let dropped = dropped.clone();
                tokio::spawn(async move {
                    forward_or_lose(client, &backend, target.as_bytes(), dropped)
                        .await
                        .unwrap();
                });
            }
        });
        Self {
            origin,
            dropped: observed,
            task,
        }
    }

    pub fn assert_dropped(&self) {
        assert_eq!(
            self.dropped.load(Ordering::SeqCst),
            2,
            "proxy did not drop a successful create response"
        );
    }
}

impl Drop for TestResponseLossProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn forward_or_lose(
    mut client: tokio::net::TcpStream,
    backend_origin: &str,
    target: &[u8],
    dropped: Arc<AtomicU8>,
) -> std::io::Result<()> {
    let mut header = Vec::new();
    while !header.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut buffer = [0_u8; 4096];
        let count = client.read(&mut buffer).await?;
        if count == 0 || header.len() + count > 16 * 1024 {
            return Err(std::io::Error::other("invalid proxy request header"));
        }
        header.extend_from_slice(&buffer[..count]);
    }
    let backend = backend_origin.trim_start_matches("http://");
    let mut upstream = tokio::net::TcpStream::connect(backend).await?;
    upstream.write_all(&header).await?;
    if header.starts_with(target)
        && dropped
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    {
        let (mut client_read, client_write) = client.into_split();
        let (mut upstream_read, mut upstream_write) = upstream.into_split();
        let forwarding = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut client_read, &mut upstream_write).await;
        });
        let mut response = [0_u8; 4096];
        let count = upstream_read.read(&mut response).await?;
        forwarding.abort();
        drop(client_write);
        if count == 0 || !response.starts_with(b"HTTP/1.1 200") {
            return Err(std::io::Error::other(
                "upstream did not publish the create response",
            ));
        }
        dropped.store(2, Ordering::SeqCst);
        return Ok(());
    }
    if let Err(error) = tokio::io::copy_bidirectional(&mut client, &mut upstream).await {
        if !matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
        ) {
            return Err(error);
        }
    }
    Ok(())
}
