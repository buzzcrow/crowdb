use std::io;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Instant;

pub(super) struct ConnectionActivity {
    start: Instant,
    latest_ms: AtomicU64,
}

impl ConnectionActivity {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            latest_ms: AtomicU64::new(0),
        })
    }

    fn record(&self) {
        let elapsed = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.latest_ms.fetch_max(elapsed, Ordering::Relaxed);
    }

    pub(super) async fn expired(&self, idle: Duration) {
        loop {
            let latest = self.latest_ms.load(Ordering::Relaxed);
            tokio::time::sleep_until(self.start + Duration::from_millis(latest) + idle).await;
            if self.latest_ms.load(Ordering::Relaxed) == latest {
                return;
            }
        }
    }
}

pub(super) struct ActiveIo<Stream> {
    stream: Stream,
    activity: Arc<ConnectionActivity>,
}

impl<Stream> ActiveIo<Stream> {
    pub(super) fn new(stream: Stream, activity: Arc<ConnectionActivity>) -> Self {
        Self { stream, activity }
    }
}

impl<Stream: AsyncRead + Unpin> AsyncRead for ActiveIo<Stream> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        let result = Pin::new(&mut this.stream).poll_read(context, buffer);
        if matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() > before {
            this.activity.record();
        }
        result
    }
}

impl<Stream: AsyncWrite + Unpin> AsyncWrite for ActiveIo<Stream> {
    fn poll_write(self: Pin<&mut Self>, context: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.stream).poll_write(context, bytes);
        if matches!(result, Poll::Ready(Ok(length)) if length > 0) {
            this.activity.record();
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(context)
    }
}

#[cfg(feature = "test-util")]
pub fn active_io_for_tests<Stream: AsyncRead + AsyncWrite + Unpin>(
    stream: Stream,
    idle: Duration,
) -> (
    impl AsyncRead + AsyncWrite + Unpin,
    impl std::future::Future<Output = ()>,
) {
    let activity = ConnectionActivity::new();
    let tracked = ActiveIo::new(stream, activity.clone());
    (tracked, async move { activity.expired(idle).await })
}
