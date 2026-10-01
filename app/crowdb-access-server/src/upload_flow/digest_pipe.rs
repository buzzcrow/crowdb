use hyper::body::Bytes;
use openssl::hash::{Hasher, MessageDigest};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// One upload's checksum pipeline. Queueing never controls socket backpressure;
/// the write flow does. Bytes clones retain the received buffer until OpenSSL
/// has consumed it, while the writer may use the same buffer.
pub(crate) struct DigestPipe {
    sender: Option<mpsc::Sender<DigestBatch>>,
    worker: Option<JoinHandle<Result<Digests, ()>>>,
}

struct DigestBatch {
    payload: Vec<Bytes>,
}

pub(crate) struct Digests {
    pub md5: [u8; 16],
    pub sha256: Option<[u8; 32]>,
    pub process_time: Duration,
}

impl DigestPipe {
    pub(crate) fn start(check_sha256: bool) -> Self {
        let (sender, mut receiver) = mpsc::channel::<DigestBatch>(1024);
        let worker = tokio::task::spawn_blocking(move || {
            let mut md5 = Hasher::new(MessageDigest::md5()).map_err(|_| ())?;
            let mut sha256 = check_sha256
                .then(|| Hasher::new(MessageDigest::sha256()).map_err(|_| ()))
                .transpose()?;
            let mut process_time = Duration::ZERO;
            while let Some(batch) = receiver.blocking_recv() {
                let started = Instant::now();
                for bytes in batch.payload {
                    md5.update(&bytes).map_err(|_| ())?;
                    if let Some(sha256) = &mut sha256 {
                        sha256.update(&bytes).map_err(|_| ())?;
                    }
                }
                process_time += started.elapsed();
            }
            Ok(Digests {
                process_time,
                md5: md5
                    .finish()
                    .map_err(|_| ())?
                    .as_ref()
                    .try_into()
                    .map_err(|_| ())?,
                sha256: sha256
                    .as_mut()
                    .map(|sha256| {
                        sha256
                            .finish()
                            .map_err(|_| ())?
                            .as_ref()
                            .try_into()
                            .map_err(|_| ())
                    })
                    .transpose()?,
            })
        });
        Self {
            sender: Some(sender),
            worker: Some(worker),
        }
    }

    pub(crate) fn enqueue(&self, payload: Vec<Bytes>) -> Result<(), ()> {
        self.sender
            .as_ref()
            .ok_or(())?
            .try_send(DigestBatch { payload })
            .map_err(|_| ())
    }

    pub(crate) async fn finish(&mut self) -> Result<Digests, ()> {
        self.sender.take();
        self.worker.take().ok_or(())?.await.map_err(|_| ())?
    }
}
