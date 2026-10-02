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
    inline: Option<InlineDigest>,
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
            inline: None,
        }
    }

    pub(crate) fn start_inline(check_sha256: bool) -> Result<Self, ()> {
        Ok(Self {
            sender: None,
            worker: None,
            inline: Some(InlineDigest {
                md5: Hasher::new(MessageDigest::md5()).map_err(|_| ())?,
                sha256: check_sha256
                    .then(|| Hasher::new(MessageDigest::sha256()).map_err(|_| ()))
                    .transpose()?,
                process_time: Duration::ZERO,
            }),
        })
    }

    pub(crate) fn enqueue(&mut self, payload: Vec<Bytes>) -> Result<(), ()> {
        if let Some(inline) = &mut self.inline {
            let started = Instant::now();
            for bytes in payload {
                inline.md5.update(&bytes).map_err(|_| ())?;
                if let Some(sha256) = &mut inline.sha256 {
                    sha256.update(&bytes).map_err(|_| ())?;
                }
            }
            inline.process_time += started.elapsed();
            return Ok(());
        }
        self.sender
            .as_ref()
            .ok_or(())?
            .try_send(DigestBatch { payload })
            .map_err(|_| ())
    }

    pub(crate) fn process_inline(&mut self, payload: &[u8]) -> Result<(), ()> {
        let inline = self.inline.as_mut().ok_or(())?;
        let started = Instant::now();
        inline.md5.update(payload).map_err(|_| ())?;
        if let Some(sha256) = &mut inline.sha256 {
            sha256.update(payload).map_err(|_| ())?;
        }
        inline.process_time += started.elapsed();
        Ok(())
    }

    pub(crate) async fn finish(&mut self) -> Result<Digests, ()> {
        if let Some(mut inline) = self.inline.take() {
            return Ok(Digests {
                md5: inline
                    .md5
                    .finish()
                    .map_err(|_| ())?
                    .as_ref()
                    .try_into()
                    .map_err(|_| ())?,
                sha256: inline
                    .sha256
                    .as_mut()
                    .map(|hash| hash.finish().map_err(|_| ())?.as_ref().try_into().map_err(|_| ()))
                    .transpose()?,
                process_time: inline.process_time,
            });
        }
        self.sender.take();
        self.worker.take().ok_or(())?.await.map_err(|_| ())?
    }
}

struct InlineDigest {
    md5: Hasher,
    sha256: Option<Hasher>,
    process_time: Duration,
}
