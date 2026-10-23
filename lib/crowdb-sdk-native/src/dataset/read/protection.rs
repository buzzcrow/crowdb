// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{now, DatasetClient};
use crate::{runtime::RequestLease, Cancellation, Error, Result};
use crowdb_access_dataset::{DatasetAuthority, DatasetIdentity, DatasetSessions, OperationId, SnapshotId};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

struct Protection {
    authority: Arc<DatasetAuthority>,
    identity: DatasetIdentity,
    snapshot: SnapshotId,
    handle: OperationId,
    session: Option<OperationId>,
    failed: AtomicBool,
}

pub(super) struct ReadProtection {
    state: Arc<Protection>,
    stop: Cancellation,
    worker: tokio::task::JoinHandle<()>,
}

impl ReadProtection {
    pub(super) async fn acquire(
        client: &DatasetClient,
        request: &RequestLease,
        snapshot: SnapshotId,
        session: Option<OperationId>,
    ) -> Result<Self> {
        let state = Arc::new(Protection {
            authority: Arc::clone(&client.authority),
            identity: client.identity.clone(),
            snapshot,
            handle: OperationId::random(),
            session,
            failed: AtomicBool::new(false),
        });
        request.run(state.renew()).await?;
        let stop = Cancellation::default();
        let closing = client.runtime.resources.closing.clone();
        let worker_stop = stop.clone();
        let worker_state = Arc::clone(&state);
        let context = request.context().clone();
        let worker = tokio::spawn(async move {
            let renewal = async {
                loop {
                    tokio::time::sleep(Duration::from_secs(15)).await;
                    worker_state.renew().await?;
                }
            };
            let result = tokio::select! {
                () = worker_stop.cancelled() => Ok(()),
                () = closing.cancelled() => Err(Error::Shutdown),
                result = context.run(renewal) => result,
            };
            if result.is_err() {
                worker_state.failed.store(true, Ordering::Release);
            }
        });
        Ok(Self { state, stop, worker })
    }

    pub(super) async fn check(&self) -> Result<()> {
        if self.state.failed.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        if let Some(session) = self.state.session {
            DatasetSessions::new(Arc::clone(&self.state.authority))
                .check_open(&self.state.identity, session)
                .await?;
        }
        Ok(())
    }
}

impl Protection {
    async fn renew(&self) -> Result<()> {
        if let Some(session) = self.session {
            DatasetSessions::new(Arc::clone(&self.authority))
                .resume(&self.identity, session, now())
                .await?;
        }
        self.authority
            .touch_read_handle(&self.identity, self.snapshot, self.handle, now(), 60)
            .await?;
        Ok(())
    }
}

impl Drop for ReadProtection {
    fn drop(&mut self) {
        self.stop.cancel();
        self.worker.abort();
        let state = Arc::clone(&self.state);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let release =
                    state
                        .authority
                        .release_read_handle(&state.identity, state.snapshot, state.handle);
                if !matches!(
                    tokio::time::timeout(Duration::from_secs(5), release).await,
                    Ok(Ok(()))
                ) {
                    tracing::error!(
                        "Dataset read release failed; inactivity expiry retains cleanup responsibility"
                    );
                }
            });
        }
    }
}
