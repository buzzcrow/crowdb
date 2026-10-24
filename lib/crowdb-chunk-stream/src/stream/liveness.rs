// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{rollover, Duration, WorkerState};

pub(super) struct Renewal(tokio::task::JoinHandle<()>);
impl Drop for Renewal {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) fn start(state: &WorkerState) -> Renewal {
    let chunks = state.chunks.clone();
    let view = state.published_view.clone();
    let authority = state.authority.clone().expect("writer worker has authority");
    let failed = state.liveness_failed.clone();
    let changed = state.liveness_changed.clone();
    let closed = state.closed_view.clone();
    let interval = state.config.liveness_interval;
    let epoch = state.writer_epoch;
    Renewal(tokio::spawn(async move {
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticks.tick().await;
        loop {
            ticks.tick().await;
            if closed.load(std::sync::atomic::Ordering::Acquire) || authority.check().is_err() {
                break;
            }
            let state = view.load_full();
            let active = state.snapshot.record();
            if active.state != crowdb_protocol::chunk_stream::StreamChunkState::Active {
                continue;
            }
            for attempt in 0..3 {
                if authority.check().is_err() {
                    return;
                }
                match chunks.renew_liveness(active.chunk_id, epoch).await {
                    Ok(()) => break,
                    Err(error) => {
                        tracing::warn!(%error, attempt, "stream chunk liveness renewal failed");
                        if attempt == 2 || matches!(error, crate::StreamError::StaleWriter) {
                            failed.store(true, std::sync::atomic::Ordering::Release);
                            changed.notify_one();
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }))
}

pub(super) async fn maintain(state: &mut WorkerState) {
    if state
        .liveness_failed
        .swap(false, std::sync::atomic::Ordering::AcqRel)
    {
        if let Err(error) = rollover(state).await {
            state.stalled = true;
            tracing::warn!(%error, "stream cannot rotate after liveness renewal failure");
        }
    }
}
