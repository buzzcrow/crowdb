// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{scan_reply, ChunkKvResponse, ClientError, HashMap, Result, RpcValue, ScanRequest};
use tokio::sync::oneshot;

type ScanReply = oneshot::Sender<Result<ChunkKvResponse>>;

pub(super) enum TestScanAction {
    Fail(Vec<u8>),
    Pause(Vec<u8>, oneshot::Sender<()>),
    Resume,
}

#[derive(Default)]
pub(super) struct TestScans {
    failed: Option<Vec<u8>>,
    paused: Option<Vec<u8>>,
    entered: Option<oneshot::Sender<()>>,
    pending: Vec<(ScanRequest, ScanReply)>,
}

impl TestScans {
    pub(super) fn configure(&mut self, action: TestScanAction, values: &HashMap<Vec<u8>, RpcValue>) {
        match action {
            TestScanAction::Fail(prefix) => self.failed = Some(prefix),
            TestScanAction::Pause(prefix, entered) => {
                self.paused = Some(prefix);
                self.entered = Some(entered);
            }
            TestScanAction::Resume => {
                self.failed = None;
                self.paused = None;
                for (request, response) in self.pending.drain(..) {
                    let _ = response.send(Ok(scan_reply(&request, values)));
                }
            }
        }
    }

    pub(super) fn handle(
        &mut self,
        request: ScanRequest,
        response: ScanReply,
        values: &HashMap<Vec<u8>, RpcValue>,
    ) {
        if self
            .failed
            .as_ref()
            .is_some_and(|prefix| matches_prefix(&request, prefix))
        {
            let _ = response.send(Err(ClientError::Transport("injected scan failure".into())));
        } else if self
            .paused
            .as_ref()
            .is_some_and(|prefix| matches_prefix(&request, prefix))
        {
            self.pending.push((request, response));
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
        } else {
            let _ = response.send(Ok(scan_reply(&request, values)));
        }
    }
}

fn matches_prefix(request: &ScanRequest, prefix: &[u8]) -> bool {
    request
        .start
        .as_ref()
        .is_some_and(|start| start.starts_with(prefix))
}
