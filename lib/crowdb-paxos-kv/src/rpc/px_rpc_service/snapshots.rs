// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    record_group_context, snapshot_error, spawn_request, submit_snapshot_begin, submit_snapshot_close,
    submit_snapshot_read, Arc, FBKvRetCode, FBMsgType, FBSnapshotAbortRequest, FBSnapshotBeginRequest,
    FBSnapshotFinishRequest, FBSnapshotReadRequest, PxRpcService, RpcServer, ServerRequest,
    SnapshotSessionId,
};

impl PxRpcService {
    // ── Snapshot streaming ───────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    pub(super) fn handle_snapshot_begin(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle = req.conn_handle as usize;
        let store = Arc::clone(&self.store);
        let registry = Arc::clone(&self.snapshots);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "snapshot_begin", store.store_id, async move {
            let Ok(request) = flatbuffers::root::<FBSnapshotBeginRequest>(req.control()) else {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::InvalidArgument,
                    Some("invalid snapshot Begin request"),
                    None,
                    0,
                );
                return;
            };
            let group_id = request.group_id();
            record_group_context(&store, group_id);
            let requested = request.max_chunk_bytes() as usize;
            if requested == 0 {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::InvalidArgument,
                    Some("max_chunk_bytes must be nonzero"),
                    None,
                    group_id,
                );
                return;
            }
            let Some(group) = store.get_group(group_id) else {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::NotFound,
                    Some("px group not found"),
                    None,
                    group_id,
                );
                return;
            };
            let membership_epoch = group.membership_epoch();
            let chunk_bytes = requested.min(store.snapshot_chunk_bytes).min(1024 * 1024);
            if chunk_bytes == 0 {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::Internal,
                    Some("snapshot source chunk size is not configured"),
                    None,
                    group_id,
                );
                return;
            }
            let permit = match registry.reserve() {
                Ok(permit) => permit,
                Err(error) => {
                    let (code, message) = snapshot_error(&error);
                    submit_snapshot_begin(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        code,
                        Some(&message),
                        None,
                        group_id,
                    );
                    return;
                }
            };
            let export_group = Arc::clone(&group);
            let export_started = std::time::Instant::now();
            let exporter = match tokio::task::spawn_blocking(move || {
                export_group
                    .local_replica()
                    .learner
                    .engine()
                    .snapshot_export_begin(chunk_bytes)
            })
            .await
            {
                Ok(Ok(exporter)) => exporter,
                Ok(Err(error)) => {
                    submit_snapshot_begin(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        FBKvRetCode::Internal,
                        Some(&error),
                        None,
                        group_id,
                    );
                    return;
                }
                Err(error) => {
                    let message = format!("snapshot export task failed: {error}");
                    submit_snapshot_begin(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        FBKvRetCode::Internal,
                        Some(&message),
                        None,
                        group_id,
                    );
                    return;
                }
            };
            registry.observe_export(export_started.elapsed());
            if group.membership_epoch() != membership_epoch {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::SnapshotTopologyChanged,
                    Some("membership changed while snapshot began"),
                    None,
                    group_id,
                );
                return;
            }
            let at_slot = exporter.metadata().at_slot;
            let term_at_slot = group
                .local_replica()
                .accepted_at(at_slot)
                .await
                .map_or(0, |entry| entry.term);
            let registration = registry.begin(group_id, membership_epoch, exporter, permit);
            submit_snapshot_begin(
                &server,
                conn_handle,
                req_id,
                create_nano,
                FBKvRetCode::Success,
                None,
                Some((&registration, term_at_slot)),
                group_id,
            );
        });
    }

    pub(super) fn handle_snapshot_read(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle = req.conn_handle as usize;
        let store = Arc::clone(&self.store);
        let registry = Arc::clone(&self.snapshots);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "snapshot_read", store.store_id, async move {
            let Ok(request) = flatbuffers::root::<FBSnapshotReadRequest>(req.control()) else {
                submit_snapshot_read(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::InvalidArgument,
                    Some("invalid snapshot Read request"),
                    SnapshotSessionId {
                        boot_nonce: 0,
                        session_number: 0,
                    },
                    0,
                    None,
                );
                return;
            };
            let id = SnapshotSessionId {
                boot_nonce: request.boot_nonce(),
                session_number: request.session_number(),
            };
            let offset = request.offset();
            let registration = match registry.registration(id) {
                Ok(value) => value,
                Err(error) => {
                    let (code, message) = snapshot_error(&error);
                    submit_snapshot_read(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        code,
                        Some(&message),
                        id,
                        offset,
                        None,
                    );
                    return;
                }
            };
            if store.get_group(registration.group_id).map_or(true, |group| {
                group.membership_epoch() != registration.membership_epoch
            }) {
                registry.expire(id);
                submit_snapshot_read(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::SnapshotTopologyChanged,
                    Some("snapshot membership epoch changed"),
                    id,
                    offset,
                    None,
                );
                return;
            }
            match registry.read(id, offset).await {
                Ok(read) => submit_snapshot_read(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    id,
                    offset,
                    Some(read),
                ),
                Err(error) => {
                    let (code, message) = snapshot_error(&error);
                    submit_snapshot_read(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        code,
                        Some(&message),
                        id,
                        offset,
                        None,
                    );
                }
            }
        });
    }

    pub(super) fn handle_snapshot_finish(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle = req.conn_handle as usize;
        let store = Arc::clone(&self.store);
        let registry = Arc::clone(&self.snapshots);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "snapshot_finish", store.store_id, async move {
            let Ok(request) = flatbuffers::root::<FBSnapshotFinishRequest>(req.control()) else {
                submit_snapshot_close(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBMsgType::ESnapshotFinishResponse.0 as u16,
                    FBKvRetCode::InvalidArgument,
                    Some("invalid snapshot Finish request"),
                    SnapshotSessionId {
                        boot_nonce: 0,
                        session_number: 0,
                    },
                );
                return;
            };
            let id = SnapshotSessionId {
                boot_nonce: request.boot_nonce(),
                session_number: request.session_number(),
            };
            if let Ok(registration) = registry.registration(id) {
                if store.get_group(registration.group_id).map_or(true, |group| {
                    group.membership_epoch() != registration.membership_epoch
                }) {
                    registry.expire(id);
                    submit_snapshot_close(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        FBMsgType::ESnapshotFinishResponse.0 as u16,
                        FBKvRetCode::SnapshotTopologyChanged,
                        Some("snapshot membership epoch changed"),
                        id,
                    );
                    return;
                }
            }
            match registry.finish(id, request.final_offset()).await {
                Ok(()) => submit_snapshot_close(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBMsgType::ESnapshotFinishResponse.0 as u16,
                    FBKvRetCode::Success,
                    None,
                    id,
                ),
                Err(error) => {
                    let (code, message) = snapshot_error(&error);
                    submit_snapshot_close(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        FBMsgType::ESnapshotFinishResponse.0 as u16,
                        code,
                        Some(&message),
                        id,
                    );
                }
            }
        });
    }

    pub(super) fn handle_snapshot_abort(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle = req.conn_handle as usize;
        let registry = Arc::clone(&self.snapshots);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "snapshot_abort", self.store.store_id, async move {
            let Ok(request) = flatbuffers::root::<FBSnapshotAbortRequest>(req.control()) else {
                submit_snapshot_close(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBMsgType::ESnapshotAbortResponse.0 as u16,
                    FBKvRetCode::InvalidArgument,
                    Some("invalid snapshot Abort request"),
                    SnapshotSessionId {
                        boot_nonce: 0,
                        session_number: 0,
                    },
                );
                return;
            };
            let id = SnapshotSessionId {
                boot_nonce: request.boot_nonce(),
                session_number: request.session_number(),
            };
            registry.abort(id).await;
            submit_snapshot_close(
                &server,
                conn_handle,
                req_id,
                create_nano,
                FBMsgType::ESnapshotAbortResponse.0 as u16,
                FBKvRetCode::Success,
                None,
                id,
            );
        });
    }
}
