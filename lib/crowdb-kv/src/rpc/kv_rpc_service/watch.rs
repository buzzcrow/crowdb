// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One-way watch subscription and push routing.

use super::{
    debug, record_group_context, request_span, Arc, Buffer, FBMsgType, FlatBufferBuilder,
    KvClientRpcForwarder, KvRpcService, RpcServer,
};
use crowdb_protocol::kv_client_fb::{
    FBWatchNotifyError, FBWatchNotifyErrorArgs, FBWatchSubscribe, FBWatchUnsubscribe,
};
use crowdb_rpc_ffi::Connection;

impl KvClientRpcForwarder {
    /// Send a fire-and-forget `WatchNotifyError` push frame on the
    /// given connection (server→client). Used when a subscribe arrives
    /// on a non-leader or for a missing group.
    pub(crate) fn send_watch_notify_error(
        &self,
        server: &RpcServer,
        conn: &Connection,
        group_id: u64,
        not_leader_hint: &str,
        error: &str,
    ) {
        let req_id = self.next_id();
        let mut builder = FlatBufferBuilder::new();
        let hint = builder.create_string(not_leader_hint);
        let err = builder.create_string(error);
        let args = FBWatchNotifyErrorArgs {
            id: req_id,
            rpc_create_nano: 0,
            group_id,
            not_leader_hint: Some(hint),
            error: Some(err),
        };
        let fb = FBWatchNotifyError::create(&mut builder, &args);
        builder.finish(fb, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EWatchNotifyError.0 as u16;
        let _ = self.rpc.send_one_way_to_handle(
            server,
            conn.handle().cast::<std::ffi::c_void>(),
            req_id,
            control,
            None,
            msg_type,
        );
    }
}

impl KvRpcService {
    // ── WatchSubscribe (fire-and-forget, no response) ─────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_watch_subscribe(&self, req: crowdb_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let span = request_span("watch_subscribe", self.store.store_id);
        let _entered = span.enter();
        let conn_handle_usize = req.conn_handle as usize;
        let Ok(fb_req) = flatbuffers::root::<FBWatchSubscribe>(req.control()) else {
            debug!("watch subscribe: invalid flatbuffer");
            return;
        };
        let group_id = fb_req.group_id();
        record_group_context(&self.store, group_id);
        let prefix = fb_req.prefix().map_or(&[][..], |v| v.bytes()).to_vec();

        let Some(group) = self.store.get_group(group_id) else {
            let conn = Connection::from_handle(conn_handle_usize as crowdb_rpc_ffi::sys::crowdb_rpc_conn_t);
            self.forwarder.send_watch_notify_error(
                server,
                &conn,
                group_id,
                "",
                &format!("group {group_id} not found on store {}", self.store.store_id),
            );
            return;
        };
        if !group.local_replica().is_leader() {
            let hint = group.leader_endpoint().unwrap_or_default();
            let conn = Connection::from_handle(conn_handle_usize as crowdb_rpc_ffi::sys::crowdb_rpc_conn_t);
            self.forwarder
                .send_watch_notify_error(server, &conn, group_id, &hint, "");
            return;
        }
        let conn = Connection::from_handle(conn_handle_usize as crowdb_rpc_ffi::sys::crowdb_rpc_conn_t);
        let target = Arc::new(crate::cluster::watch_registry::CrowdbRpcPushTarget::new(
            conn,
            Arc::clone(&self.forwarder.rpc),
            Arc::clone(server),
        ));
        let registry = group.watch_registry.clone();
        let watcher_id = registry.subscribe_crowdb_rpc(&prefix, target);
        debug!(
            group_id,
            watcher_id,
            prefix_len = prefix.len(),
            "watch subscribed (crowdb-rpc push target)"
        );
        // req dropped here, frame released
    }

    // ── WatchUnsubscribe (fire-and-forget, no response) ───────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_watch_unsubscribe(
        &self,
        req: crowdb_rpc_ffi::ServerRequest,
        _server: &Arc<RpcServer>,
    ) {
        let span = request_span("watch_unsubscribe", self.store.store_id);
        let _entered = span.enter();
        let Ok(fb_req) = flatbuffers::root::<FBWatchUnsubscribe>(req.control()) else {
            debug!("watch unsubscribe: invalid flatbuffer");
            return;
        };
        let group_id = fb_req.group_id();
        record_group_context(&self.store, group_id);
        let prefix = fb_req.prefix().map_or(&[][..], |v| v.bytes()).to_vec();

        let Some(group) = self.store.get_group(group_id) else {
            debug!("watch unsubscribe dropped (group not found)");
            return;
        };
        let _registry = group.watch_registry.clone();
        debug!(
            group_id,
            prefix_len = prefix.len(),
            "watch unsubscribe received (crowdb-rpc: lazy cleanup via dead-connection detection)"
        );
        // req dropped here, frame released
    }
}
