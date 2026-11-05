// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::fb::ConnectionPingRequest;
use crowdb_rpc_ffi::RpcError;

/// The receiver extracts the RPC ID from the control table, not `OutFrame`.
/// Forwarding uses an independent ID on its leader connection; restore the
/// caller's ID while preserving the response payload and logical request ID.
pub(super) fn forwarded_control(bytes: &[u8], caller_id: u64) -> Result<Vec<u8>, RpcError> {
    // KV responses share the common id/rpc_create_nano table prefix.
    let common = flatbuffers::root::<ConnectionPingRequest>(bytes).map_err(|_| RpcError::ConnectionError)?;
    let offset = common._tab.vtable().get(ConnectionPingRequest::VT_ID);
    if offset == 0 {
        return Err(RpcError::ConnectionError);
    }
    let position = common._tab.loc() + usize::from(offset);
    let mut control = bytes.to_vec();
    control
        .get_mut(position..position + size_of::<u64>())
        .ok_or(RpcError::ConnectionError)?
        .copy_from_slice(&caller_id.to_le_bytes());
    Ok(control)
}
