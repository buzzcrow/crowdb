// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Internal `DiskIO` wire transport.

use crowdb_common::RequestIdGen;
use crowdb_protocol::diskio_fb::{
    FBDiskFsyncRequest, FBDiskFsyncRequestArgs, FBDiskFsyncResponse, FBDiskReadRequest,
    FBDiskReadRequestArgs, FBDiskReadResponse, FBDiskWriteRequest, FBDiskWriteRequestArgs,
    FBDiskWriteResponse, FBInt128 as FBDiskInt128,
};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{
    Buffer, BufferChain, CallFuture, Connection, RpcClient, RpcClientHandle, RpcError, RpcServer,
};
use flatbuffers::FlatBufferBuilder;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

use crate::DiskId;

impl DiskId {
    pub(crate) fn to_fb(self) -> FBDiskInt128 {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.high.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.low.to_le_bytes());
        FBDiskInt128(bytes)
    }
}

/// Physical write address plus the base used to order related writes.
#[derive(Debug, Clone, Copy)]
pub struct WireWriteTarget {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub zone_offset: u64,
    pub ordering_zone_offset: u64,
}

/// Disk I/O return codes (mirrors `FBDiskIoRetCode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum DiskIoRetCode {
    Success = 0,
    DiskNotExist = 1,
    ZoneNotExist = 2,
    IoError = 3,
    PartialWrite = 4,
    InvalidAlignment = 5,
    ConnectionError = 6,
    /// Compatibility response from an older `DiskIO` server.
    StaleAllocation = 7,
    OldRequest = 8,
}

#[derive(Clone, Copy)]
struct WriteTarget {
    disk_id: DiskId,
    zone_index: u32,
    zone_offset: u64,
    size: u32,
    ordering_zone_offset: u64,
}

impl From<i16> for DiskIoRetCode {
    fn from(v: i16) -> Self {
        match v {
            0 => Self::Success,
            1 => Self::DiskNotExist,
            2 => Self::ZoneNotExist,
            4 => Self::PartialWrite,
            5 => Self::InvalidAlignment,
            6 => Self::ConnectionError,
            7 => Self::StaleAllocation,
            8 => Self::OldRequest,
            _ => Self::IoError,
        }
    }
}

/// Error type for diskio client operations.
#[derive(Debug, Error)]
pub enum WireError {
    #[error("disk I/O error: {0:?}")]
    IoError(DiskIoRetCode),
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),
    #[error("wire protocol error: {0}")]
    Protocol(String),
}

pub type WireResult<T> = std::result::Result<T, WireError>;

/// Sends `DiskIO` wire requests via crowdb-rpc.
pub struct WireClient {
    rpc: RpcClient,
    req_id_gen: RequestIdGen,
}

impl RpcClientHandle for WireClient {
    fn rpc_client_handle(&self) -> *mut std::ffi::c_void {
        self.rpc.handle().cast()
    }
}

impl std::fmt::Debug for WireClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireClient")
            .field("req_id_gen", &"RequestIdGen")
            .finish_non_exhaustive()
    }
}

impl WireClient {
    /// Create a wire client. Call `attach()` before issuing requests.
    #[must_use]
    pub fn new() -> Self {
        Self::with_completion_capacity(1024)
    }

    #[must_use]
    pub fn with_completion_capacity(capacity: usize) -> Self {
        let rpc = RpcClient::new();
        rpc.set_completion_pool_size(u32::try_from(capacity.max(1)).unwrap_or(u32::MAX));
        rpc.start_reaper(5_000_000_000, 500_000_000);
        Self {
            rpc,
            req_id_gen: RequestIdGen::new(),
        }
    }

    /// Attach to a connection (routes responses to this client).
    pub fn attach(&self, conn: &Connection) {
        self.rpc.attach(conn);
    }

    fn next_id(&self) -> u64 {
        self.req_id_gen.next().as_u64()
    }

    /// Send a disk write request. `data` is the payload to write.
    /// Returns a `CallFuture` that resolves to the response.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the data is too large or the send fails.
    #[cfg_attr(not(feature = "test-util"), allow(dead_code))]
    pub fn write(
        &self,
        server: &RpcServer,
        conn: &Connection,
        disk_id: DiskId,
        zone_index: u32,
        zone_offset: u64,
        data: Vec<u8>,
    ) -> Result<CallFuture, WireError> {
        let size = u32::try_from(data.len()).map_err(|_| WireError::Protocol("data too large".into()))?;
        self.write_buffer(
            server,
            conn,
            WriteTarget {
                disk_id,
                zone_index,
                zone_offset,
                size,
                ordering_zone_offset: zone_offset,
            },
            Buffer::from_vec(data),
        )
    }

    /// Send a disk write while retaining an owned `Bytes` allocation through
    /// RPC completion, without copying it into a `Vec` or C++ buffer.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the data is too large or the send fails.
    #[cfg_attr(not(feature = "test-util"), allow(dead_code))]
    pub fn write_bytes(
        &self,
        server: &RpcServer,
        conn: &Connection,
        disk_id: DiskId,
        zone_index: u32,
        zone_offset: u64,
        data: bytes::Bytes,
    ) -> Result<CallFuture, WireError> {
        let size = u32::try_from(data.len()).map_err(|_| WireError::Protocol("data too large".into()))?;
        self.write_buffer(
            server,
            conn,
            WriteTarget {
                disk_id,
                zone_index,
                zone_offset,
                size,
                ordering_zone_offset: zone_offset,
            },
            Buffer::from_owned_bytes(data),
        )
    }

    /// Send a segment write. `DiskIO` orders writes sharing the target's
    /// ordering base so partial-block updates cannot race each other.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the data is too large or the send fails.
    pub fn write_segment_bytes(
        &self,
        server: &RpcServer,
        conn: &Connection,
        target: WireWriteTarget,
        data: bytes::Bytes,
    ) -> Result<CallFuture, WireError> {
        let size = u32::try_from(data.len()).map_err(|_| WireError::Protocol("data too large".into()))?;
        self.write_buffer(
            server,
            conn,
            WriteTarget {
                disk_id: target.disk_id,
                zone_index: target.zone_index,
                zone_offset: target.zone_offset,
                size,
                ordering_zone_offset: target.ordering_zone_offset,
            },
            Buffer::from_owned_bytes(data),
        )
    }

    /// Send a segment write as one bounded immutable RPC data-view chain.
    ///
    /// # Errors
    ///
    /// Returns a protocol error when the chain violates the transport bound,
    /// or an RPC error when submission fails.
    pub fn write_segment_views(
        &self,
        server: &RpcServer,
        conn: &Connection,
        target: WireWriteTarget,
        data: Vec<bytes::Bytes>,
    ) -> Result<CallFuture, WireError> {
        let data =
            BufferChain::from_owned_bytes(data).map_err(|error| WireError::Protocol(error.to_string()))?;
        let size = data.len();
        let req_id = self.next_id();
        let mut fbb = FlatBufferBuilder::new();
        let fb_disk_id = target.disk_id.to_fb();
        let off = FBDiskWriteRequest::create(
            &mut fbb,
            &FBDiskWriteRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                disk_id: Some(&fb_disk_id),
                zone_index: target.zone_index,
                zone_offset: target.zone_offset,
                size,
                ordering_zone_offset: target.ordering_zone_offset,
                write_create_time_ms: unix_time_ms(),
            },
        );
        fbb.finish(off, None);
        let control = Buffer::from_bytes(fbb.finished_data());
        self.rpc
            .call_chain(
                server,
                conn,
                req_id,
                control,
                data,
                FBMsgType::EDiskWriteRequest.0 as u16,
            )
            .map_err(WireError::from)
    }

    fn write_buffer(
        &self,
        server: &RpcServer,
        conn: &Connection,
        target: WriteTarget,
        data_buf: Buffer,
    ) -> Result<CallFuture, WireError> {
        let req_id = self.next_id();
        let mut fbb = FlatBufferBuilder::new();
        let fb_disk_id = target.disk_id.to_fb();
        let off = FBDiskWriteRequest::create(
            &mut fbb,
            &FBDiskWriteRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                disk_id: Some(&fb_disk_id),
                zone_index: target.zone_index,
                zone_offset: target.zone_offset,
                size: target.size,
                ordering_zone_offset: target.ordering_zone_offset,
                write_create_time_ms: unix_time_ms(),
            },
        );
        fbb.finish(off, None);
        let control = Buffer::from_bytes(fbb.finished_data());
        let msg_type = FBMsgType::EDiskWriteRequest.0 as u16;
        self.rpc
            .call(server, conn, req_id, control, Some(data_buf), msg_type)
            .map_err(WireError::from)
    }

    /// Send a disk read request. Returns a `CallFuture` that resolves to
    /// the response (`ret_code` + data).
    ///
    /// `test_pattern_offset` is used by `NullDisk` for deterministic content
    /// generation (testing only); real engines ignore it. Pass the physical
    /// offset (`zone_index * zone_size + zone_offset`) for raw disk reads,
    /// or a logical object offset for object reads.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the send fails.
    #[allow(clippy::too_many_arguments)]
    pub fn read(
        &self,
        server: &RpcServer,
        conn: &Connection,
        disk_id: DiskId,
        zone_index: u32,
        zone_offset: u64,
        size: u32,
        test_pattern_offset: u64,
    ) -> Result<CallFuture, WireError> {
        let req_id = self.next_id();
        let mut fbb = FlatBufferBuilder::new();
        let fb_disk_id = disk_id.to_fb();
        let off = FBDiskReadRequest::create(
            &mut fbb,
            &FBDiskReadRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                disk_id: Some(&fb_disk_id),
                zone_index,
                zone_offset,
                size,
                test_pattern_offset,
            },
        );
        fbb.finish(off, None);
        let control = Buffer::from_bytes(fbb.finished_data());
        let msg_type = FBMsgType::EDiskReadRequest.0 as u16;
        self.rpc
            .call(server, conn, req_id, control, None, msg_type)
            .map_err(WireError::from)
    }

    /// Send a disk fsync request. Returns a `CallFuture` that resolves to
    /// the response.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the send fails.
    pub fn fsync(
        &self,
        server: &RpcServer,
        conn: &Connection,
        disk_id: DiskId,
    ) -> Result<CallFuture, WireError> {
        let req_id = self.next_id();
        let mut fbb = FlatBufferBuilder::new();
        let fb_disk_id = disk_id.to_fb();
        let off = FBDiskFsyncRequest::create(
            &mut fbb,
            &FBDiskFsyncRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                disk_id: Some(&fb_disk_id),
            },
        );
        fbb.finish(off, None);
        let control = Buffer::from_bytes(fbb.finished_data());
        let msg_type = FBMsgType::EDiskFsyncRequest.0 as u16;
        self.rpc
            .call(server, conn, req_id, control, None, msg_type)
            .map_err(WireError::from)
    }

    /// Parse a write response from a completed `CallFuture`.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the response is missing or invalid.
    pub async fn await_write_response(fut: CallFuture) -> WireResult<DiskIoRetCode> {
        let resp = fut.await.map_err(WireError::from)?;
        parse_ret_code(&resp)
    }

    /// Parse a read response from a completed `CallFuture`.
    /// Returns (`ret_code`, data) on success.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the response is missing or invalid.
    pub async fn await_read_response(fut: CallFuture) -> WireResult<(DiskIoRetCode, Option<Vec<u8>>)> {
        let resp = fut.await.map_err(WireError::from)?;
        let code = parse_ret_code(&resp)?;
        let data = resp.data.map(|b| b.bytes().to_vec());
        Ok((code, data))
    }

    /// Parse an fsync response from a completed `CallFuture`.
    ///
    /// # Errors
    ///
    /// Returns `DiskioError::Rpc` if the response is missing or invalid.
    #[cfg_attr(not(feature = "test-util"), allow(dead_code))]
    pub async fn await_fsync_response(fut: CallFuture) -> WireResult<DiskIoRetCode> {
        Self::await_write_response(fut).await
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

impl Default for WireClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse the `ret_code` from a diskio response control buffer.
fn parse_ret_code(resp: &crowdb_rpc_ffi::Response) -> WireResult<DiskIoRetCode> {
    let ctrl = resp
        .control
        .as_ref()
        .ok_or_else(|| WireError::Protocol("missing control buffer in response".into()))?;
    let ctrl_bytes = ctrl.bytes();
    let raw = if let Ok(r) = flatbuffers::root::<FBDiskWriteResponse>(ctrl_bytes) {
        r.ret_code().0
    } else if let Ok(r) = flatbuffers::root::<FBDiskReadResponse>(ctrl_bytes) {
        r.ret_code().0
    } else if let Ok(r) = flatbuffers::root::<FBDiskFsyncResponse>(ctrl_bytes) {
        r.ret_code().0
    } else {
        return Err(WireError::Protocol("invalid response flatbuffer".into()));
    };
    let code = DiskIoRetCode::from(raw);
    if code == DiskIoRetCode::Success {
        Ok(code)
    } else {
        Err(WireError::IoError(code))
    }
}
