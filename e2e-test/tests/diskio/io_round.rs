// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::pattern::null_disk_pattern;
use crowdb_diskio_client::{DiskId as DioDiskId, DiskIoRetCode, TestWireDiskioClient as DiskioClient};
use crowdb_rpc_ffi::RpcServer;
/// Which backend to test.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Mem,
    Null,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl Backend {
    pub fn name(self) -> &'static str {
        match self {
            Self::Mem => "MemDisk",
            Self::Null => "NullDisk",
        }
    }
}

/// Parameters for `test_io_round`, grouped to avoid too-many-arguments.
pub struct IoRoundParams {
    pub backend: Backend,
    pub disk_id: DioDiskId,
    pub zone_index: u32,
    pub zone_offset: u64,
    pub size: usize,
    pub label: &'static str,
}

/// Run a write+read+verify + overwrite cycle at the given size.
///
/// For `MemDisk`: read after write returns the same bytes that were
/// written; read after overwrite returns the second write's bytes.
///
/// For `NullDisk`: read always returns the deterministic pattern
/// regardless of what was written.
#[allow(clippy::too_many_lines)]
pub async fn test_io_round(
    dio_client: &DiskioClient,
    server: &RpcServer,
    conn: &crowdb_rpc_ffi::Connection,
    params: &IoRoundParams,
) {
    let IoRoundParams {
        backend,
        disk_id,
        zone_index,
        zone_offset,
        size,
        label,
    } = *params;
    let zone_size_units = crate::common::hardware::ZONE_SIZE_UNITS;
    let unit_size_bytes = crate::common::hardware::UNIT_SIZE_BYTES;
    let phys_offset = u64::from(zone_index) * (zone_size_units * u64::from(unit_size_bytes)) + zone_offset;
    let read_size = u32::try_from(size).unwrap();

    // ── Write + read + verify ──
    let write_data: Vec<u8> = (0..size)
        .map(|i| u8::try_from((i * 7 + 13) % 256).unwrap())
        .collect();
    let write_fut = dio_client
        .write(server, conn, disk_id, zone_index, zone_offset, write_data.clone())
        .expect("write send");
    let write_code = DiskioClient::await_write_response(write_fut)
        .await
        .expect("write IO");
    assert_eq!(
        write_code,
        DiskIoRetCode::Success,
        "{label}: {backend} write should succeed"
    );

    let read_fut = dio_client
        .read(
            server,
            conn,
            disk_id,
            zone_index,
            zone_offset,
            read_size,
            phys_offset,
        )
        .expect("read send");
    let (read_code, read_data) = DiskioClient::await_read_response(read_fut)
        .await
        .expect("read IO");
    assert_eq!(
        read_code,
        DiskIoRetCode::Success,
        "{label}: {backend} read should succeed"
    );
    let read_data = read_data.unwrap_or_default();
    assert_eq!(read_data.len(), size, "{label}: {backend} read length mismatch");

    if size > 0 {
        match backend {
            Backend::Mem => {
                assert_eq!(
                    read_data, write_data,
                    "{label}: {backend} read data must match written data"
                );
            }
            Backend::Null => {
                let expected = null_disk_pattern(disk_id, phys_offset, size);
                assert_eq!(
                    read_data, expected,
                    "{label}: {backend} read data must match computed pattern"
                );
            }
        }
    }

    // ── Overwrite: write different data, read back, verify ──
    if size > 0 {
        let overwrite_data: Vec<u8> = (0..size)
            .map(|i| u8::try_from((i * 3 + 99) % 256).unwrap())
            .collect();
        let ow_fut = dio_client
            .write(
                server,
                conn,
                disk_id,
                zone_index,
                zone_offset,
                overwrite_data.clone(),
            )
            .expect("overwrite send");
        let ow_code = DiskioClient::await_write_response(ow_fut)
            .await
            .expect("overwrite IO");
        assert_eq!(
            ow_code,
            DiskIoRetCode::Success,
            "{label}: {backend} overwrite should succeed"
        );

        let ow_read_fut = dio_client
            .read(
                server,
                conn,
                disk_id,
                zone_index,
                zone_offset,
                read_size,
                phys_offset,
            )
            .expect("overwrite read send");
        let (ow_read_code, ow_read_data) = DiskioClient::await_read_response(ow_read_fut)
            .await
            .expect("overwrite read IO");
        assert_eq!(
            ow_read_code,
            DiskIoRetCode::Success,
            "{label}: {backend} overwrite read should succeed"
        );
        let ow_read_data = ow_read_data.unwrap_or_default();
        assert_eq!(
            ow_read_data.len(),
            size,
            "{label}: {backend} overwrite read length mismatch"
        );
        match backend {
            Backend::Mem => {
                assert_eq!(
                    ow_read_data, overwrite_data,
                    "{label}: {backend} overwrite read must match second write"
                );
            }
            Backend::Null => {
                let expected = null_disk_pattern(disk_id, phys_offset, size);
                assert_eq!(
                    ow_read_data, expected,
                    "{label}: {backend} overwrite read should still match pattern"
                );
            }
        }
    }

    // ── Fsync ──
    let fsync_fut = dio_client.fsync(server, conn, disk_id).expect("fsync send");
    let fsync_code = DiskioClient::await_fsync_response(fsync_fut)
        .await
        .expect("fsync IO");
    assert_eq!(
        fsync_code,
        DiskIoRetCode::Success,
        "{label}: {backend} fsync should succeed"
    );

    eprintln!(
        "  {label}: {backend} write+read+overwrite+fsync OK ({size} bytes, zone {zone_index} offset {zone_offset})"
    );
}

/// Test read-before-write: reading from an uninitialized area of the
/// disk. Uses a fresh disk (not written to by `wait_for_disks`) and a
/// zone-0 offset within the memfd's initial ftruncate size.
///
/// For `MemDisk`: the memfd is zero-filled, so reads return zeros.
/// For `NullDisk`: reads return the deterministic pattern.
pub async fn test_read_before_write(
    dio_client: &DiskioClient,
    server: &RpcServer,
    conn: &crowdb_rpc_ffi::Connection,
    backend: Backend,
    disk_id: DioDiskId,
) {
    let zone_index = 0u32;
    let zone_offset = 1024 * 1024u64; // 1 MB
    let size = 4096usize;
    let zone_size_units = crate::common::hardware::ZONE_SIZE_UNITS;
    let unit_size_bytes = crate::common::hardware::UNIT_SIZE_BYTES;
    let phys_offset = u64::from(zone_index) * (zone_size_units * u64::from(unit_size_bytes)) + zone_offset;
    let read_size = u32::try_from(size).unwrap();

    let rf = dio_client
        .read(
            server,
            conn,
            disk_id,
            zone_index,
            zone_offset,
            read_size,
            phys_offset,
        )
        .expect("read-before-write send");
    let (code, data) = DiskioClient::await_read_response(rf)
        .await
        .expect("read-before-write IO");
    assert_eq!(
        code,
        DiskIoRetCode::Success,
        "{backend}: read-before-write should succeed"
    );
    let data = data.expect("read-before-write data should be present");
    assert_eq!(data.len(), size, "{backend}: read-before-write length mismatch");
    match backend {
        Backend::Mem => {
            assert!(
                data.iter().all(|&b| b == 0),
                "{backend}: read-before-write should return zeros (uninitialized memfd)"
            );
        }
        Backend::Null => {
            let expected = null_disk_pattern(disk_id, phys_offset, size);
            assert_eq!(
                data, expected,
                "{backend}: read-before-write should match pattern"
            );
        }
    }
    eprintln!("  {backend} read-before-write OK (4 KB at zone 0 offset 1 MB, uninitialized)");
}

// ── concurrent benchmark ─────────────────────────────────────────
