// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::diskio_fb::{
    FBDiskIoRetCode, FBDiskWriteRequest, FBDiskWriteRequestArgs, FBDiskWriteResponse,
    FBDiskWriteResponseArgs, FBInt128,
};
use flatbuffers::FlatBufferBuilder;

#[test]
fn synchronized_write_appends_fields_without_reusing_the_retired_fence_slot() {
    assert_eq!(FBDiskWriteRequest::VT_ORDERING_ZONE_OFFSET, 18);
    assert_eq!(FBDiskWriteRequest::VT_WRITE_CREATE_TIME_MS, 20);
    assert_eq!(FBDiskWriteRequest::VT_SYNC, 22);
    assert_eq!(FBDiskWriteResponse::VT_SYNC_COMPLETED, 10);
    let mut builder = FlatBufferBuilder::new();
    let disk = FBInt128([7; 16]);
    let offset = FBDiskWriteRequest::create(
        &mut builder,
        &FBDiskWriteRequestArgs {
            id: 41,
            disk_id: Some(&disk),
            zone_index: 3,
            zone_offset: 8192,
            ordering_zone_offset: 4096,
            write_create_time_ms: 12345,
            size: 4096,
            sync: true,
            ..Default::default()
        },
    );
    builder.finish(offset, None);
    let request = flatbuffers::root::<FBDiskWriteRequest>(builder.finished_data()).unwrap();
    assert!(request.sync());
    assert_eq!(request.id(), 41);
    assert_eq!(request.disk_id().unwrap().0, disk.0);
    assert_eq!(request.zone_index(), 3);
    assert_eq!(request.zone_offset(), 8192);
    assert_eq!(request.ordering_zone_offset(), 4096);
    assert_eq!(request.write_create_time_ms(), 12345);
    assert_eq!(request.size(), 4096);
}

#[test]
fn old_request_and_response_layouts_do_not_prove_synchronization() {
    let mut builder = FlatBufferBuilder::new();
    // Default false fields are omitted, retaining the pre-sync wire layout.
    let offset = FBDiskWriteRequest::create(
        &mut builder,
        &FBDiskWriteRequestArgs {
            id: 42,
            write_create_time_ms: 12345,
            ..Default::default()
        },
    );
    builder.finish(offset, None);
    let request = flatbuffers::root::<FBDiskWriteRequest>(builder.finished_data()).unwrap();
    assert!(!request.sync());
    builder.reset();
    let offset = FBDiskWriteResponse::create(
        &mut builder,
        &FBDiskWriteResponseArgs {
            id: 42,
            ret_code: FBDiskIoRetCode::Success,
            ..Default::default()
        },
    );
    builder.finish(offset, None);
    let response = flatbuffers::root::<FBDiskWriteResponse>(builder.finished_data()).unwrap();
    assert_eq!(response.ret_code(), FBDiskIoRetCode::Success);
    assert!(!response.sync_completed());
}

#[test]
fn synchronized_response_and_flush_failure_have_explicit_wire_values() {
    assert_eq!(FBDiskIoRetCode::FsyncFailed.0, 9);
    for (code, completed) in [
        (FBDiskIoRetCode::Success, true),
        (FBDiskIoRetCode::FsyncFailed, false),
    ] {
        let mut builder = FlatBufferBuilder::new();
        let offset = FBDiskWriteResponse::create(
            &mut builder,
            &FBDiskWriteResponseArgs {
                id: 43,
                ret_code: code,
                sync_completed: completed,
                ..Default::default()
            },
        );
        builder.finish(offset, None);
        let response = flatbuffers::root::<FBDiskWriteResponse>(builder.finished_data()).unwrap();
        assert_eq!(response.ret_code(), code);
        assert_eq!(response.sync_completed(), completed);
    }
}
