// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs};
use crowdb_rpc_ffi::{sys, Buffer, OwnedClientRoute, RpcClient, RpcClientHandle, RpcServer};
use flatbuffers::FlatBufferBuilder;

#[derive(Clone, Copy)]
pub enum TestReply {
    Success,
    Close,
    Timeout,
}

struct TestClient {
    rpc: Option<RpcClient>,
    destroyed: mpsc::Sender<ThreadId>,
}

impl RpcClientHandle for TestClient {
    fn rpc_client_handle(&self) -> *mut c_void {
        self.rpc.as_ref().unwrap().handle().cast()
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        drop(self.rpc.take());
        let _ = self.destroyed.send(thread::current().id());
    }
}

struct TestCompletion {
    lease: OwnedClientRoute,
    evicted: Arc<AtomicBool>,
    completed: mpsc::Sender<(i32, ThreadId, bool)>,
}

unsafe extern "C" fn complete(
    _request: u64,
    control: sys::crowdb_rpc_buffer_t,
    data: sys::crowdb_rpc_buffer_t,
    status: i32,
    context: *mut c_void,
) {
    // SAFETY: send transfers this unique box to exactly one completion. Both
    // response handles also belong to this callback, including error outcomes.
    let completion = unsafe { Box::from_raw(context.cast::<TestCompletion>()) };
    for buffer in [control, data] {
        if !buffer.is_null() {
            unsafe { sys::crowdb_rpc_buffer_release(buffer) };
        }
    }
    drop(completion.lease);
    let _ = completion.completed.send((
        status,
        thread::current().id(),
        completion.evicted.load(Ordering::Acquire),
    ));
}

pub fn check_evicted_route(reply: TestReply) {
    let rpc = RpcClient::new();
    rpc.set_completion_pool_size(8);
    rpc.start_reaper(1_000_000_000, 10_000_000);
    let remote = Arc::new(RpcServer::new(None));
    remote.listen("127.0.0.1", 0).unwrap();
    let release = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::channel();
    remote.register_owned_handler(31011, {
        let release = Arc::clone(&release);
        move |request| {
            let _ = entered_tx.send(());
            let deadline = Instant::now() + Duration::from_secs(3);
            while !release.load(Ordering::Acquire) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            match reply {
                TestReply::Success => {
                    let _ = request.reply(control(), None, 31012);
                }
                TestReply::Close => request.close_connection(),
                TestReply::Timeout => {}
            }
        }
    });
    remote.start();
    let server = Arc::new(RpcServer::new(None));
    server.start();
    let connection = server.connect("127.0.0.1", remote.port()).unwrap();
    rpc.attach(&connection);
    let (destroyed_tx, destroyed_rx) = mpsc::channel();
    let client = Arc::new(TestClient {
        rpc: Some(rpc),
        destroyed: destroyed_tx,
    });
    let weak_client = Arc::downgrade(&client);
    let weak_server = Arc::downgrade(&server);
    let cache = ArcSwapOption::from(Some(Arc::new(OwnedClientRoute::new(
        Arc::clone(&client),
        Arc::clone(&server),
        connection.clone(),
    ))));
    let (completed_tx, completed_rx) = mpsc::channel();
    let evicted = Arc::new(AtomicBool::new(false));
    let lease = cache.load_full().unwrap().as_ref().clone();
    let handles = lease.raw_handles();
    let completion = Box::into_raw(Box::new(TestCompletion {
        lease,
        evicted: Arc::clone(&evicted),
        completed: completed_tx,
    }));
    // Model the native transport: its lease is the sole owner before submission.
    // Setup cannot consume the request's timeout before cache eviction.
    drop(cache.swap(None));
    drop(connection);
    drop(client);
    drop(server);
    assert!(weak_client.upgrade().is_some());
    assert!(weak_server.upgrade().is_some());
    evicted.store(true, Ordering::Release);
    // SAFETY: the unique completion lease retains all matching raw handles;
    // success transfers the box to one callback, rejection leaves it to us.
    let submitted = unsafe {
        sys::crowdb_rpc_client_send(
            handles.0.cast(),
            handles.1.cast(),
            handles.2.cast(),
            1,
            control().into_raw(),
            std::ptr::null_mut(),
            31011,
            Some(complete),
            completion.cast(),
        )
    };
    if submitted != sys::CROWDB_RPC_OK {
        // SAFETY: a rejected submission guarantees callback ownership was not
        // transferred, so the caller must reclaim the original box.
        drop(unsafe { Box::from_raw(completion) });
    }
    assert_eq!(submitted, sys::CROWDB_RPC_OK);
    entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    release.store(true, Ordering::Release);
    let (status, callback_thread, after_eviction) =
        completed_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(after_eviction, "completion ran before last-owner eviction");
    let expected = match reply {
        TestReply::Success => sys::CROWDB_RPC_OK,
        TestReply::Close => sys::CROWDB_RPC_ERR_CONN_CLOSED,
        TestReply::Timeout => sys::CROWDB_RPC_ERR_TIMEOUT,
    };
    assert_eq!(status, expected);
    let destructor_thread = destroyed_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_ne!(destructor_thread, callback_thread);
    assert!(weak_client.upgrade().is_none());
    let deadline = Instant::now() + Duration::from_secs(3);
    while weak_server.upgrade().is_some() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert!(weak_server.upgrade().is_none());
    assert!(OwnedClientRoute::drain_retired_for_tests(Duration::from_secs(3)));
    remote.stop();
}

fn control() -> Buffer {
    let mut builder = FlatBufferBuilder::new();
    let ping = ConnectionPingRequest::create(
        &mut builder,
        &ConnectionPingRequestArgs {
            id: 1,
            rpc_create_nano: 0,
        },
    );
    builder.finish(ping, None);
    Buffer::from_bytes(builder.finished_data())
}
