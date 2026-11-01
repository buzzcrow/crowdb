// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use crowdb_rpc_ffi::{OwnedClientRoute, RpcClient, RpcClientHandle, RpcServer};

pub enum TestCleanup {
    None,
    ExitDelay,
    Cascade(Option<OwnedClientRoute>),
    Block {
        entered: mpsc::Sender<()>,
        release: Arc<AtomicBool>,
    },
}

pub struct TestReleaseGuard(pub Arc<AtomicBool>);

impl Drop for TestReleaseGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

struct TestClient {
    rpc: Option<RpcClient>,
    cleanup: TestCleanup,
    released: mpsc::Sender<()>,
}

impl RpcClientHandle for TestClient {
    fn rpc_client_handle(&self) -> *mut c_void {
        self.rpc.as_ref().unwrap().handle().cast()
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        match &mut self.cleanup {
            TestCleanup::None => {}
            TestCleanup::ExitDelay => {
                std::thread::sleep(Duration::from_millis(100));
                eprintln!("ROUTE_RETIREMENT_EXIT_DRAINED");
            }
            TestCleanup::Cascade(child) => drop(child.take()),
            TestCleanup::Block { entered, release } => {
                let _ = entered.send(());
                let deadline = Instant::now() + Duration::from_secs(3);
                while !release.load(Ordering::Acquire) && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
        drop(self.rpc.take());
        let _ = self.released.send(());
    }
}

pub fn route(cleanup: TestCleanup, released: mpsc::Sender<()>) -> OwnedClientRoute {
    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).unwrap();
    server.start();
    let connection = server.connect("127.0.0.1", server.port()).unwrap();
    let rpc = RpcClient::new();
    rpc.attach(&connection);
    OwnedClientRoute::new(
        Arc::new(TestClient {
            rpc: Some(rpc),
            cleanup,
            released,
        }),
        server,
        connection,
    )
}
