// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Once;

pub(super) fn init() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let artifacts = std::env::var("CROWDB_E2E_ARTIFACTS").expect("fixture artifacts");
        crowdb_rpc_ffi::init_logging(&artifacts, "debug", 30, 5, "client-rpc");
        crowdb_rpc_ffi::add_log_stderr("error");
    });
}
