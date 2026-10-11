// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A fake child with a controllable readiness window for ownership tests.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub(super) struct TestChild {
    pub(super) binary: PathBuf,
    hold: PathBuf,
}

impl TestChild {
    pub(super) fn new(root: &Path) -> Self {
        Self::write(
            root.join("fake-kv-child"),
            root.join("hold-kv-readiness"),
            "--management-port",
        )
    }

    pub(super) fn diskdb(root: &Path) -> Self {
        Self::write(
            root.join("N-1/bin/crowdb-diskdb"),
            root.join("hold-diskdb-readiness"),
            "--http-addr",
        )
    }

    fn write(binary: PathBuf, hold: PathBuf, port_argument: &str) -> Self {
        std::fs::create_dir_all(binary.parent().unwrap()).unwrap();
        let script = format!(
            r#"#!/usr/bin/env python3
import http.server
import pathlib
import sys
import time
hold = pathlib.Path({})
while hold.exists():
    time.sleep(0.01)
port = int(sys.argv[sys.argv.index({}) + 1].rsplit(":", 1)[-1])
class Health(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{{"status":"ok"}}'
        self.send_response(200 if self.path == "/health" else 404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
http.server.HTTPServer(("127.0.0.1", port), Health).serve_forever()
"#,
            serde_json::to_string(hold.to_str().unwrap()).unwrap(),
            serde_json::to_string(port_argument).unwrap()
        );
        std::fs::write(&binary, script).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self { binary, hold }
    }

    pub(super) fn block_readiness(&self) {
        std::fs::write(&self.hold, b"hold").unwrap();
    }

    pub(super) fn release_readiness(&self) {
        std::fs::remove_file(&self.hold).unwrap();
    }
}
