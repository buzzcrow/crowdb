// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A fake child with a controllable readiness window for ownership tests.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub(super) struct TestKvChild {
    pub(super) binary: PathBuf,
    hold: PathBuf,
}

impl TestKvChild {
    pub(super) fn new(root: &Path) -> Self {
        let binary = root.join("fake-kv-child");
        let hold = root.join("hold-child-readiness");
        let script = format!(
            r#"#!/usr/bin/env python3
import http.server
import pathlib
import sys
import time
hold = pathlib.Path({})
while hold.exists():
    time.sleep(0.01)
port = int(sys.argv[sys.argv.index("--management-port") + 1])
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
            serde_json::to_string(hold.to_str().unwrap()).unwrap()
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
