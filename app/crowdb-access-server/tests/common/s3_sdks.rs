// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;
use std::process::Command;

pub fn run(language: &str, listen: &str, access_key: &str, secret_key: &str) {
    assert!(matches!(language, "java" | "js" | "go"), "unknown S3 SDK");
    let output = Command::new("timeout")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .args(["--signal=TERM", "--kill-after=30s", "700s", "pixi", "run", "-e"])
        .arg(format!("s3-{language}-sdk"))
        .args([
            "--",
            "bash",
            "tools/pixi-tasks/test-s3-sdk.sh",
            language,
            "verify",
        ])
        .env("CROWDB_S3_E2E_ENDPOINT", format!("http://{listen}"))
        .env("CROWDB_S3_E2E_ACCESS_KEY", access_key)
        .env("CROWDB_S3_E2E_SECRET_KEY", secret_key)
        .output()
        .expect("run optional S3 language SDK");
    let redact = |bytes: &[u8]| {
        String::from_utf8_lossy(bytes)
            .replace(access_key, "<redacted>")
            .replace(secret_key, "<redacted>")
    };
    println!("{}", redact(&output.stdout));
    assert!(
        output.status.success(),
        "{language} SDK failed: {}",
        redact(&output.stderr)
    );
}
