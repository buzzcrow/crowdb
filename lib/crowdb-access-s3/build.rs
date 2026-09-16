// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::process::Command;

fn main() {
    const SCHEMA: &str = "src/fbs/s3_metadata.fbs";
    println!("cargo:rerun-if-changed={SCHEMA}");
    let output = std::env::var("OUT_DIR").expect("OUT_DIR is set for build scripts");
    let status = Command::new("flatc")
        .args(["--rust", "--gen-all", "-o", &output, SCHEMA])
        .status()
        .expect("flatc must be available through pixi");
    assert!(status.success(), "flatc failed for S3 metadata schema");
}
