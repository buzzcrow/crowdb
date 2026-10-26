// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::SDK_PRESSURE_SCRIPT;
use crowdb_access_iceberg::key::OperationId;
use std::{
    path::PathBuf,
    process::{Child, Command, ExitStatus},
};

pub(super) struct TestSdkPressure {
    child: Child,
    directory: PathBuf,
}

impl TestSdkPressure {
    pub(super) fn start(origin: &str) -> Self {
        let directory = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap())
            .join(format!("gc-foreground-sdk-{}", OperationId::random()));
        std::fs::create_dir(&directory).unwrap();
        let output = std::fs::File::create(directory.join("stdout.log")).unwrap();
        let child = Command::new("pixi")
            .args(["run", "-e", "iceberg-e2e", "python", "-c", SDK_PRESSURE_SCRIPT])
            .args([origin, "0", "0", "gc-pressure"])
            .arg(&directory)
            .stdout(output)
            .spawn()
            .unwrap();
        Self { child, directory }
    }

    pub(super) fn progress(&self) -> usize {
        (0..4)
            .map(|worker| {
                let path = self.directory.join(format!("progress-{worker}"));
                match std::fs::read_to_string(path) {
                    Ok(count) => count.parse::<usize>().unwrap(),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                    Err(error) => panic!("SDK progress is unreadable: {error}"),
                }
            })
            .sum()
    }

    pub(super) fn ready(&self) -> bool {
        (0..4).all(|worker| self.directory.join(format!("ready-{worker}")).exists())
    }

    pub(super) fn release(&self) {
        std::fs::write(self.directory.join("run"), b"").unwrap();
    }

    pub(super) fn stop(&self) {
        std::fs::write(self.directory.join("stop"), b"").unwrap();
    }

    pub(super) fn try_wait(&mut self) -> Option<ExitStatus> {
        self.child.try_wait().unwrap()
    }

    pub(super) fn print_report(&self) {
        print!(
            "{}",
            std::fs::read_to_string(self.directory.join("stdout.log")).unwrap()
        );
    }
}

impl Drop for TestSdkPressure {
    fn drop(&mut self) {
        let _ = std::fs::write(self.directory.join("stop"), b"");
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
