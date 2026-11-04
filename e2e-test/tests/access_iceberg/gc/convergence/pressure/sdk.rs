// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Own the real SDK process across warmup, endpoint restarts and release.

use super::super::super::foreground::SDK_PRESSURE_SCRIPT;
use crowdb_access_iceberg::key::OperationId;
use std::os::unix::process::CommandExt;
use std::{
    path::PathBuf,
    process::{Child, ExitStatus},
    time::{Duration, Instant},
};

pub(super) struct TestSdkWorker {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
    control: PathBuf,
    released: bool,
    reported: bool,
    cleanup: Option<Result<(), String>>,
}

impl TestSdkWorker {
    pub(super) fn prepare(origin: &str) -> Result<Self, String> {
        let stdout = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").map_err(|error| error.to_string())?)
            .join(format!("gc-inline-sdk-{}.log", OperationId::random()));
        let stderr = stdout.with_extension("stderr.log");
        let control = stdout.with_extension("control");
        std::fs::create_dir(&control).map_err(|error| error.to_string())?;
        let output = std::fs::File::create(&stdout).map_err(|error| error.to_string())?;
        let errors = std::fs::File::create(&stderr).map_err(|error| error.to_string())?;
        let mut command = std::process::Command::new("pixi");
        command
            .args(["run", "-e", "iceberg-e2e", "python", "-c", SDK_PRESSURE_SCRIPT])
            .args([origin, "12", "0.5", "gc-convergence"])
            .arg(&control)
            .stdout(output)
            .stderr(errors)
            .process_group(0);
        let child = command.spawn().map_err(|error| error.to_string())?;
        Ok(Self {
            child,
            stdout,
            stderr,
            control,
            released: false,
            reported: false,
            cleanup: None,
        })
    }

    pub(super) async fn wait_ready(&mut self) -> Result<(), String> {
        loop {
            if self.ready()? {
                println!(
                    "gc_foreground_ready {}",
                    serde_json::json!({"workers":4,"run_exists":false,"stdout":self.stdout,"stderr":self.stderr})
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn ready(&mut self) -> Result<bool, String> {
        if let Some(status) = self.try_wait()? {
            return Err(format!("official SDK exited before release: {status}"));
        }
        if self
            .control
            .join("run")
            .try_exists()
            .map_err(|error| error.to_string())?
        {
            return Err("official SDK was released before warmup completed".into());
        }
        for worker in 0..4 {
            if !self
                .control
                .join(format!("ready-{worker}"))
                .try_exists()
                .map_err(|error| error.to_string())?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn check_failure(&self) -> Result<(), String> {
        for worker in 0..4 {
            let failed = self.control.join(format!("failed-{worker}"));
            if failed.try_exists().map_err(|error| error.to_string())? {
                return Err(format!(
                    "SDK worker {worker} failed: {}",
                    std::fs::read_to_string(failed).map_err(|error| error.to_string())?
                ));
            }
        }
        Ok(())
    }

    pub(super) fn release(&mut self) -> Result<(), String> {
        if self.released || !self.ready()? {
            return Err("official SDK release requires four parked ready workers".into());
        }
        std::fs::write(self.control.join("run"), b"").map_err(|error| error.to_string())?;
        self.released = true;
        Ok(())
    }

    pub(super) fn released(&self) -> bool {
        self.released
    }

    pub(super) fn try_wait(&mut self) -> Result<Option<ExitStatus>, String> {
        self.check_failure()?;
        self.child.try_wait().map_err(|error| error.to_string())
    }

    pub(super) fn stop(&self) -> Result<(), String> {
        std::fs::write(self.control.join("stop"), b"")
            .map_err(|error| format!("SDK stop signal {}: {error}", self.control.display()))
    }

    pub(super) fn stdout(&self) -> &std::path::Path {
        &self.stdout
    }

    pub(super) fn output(&mut self) -> Result<String, String> {
        let stdout = std::fs::read_to_string(&self.stdout).map_err(|error| error.to_string())?;
        if !self.reported {
            print!("{stdout}");
            match std::fs::read_to_string(&self.stderr) {
                Ok(stderr) => eprint!("{stderr}"),
                Err(error) => eprintln!("SDK stderr artifact {}: {error}", self.stderr.display()),
            }
            self.reported = true;
        }
        Ok(stdout)
    }

    pub(super) fn terminate(&mut self) -> Result<(), String> {
        if let Some(result) = &self.cleanup {
            return result.clone();
        }
        let mut errors = self.stop().err().into_iter().collect::<Vec<_>>();
        let started = Instant::now();
        let deadline = started + Duration::from_secs(2);
        match self.wait_until(started + Duration::from_millis(1500)) {
            Ok(true) => {}
            result => {
                if let Err(error) = result {
                    errors.push(error);
                }
                // A reaped launcher does not imply that its Python descendants
                // exited. Kill only the new group created by this spawn.
                if let Err(error) = self.signal_group("-KILL") {
                    errors.push(error);
                }
                match self.wait_until(deadline) {
                    Ok(true) => {}
                    Ok(false) => errors.push(format!(
                        "SDK owned group {} did not exit and reap within cleanup's two seconds",
                        self.child.id()
                    )),
                    Err(error) => errors.push(error),
                }
            }
        }
        let result = if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("\n"))
        };
        self.cleanup = Some(result.clone());
        result
    }

    fn wait_until(&mut self, deadline: Instant) -> Result<bool, String> {
        loop {
            let reaped = self
                .child
                .try_wait()
                .map_err(|error| format!("SDK child status: {error}"))?
                .is_some();
            if !self.signal_group("-0")? && reaped {
                // try_wait has cached the exit status, so wait cannot block.
                self.child
                    .wait()
                    .map_err(|error| format!("SDK child wait: {error}"))?;
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn signal_group(&self, signal: &str) -> Result<bool, String> {
        let output = std::process::Command::new("/bin/kill")
            .env("LC_ALL", "C")
            .args([signal, "--", &format!("-{}", self.child.id())])
            .output()
            .map_err(|error| format!("SDK owned group {} {signal}: {error}", self.child.id()))?;
        if output.status.success() {
            Ok(true)
        } else if String::from_utf8_lossy(&output.stderr).contains("No such process") {
            Ok(false)
        } else {
            Err(format!(
                "SDK owned group {} {signal}: {}: {}{}",
                self.child.id(),
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }
}

impl Drop for TestSdkWorker {
    fn drop(&mut self) {
        if let Err(error) = self.terminate() {
            eprintln!("SDK cleanup also failed: {error}");
        }
        if let Err(error) = self.output() {
            eprintln!("SDK stdout artifact {}: {error}", self.stdout.display());
        }
    }
}
