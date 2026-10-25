// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::TestResult;
use crowdb_console_shared::{lifecycle, ops::OpContext};
use crowdb_protocol::port::namespace::RuntimeNamespace;
use std::{collections::BTreeMap, fs, path::Path, time::Duration};

pub fn process_start(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let fields: Vec<_> = stat.rsplit_once(')')?.1.split_whitespace().collect();
        if fields[0] == "Z" {
            return None;
        }
        fields.get(19).map(|value| (*value).to_owned())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let output = std::process::Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "lstart="])
            .output()
            .ok()?;
        let value = String::from_utf8(output.stdout).ok()?;
        (!value.trim().is_empty()).then(|| value.trim().to_owned())
    }
}

pub struct TestOwnedProcesses {
    processes: BTreeMap<u32, String>,
    namespace: Option<RuntimeNamespace>,
}

impl TestOwnedProcesses {
    pub fn new(root: &Path) -> TestResult<Self> {
        if fs::read_dir(root)?.next().is_some() {
            return Err("regression fixture requires a fresh empty owned directory".into());
        }
        Ok(Self {
            processes: BTreeMap::new(),
            namespace: Some(RuntimeNamespace::persistent(root, "regression-fixture")?),
        })
    }
    pub fn record(&mut self, context: &OpContext) -> TestResult<()> {
        for server in &context.config().servers {
            if let Some(pid) = server.pid {
                if let Some(start) = process_start(pid) {
                    self.namespace.as_mut().unwrap().record_process(pid)?;
                    self.processes.insert(pid, start);
                }
            }
        }
        Ok(())
    }
    pub fn stop(&mut self) -> TestResult<()> {
        let mut errors = Vec::new();
        for (&pid, start) in self.processes.iter().rev() {
            if process_start(pid).as_ref() == Some(start) {
                if let Err(error) = lifecycle::stop_pid_with_timeout(pid, Duration::from_secs(5)) {
                    errors.push(format!("stop owned fixture process {pid}: {error}"));
                }
            }
        }
        if let Some(namespace) = self.namespace.take() {
            if let Err(error) = namespace.release() {
                errors.push(format!("release fixture namespace: {error}"));
            }
        }
        if let Err(error) = crowdb_protocol::port::namespace::release_process_ports() {
            errors.push(format!("release fixture process ports: {error}"));
        }
        if !errors.is_empty() {
            return Err(errors.join("\n").into());
        }
        self.processes.clear();
        Ok(())
    }

    pub async fn shutdown(&mut self) -> TestResult<()> {
        let processes = self.processes.clone();
        self.stop()?;
        // A zombie is stopped but still owns its Tokio child registration.
        // Keep the runtime alive until its reaper tasks release those handles.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                #[cfg(target_os = "linux")]
                let pending = processes.iter().any(|(pid, start)| {
                    fs::read_to_string(format!("/proc/{pid}/stat"))
                        .ok()
                        .and_then(|stat| {
                            stat.rsplit_once(')')?
                                .1
                                .split_whitespace()
                                .nth(19)
                                .map(str::to_owned)
                        })
                        .as_ref()
                        == Some(start)
                });
                #[cfg(not(target_os = "linux"))]
                let pending = processes
                    .iter()
                    .any(|(pid, start)| process_start(*pid).as_ref() == Some(start));
                if !pending {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "owned measurement child was not reaped before runtime shutdown")?;
        Ok(())
    }
}

impl Drop for TestOwnedProcesses {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("owned fixture cleanup failed: {error}");
        }
    }
}
