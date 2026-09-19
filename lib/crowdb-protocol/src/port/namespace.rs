// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Runtime namespace ownership for stable ports and generated paths.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use super::ports::ServicePort;

const MANIFEST_VERSION: u32 = 1;
static NAMESPACE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceMode {
    Ephemeral,
    Persistent,
}

#[derive(Debug, Serialize, Deserialize)]
struct NamespaceManifest {
    version: u32,
    id: String,
    name: String,
    mode: NamespaceMode,
    owner_pid: u32,
    owner_start: String,
    assignments: BTreeMap<String, u16>,
    #[serde(default)]
    processes: Vec<ProcessOwner>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProcessOwner {
    pid: u32,
    start: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PortClaim {
    port: u16,
    namespace_id: String,
    mode: NamespaceMode,
    owner_pid: u32,
    owner_start: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeNamespaceError {
    #[error("runtime namespace I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("runtime namespace manifest error: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("unsupported runtime namespace manifest version {0}")]
    UnsupportedVersion(u32),
    #[error("runtime namespace {actual:?} does not match requested identity {expected:?}")]
    IdentityMismatch { expected: String, actual: String },
    #[error("port {port} is claimed by runtime namespace {owner:?}")]
    PortConflict { port: u16, owner: String },
    #[error("port {port} is occupied outside the runtime namespace registry")]
    PortOccupied { port: u16 },
    #[error("no free port for {service:?} logical identity {identity:?}")]
    Exhausted { service: ServicePort, identity: String },
}

/// One ownership boundary for generated paths and stable service ports.
pub struct RuntimeNamespace {
    root: PathBuf,
    registry_path: PathBuf,
    manifest: NamespaceManifest,
    cleanup: bool,
    released: bool,
}

impl RuntimeNamespace {
    /// Create one disposable namespace below the workspace runtime root.
    ///
    /// # Errors
    /// Returns an error when the namespace directories or manifest cannot be
    /// created.
    pub fn ephemeral(tag: &str) -> Result<Self, RuntimeNamespaceError> {
        let runtime_root = runtime_root();
        let id = format!(
            "{}-{}-{}",
            sanitize(tag),
            std::process::id(),
            NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        Self::create(
            runtime_root.join("ephemeral").join(&id),
            &runtime_root,
            sanitize(tag),
            id,
            NamespaceMode::Ephemeral,
        )
    }

    /// Create a durable namespace at an operator-selected root.
    ///
    /// An existing manifest is reopened without changing its paths or port
    /// assignments.
    ///
    /// # Errors
    /// Returns an error when the namespace cannot be created or reopened, or
    /// when its saved identity or format does not match the request.
    pub fn persistent(root: impl Into<PathBuf>, id: &str) -> Result<Self, RuntimeNamespaceError> {
        let runtime_root = runtime_root();
        let root = root.into();
        let name = sanitize(id);
        if root.join("namespace.json").is_file() {
            return Self::reopen_persistent(root, &runtime_root, &name);
        }
        let unique_id = format!(
            "{}-{}-{}",
            name,
            std::process::id(),
            NAMESPACE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        Self::create(root, &runtime_root, name, unique_id, NamespaceMode::Persistent)
    }

    fn create(
        root: PathBuf,
        runtime_root: &Path,
        name: String,
        id: String,
        mode: NamespaceMode,
    ) -> Result<Self, RuntimeNamespaceError> {
        for child in ["data", "config", "log", "artifacts", "services"] {
            fs::create_dir_all(root.join(child))?;
        }
        fs::create_dir_all(runtime_root.join("ports"))?;
        let owner_pid = std::process::id();
        let owner_start = process_start(owner_pid).unwrap_or_else(|| format!("process-{owner_pid}"));
        let namespace = Self {
            root,
            registry_path: runtime_root.join("ports").join("claims.json"),
            manifest: NamespaceManifest {
                version: MANIFEST_VERSION,
                id,
                name,
                mode,
                owner_pid,
                owner_start,
                assignments: BTreeMap::new(),
                processes: Vec::new(),
            },
            cleanup: mode == NamespaceMode::Ephemeral,
            released: false,
        };
        namespace.save_manifest()?;
        Ok(namespace)
    }

    fn reopen_persistent(
        root: PathBuf,
        runtime_root: &Path,
        id: &str,
    ) -> Result<Self, RuntimeNamespaceError> {
        let manifest: NamespaceManifest = serde_json::from_slice(&fs::read(root.join("namespace.json"))?)?;
        if manifest.version != MANIFEST_VERSION {
            return Err(RuntimeNamespaceError::UnsupportedVersion(manifest.version));
        }
        if manifest.name != id || manifest.mode != NamespaceMode::Persistent {
            return Err(RuntimeNamespaceError::IdentityMismatch {
                expected: id.to_string(),
                actual: manifest.name,
            });
        }
        for child in ["data", "config", "log", "artifacts", "services"] {
            fs::create_dir_all(root.join(child))?;
        }
        fs::create_dir_all(runtime_root.join("ports"))?;
        Ok(Self {
            root,
            registry_path: runtime_root.join("ports").join("claims.json"),
            manifest,
            cleanup: false,
            released: false,
        })
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.manifest.id
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn data_dir(&self) -> PathBuf {
        self.root.join("data")
    }

    #[must_use]
    pub fn config_dir(&self) -> PathBuf {
        self.root.join("config")
    }

    #[must_use]
    pub fn log_dir(&self) -> PathBuf {
        self.root.join("log")
    }

    #[must_use]
    pub fn artifacts_dir(&self) -> PathBuf {
        self.root.join("artifacts")
    }

    /// Return a stable directory for a logical service identity.
    ///
    /// # Errors
    /// Returns an error when one of the service directories cannot be created.
    pub fn service_dir(&self, service: &str, identity: &str) -> Result<PathBuf, RuntimeNamespaceError> {
        let root = self
            .root
            .join("services")
            .join(sanitize(service))
            .join(sanitize(identity));
        for child in ["data", "config", "log", "artifacts"] {
            fs::create_dir_all(root.join(child))?;
        }
        Ok(root)
    }

    /// Return the stable assignment for a logical listener, allocating it once.
    ///
    /// # Errors
    /// Returns an error when registry access fails, the registry is malformed,
    /// or the service range has no available port.
    pub fn assign_port(&mut self, service: ServicePort, instance: u16) -> Result<u16, RuntimeNamespaceError> {
        self.assign(service, assignment_key(service, &instance.to_string()), instance)
    }

    /// Return the stable assignment for an arbitrary logical service identity.
    ///
    /// # Errors
    /// Returns an error when registry access fails, the registry is malformed,
    /// or the service range has no available port.
    pub fn assign_named_port(
        &mut self,
        service: ServicePort,
        identity: &str,
    ) -> Result<u16, RuntimeNamespaceError> {
        self.assign(service, assignment_key(service, identity), 0)
    }

    fn assign(
        &mut self,
        service: ServicePort,
        key: String,
        start_instance: u16,
    ) -> Result<u16, RuntimeNamespaceError> {
        let mut registry = lock_registry(&self.registry_path)?;
        let mut claims = read_claims(&mut registry)?;
        claims.retain(claim_is_live);
        if let Some(port) = self.manifest.assignments.get(&key).copied() {
            self.reclaim_assignment(&mut registry, &mut claims, port)?;
            return Ok(port);
        }
        let claimed = claims.iter().map(|claim| claim.port).collect::<HashSet<_>>();
        let port = (start_instance..service.range_size())
            .map(|candidate| service.port(candidate))
            .find(|port| !claimed.contains(port) && port_is_free(*port))
            .ok_or_else(|| RuntimeNamespaceError::Exhausted {
                service,
                identity: key.clone(),
            })?;
        claims.push(PortClaim {
            port,
            namespace_id: self.manifest.id.clone(),
            mode: self.manifest.mode,
            owner_pid: self.manifest.owner_pid,
            owner_start: self.manifest.owner_start.clone(),
        });
        write_claims(&mut registry, &claims)?;
        self.manifest.assignments.insert(key, port);
        self.save_manifest()?;
        Ok(port)
    }

    fn reclaim_assignment(
        &self,
        registry: &mut File,
        claims: &mut Vec<PortClaim>,
        port: u16,
    ) -> Result<(), RuntimeNamespaceError> {
        if let Some(claim) = claims.iter().find(|claim| claim.port == port) {
            if claim.namespace_id == self.manifest.id {
                write_claims(registry, claims)?;
                return Ok(());
            }
            return Err(RuntimeNamespaceError::PortConflict {
                port,
                owner: claim.namespace_id.clone(),
            });
        }
        if !port_is_free(port) {
            return Err(RuntimeNamespaceError::PortOccupied { port });
        }
        claims.push(self.port_claim(port));
        write_claims(registry, claims)
    }

    fn port_claim(&self, port: u16) -> PortClaim {
        PortClaim {
            port,
            namespace_id: self.manifest.id.clone(),
            mode: self.manifest.mode,
            owner_pid: self.manifest.owner_pid,
            owner_start: self.manifest.owner_start.clone(),
        }
    }

    /// Preserve an ephemeral namespace after normal completion.
    pub fn preserve(&mut self) {
        self.cleanup = false;
    }

    /// Record a child process owned by this namespace for targeted cleanup.
    ///
    /// # Errors
    /// Returns an error when the process identity cannot be observed or the
    /// manifest cannot be persisted.
    pub fn record_process(&mut self, pid: u32) -> Result<(), RuntimeNamespaceError> {
        let start = process_start(pid).ok_or_else(|| {
            RuntimeNamespaceError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("process {pid} is not running"),
            ))
        })?;
        self.manifest.processes.retain(|process| process.pid != pid);
        self.manifest.processes.push(ProcessOwner { pid, start });
        self.save_manifest()
    }

    /// Release every port claim and remove this namespace tree.
    ///
    /// This is the explicit destructive lifecycle operation for persistent
    /// namespaces. Dropping or stopping a persistent namespace preserves it.
    ///
    /// # Errors
    /// Returns an error when claims or files cannot be removed.
    pub fn delete(mut self) -> Result<(), RuntimeNamespaceError> {
        self.release_claims()?;
        if self.root.exists() {
            fs::remove_dir_all(&self.root)?;
        }
        self.released = true;
        Ok(())
    }

    fn save_manifest(&self) -> Result<(), RuntimeNamespaceError> {
        let path = self.root.join("namespace.json");
        let temporary = self.root.join("namespace.json.new");
        let bytes = serde_json::to_vec_pretty(&self.manifest)?;
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(temporary, path)?;
        Ok(())
    }

    fn release_claims(&self) -> Result<(), RuntimeNamespaceError> {
        if !self.registry_path.exists() {
            return Ok(());
        }
        let mut registry = lock_registry(&self.registry_path)?;
        let mut claims = read_claims(&mut registry)?;
        claims.retain(|claim| claim.namespace_id != self.manifest.id);
        write_claims(&mut registry, &claims)
    }
}

impl Drop for RuntimeNamespace {
    fn drop(&mut self) {
        if !self.released && self.manifest.mode == NamespaceMode::Ephemeral {
            let _ = self.release_claims();
            if self.cleanup && !std::thread::panicking() {
                let _ = fs::remove_dir_all(&self.root);
            }
        }
    }
}

fn assignment_key(service: ServicePort, identity: &str) -> String {
    format!("{}:{}", service.name(), sanitize(identity))
}

fn sanitize(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "runtime".to_string()
    } else {
        sanitized
    }
}

fn lock_registry(path: &Path) -> Result<File, RuntimeNamespaceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    file.lock_exclusive()?;
    Ok(file)
}

fn read_claims(file: &mut File) -> Result<Vec<PortClaim>, RuntimeNamespaceError> {
    let mut bytes = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut bytes)?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn write_claims(file: &mut File, claims: &[PortClaim]) -> Result<(), RuntimeNamespaceError> {
    let bytes = serde_json::to_vec_pretty(claims)?;
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn claim_is_live(claim: &PortClaim) -> bool {
    claim.mode == NamespaceMode::Persistent
        || process_start(claim.owner_pid).is_some_and(|start| start == claim.owner_start)
}

fn port_is_free(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Resolve the single workspace-local root for generated runtime state.
///
/// `CROWDB_RUNTIME_ROOT` is used when a runner passes the root explicitly;
/// otherwise the repository is found by walking up to `pixi.toml`.
#[must_use]
pub fn runtime_root() -> PathBuf {
    if let Some(root) = std::env::var_os("CROWDB_RUNTIME_ROOT") {
        return PathBuf::from(root);
    }
    let mut root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    loop {
        if root.join("pixi.toml").is_file() {
            return root.join(".crowdb-runtime");
        }
        if !root.pop() {
            return PathBuf::from(".crowdb-runtime");
        }
    }
}

/// Allocate compatibility ports owned by the current process in the global
/// namespace registry.
///
/// This keeps older test callers coordinated with explicit
/// [`RuntimeNamespace`] environments during migration.
///
/// # Errors
/// Returns an error when the registry cannot be read or written, or no
/// consecutive range is available.
pub fn assign_process_ports(
    service: ServicePort,
    start_instance: u16,
    count: u16,
) -> Result<Vec<u16>, RuntimeNamespaceError> {
    assign_owned_process_ports(service, start_instance, count, std::process::id())
}

/// Allocate compatibility ports owned by a caller-supplied live process.
///
/// This supports short-lived allocator commands on behalf of a longer-lived
/// E2E runner. Claims are reclaimed only after that runner exits.
///
/// # Errors
/// Returns an error when the owner is not alive, the registry cannot be read
/// or written, or no consecutive range is available.
pub fn assign_owned_process_ports(
    service: ServicePort,
    start_instance: u16,
    count: u16,
    pid: u32,
) -> Result<Vec<u16>, RuntimeNamespaceError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let start = process_start(pid).ok_or_else(|| {
        RuntimeNamespaceError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("port claim owner process {pid} is not running"),
        ))
    })?;
    let owner = format!("legacy-process-{pid}-{start}");
    let registry_path = runtime_root().join("ports").join("claims.json");
    let mut registry = lock_registry(&registry_path)?;
    let mut claims = read_claims(&mut registry)?;
    claims.retain(claim_is_live);
    let claimed = claims.iter().map(|claim| claim.port).collect::<HashSet<_>>();
    let last_start = service.range_size().saturating_sub(count);
    let ports = (start_instance..=last_start)
        .map(|candidate| {
            (0..count)
                .map(|offset| service.port(candidate + offset))
                .collect::<Vec<_>>()
        })
        .find(|ports| {
            ports
                .iter()
                .all(|port| !claimed.contains(port) && port_is_free(*port))
        })
        .ok_or_else(|| RuntimeNamespaceError::Exhausted {
            service,
            identity: owner.clone(),
        })?;
    claims.extend(ports.iter().map(|port| PortClaim {
        port: *port,
        namespace_id: owner.clone(),
        mode: NamespaceMode::Ephemeral,
        owner_pid: pid,
        owner_start: start.clone(),
    }));
    write_claims(&mut registry, &claims)?;
    Ok(ports)
}

/// Release compatibility claims owned by the current process.
///
/// # Errors
/// Returns an error when the registry cannot be read or written.
pub fn release_process_ports() -> Result<(), RuntimeNamespaceError> {
    let registry_path = runtime_root().join("ports").join("claims.json");
    if !registry_path.exists() {
        return Ok(());
    }
    let pid = std::process::id();
    let start = process_start(pid).unwrap_or_else(|| format!("process-{pid}"));
    let owner = format!("legacy-process-{pid}-{start}");
    let mut registry = lock_registry(&registry_path)?;
    let mut claims = read_claims(&mut registry)?;
    claims.retain(|claim| claim.namespace_id != owner);
    write_claims(&mut registry, &claims)
}

#[cfg(target_os = "linux")]
fn process_start(pid: u32) -> Option<String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = stat.rsplit_once(") ")?.1.split_whitespace().collect::<Vec<_>>();
    fields.get(19).map(|value| (*value).to_string())
}

#[cfg(not(target_os = "linux"))]
fn process_start(pid: u32) -> Option<String> {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .ok()
        .filter(std::process::ExitStatus::success)
        .map(|_| format!("process-{pid}"))
}
