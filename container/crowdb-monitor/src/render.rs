use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use thiserror::Error;
use uuid::Uuid;

use crate::DeploymentProfile;

const MAX_TEMPLATE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Error)]
pub enum RenderError {
    #[error("configuration rendering failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration template is invalid: {0}")]
    Invalid(String),
}

#[derive(Debug, Eq, PartialEq)]
pub struct RenderedConfig {
    pub service_id: String,
    pub path: PathBuf,
}

/// # Errors
/// Rejects missing, oversized, symlinked, ambiguous, or unsafe templates.
pub fn render_configs(
    profile: &DeploymentProfile,
    template_root: &Path,
    run_root: &Path,
) -> Result<Vec<RenderedConfig>, RenderError> {
    let variables = variables(profile)?;
    require_directory(template_root)?;
    require_directory(run_root)?;
    let destination = run_root.join("config");
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return invalid("run/config must be a directory, not a link");
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(&destination)?,
        Err(error) => return Err(error.into()),
    }
    let mut outputs = Vec::new();
    let mut file_names = BTreeSet::new();
    for service in &profile.services {
        let Some(template) = &service.config_template else {
            continue;
        };
        let name = template
            .file_name()
            .ok_or_else(|| RenderError::Invalid("template path has no file name".into()))?;
        if !file_names.insert(name.to_os_string()) {
            return invalid("multiple services render to the same file name");
        }
        let source = template_root.join(name);
        let metadata = fs::symlink_metadata(&source)?;
        if !metadata.file_type().is_file() || metadata.len() > MAX_TEMPLATE_BYTES {
            return invalid(format!(
                "template for {} is not a bounded regular file",
                service.id
            ));
        }
        let body = fs::read_to_string(&source)?;
        let rendered = substitute(&body, &variables)?;
        toml::from_str::<toml::Value>(&rendered).map_err(|error| {
            RenderError::Invalid(format!("template for {} is not TOML: {error}", service.id))
        })?;
        let path = destination.join(name);
        atomic_write(&path, rendered.as_bytes())?;
        outputs.push(RenderedConfig {
            service_id: service.id.clone(),
            path,
        });
    }
    Ok(outputs)
}

fn variables(profile: &DeploymentProfile) -> Result<BTreeMap<String, String>, RenderError> {
    let mut values = BTreeMap::new();
    for (name, path) in [
        ("install_root", &profile.paths.install_root),
        ("bin_root", &profile.paths.bin_root),
        ("template_root", &profile.paths.template_root),
        ("data_root", &profile.paths.data_root),
        ("run_root", &profile.paths.run_root),
        ("log_root", &profile.paths.log_root),
    ] {
        values.insert(name.into(), safe_value(&path.to_string_lossy())?);
    }
    values.insert("s3_tenant".into(), safe_value(&profile.s3_tenant)?);
    values.insert("iceberg_catalog".into(), safe_value(&profile.iceberg_catalog)?);
    for (index, node) in profile.nodes.iter().enumerate() {
        values.insert(format!("node.{index}.id"), node.node_id.to_string());
        values.insert(format!("node.{index}.rack_id"), node.rack_id.to_string());
    }
    for (index, group) in profile.groups.iter().enumerate() {
        values.insert(format!("group.{index}.id"), group.group_id.to_string());
        values.insert(format!("group.{index}.store_id"), group.store_id.to_string());
    }
    for (index, disk) in profile.disks.iter().enumerate() {
        values.insert(
            format!("disk.{index}.path"),
            safe_value(&disk.path.to_string_lossy())?,
        );
        values.insert(
            format!("disk.{index}.capacity_bytes"),
            disk.capacity_bytes.to_string(),
        );
    }
    Ok(values)
}

fn safe_value(value: &str) -> Result<String, RenderError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._/".contains(&byte))
    {
        return invalid("profile value is unsafe for template substitution");
    }
    Ok(value.into())
}

fn substitute(template: &str, variables: &BTreeMap<String, String>) -> Result<String, RenderError> {
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let after_open = &rest[start + 2..];
        let end = after_open
            .find("}}")
            .ok_or_else(|| RenderError::Invalid("unclosed template variable".into()))?;
        let name = &after_open[..end];
        let value = variables
            .get(name)
            .ok_or_else(|| RenderError::Invalid(format!("unknown template variable {name}")))?;
        output.push_str(value);
        rest = &after_open[end + 2..];
    }
    if rest.contains("}}") {
        return invalid("stray template terminator");
    }
    output.push_str(rest);
    Ok(output)
}

fn require_directory(path: &Path) -> Result<(), RenderError> {
    if !fs::symlink_metadata(path)?.file_type().is_dir() {
        return invalid("render root must be a directory, not a link");
    }
    Ok(())
}

fn atomic_write(path: &Path, body: &[u8]) -> Result<(), RenderError> {
    let directory = path
        .parent()
        .ok_or_else(|| RenderError::Invalid("render path has no parent".into()))?;
    let temporary = directory.join(format!(".config-{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(body)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(RenderError::Io)
}

fn invalid<T>(message: impl Into<String>) -> Result<T, RenderError> {
    Err(RenderError::Invalid(message.into()))
}
