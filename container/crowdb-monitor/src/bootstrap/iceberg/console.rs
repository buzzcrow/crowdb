// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Native Console provisioning journals stable management request identities.

use super::{CatalogInspection, IcebergBootstrapError, ManagementCommand, CAPABILITIES};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};
use uuid::Uuid;

const NAME: &str = "CROWDB";
const JOURNAL: &str = "console-iceberg-bootstrap.json";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    initialize: Uuid,
    activate: Uuid,
    catalog: Option<Uuid>,
    #[serde(default)]
    activated: bool,
}

pub(super) async fn ensure(
    root: &Path,
    command: &ManagementCommand<'_>,
) -> Result<(), IcebergBootstrapError> {
    let path = root.join(JOURNAL);
    let mut journal = load(&path)?;
    let mut observed = command.inspect().await?;
    if observed.initialized && journal.is_none() {
        // Existing operator-managed policy, including disabled capabilities,
        // is authoritative. Deployment is not permission to overwrite it.
        return if observed.state.as_deref() == Some("Ready") {
            Ok(())
        } else {
            Err(IcebergBootstrapError::Conflict("existing catalog is not ready"))
        };
    }
    if journal.is_none() {
        let value = Journal {
            initialize: Uuid::now_v7(),
            activate: Uuid::now_v7(),
            catalog: None,
            activated: false,
        };
        save(&path, &value)?;
        journal = Some(value);
    }
    let mut journal = journal.expect("journal was reserved");
    if journal.activated {
        if observed.initialized
            && observed.state.as_deref() == Some("Ready")
            && observed.catalog_id == journal.catalog
        {
            return Ok(());
        }
        return Err(IcebergBootstrapError::Conflict(
            "previous Console catalog identity is absent or changed",
        ));
    }
    if !observed.initialized || observed.state.as_deref() != Some("Ready") {
        if journal.catalog.is_some() {
            return Err(IcebergBootstrapError::Conflict(
                "previous Console catalog is absent or not ready",
            ));
        }
        command
            .execute(&["initialize", &journal.initialize.to_string(), NAME])
            .await?;
        observed = command.inspect().await?;
    }
    validate_identity(&observed, &journal)?;
    if journal.catalog.is_none() {
        journal.catalog = observed.catalog_id;
        save(&path, &journal)?;
    }
    if observed.root_operation_id.as_deref() == Some(journal.initialize.simple().to_string().as_str()) {
        if observed.capability_bits.as_deref() != Some("0x0000") {
            return Err(IcebergBootstrapError::Conflict(
                "initial Console capability policy changed",
            ));
        }
        command
            .execute(&["activate", &journal.activate.to_string(), NAME, "1", CAPABILITIES])
            .await?;
        observed = command.inspect().await?;
        validate_identity(&observed, &journal)?;
    }
    if observed.root_operation_id.as_deref() != Some(journal.activate.simple().to_string().as_str())
        || observed.capability_bits.as_deref() != Some(CAPABILITIES)
    {
        return Err(IcebergBootstrapError::Conflict(
            "Console activation changed or is incomplete",
        ));
    }
    journal.activated = true;
    save(&path, &journal)
}

fn validate_identity(observed: &CatalogInspection, journal: &Journal) -> Result<(), IcebergBootstrapError> {
    let ours = [
        journal.initialize.simple().to_string(),
        journal.activate.simple().to_string(),
    ];
    if !observed.initialized
        || observed.state.as_deref() != Some("Ready")
        || observed.display_name.as_deref() != Some(NAME)
        || observed.activation_epoch != Some(1)
        || observed.catalog_id.is_none()
        || journal
            .catalog
            .is_some_and(|catalog| Some(catalog) != observed.catalog_id)
        || !ours
            .iter()
            .any(|operation| observed.root_operation_id.as_deref() == Some(operation.as_str()))
    {
        return Err(IcebergBootstrapError::Conflict(
            "Console catalog identity or management operation changed",
        ));
    }
    Ok(())
}

fn load(path: &Path) -> Result<Option<Journal>, IcebergBootstrapError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut body = Vec::new();
    file.take(4097).read_to_end(&mut body)?;
    if body.len() > 4096 {
        return Err(IcebergBootstrapError::Conflict(
            "Console bootstrap journal is oversized",
        ));
    }
    let journal: Journal = serde_json::from_slice(&body)
        .map_err(|_| IcebergBootstrapError::Conflict("Console bootstrap journal is invalid"))?;
    if journal.initialize.get_version_num() != 7 || journal.activate.get_version_num() != 7 {
        return Err(IcebergBootstrapError::Conflict(
            "Console bootstrap request identity is invalid",
        ));
    }
    Ok(Some(journal))
}

fn save(path: &Path, journal: &Journal) -> Result<(), IcebergBootstrapError> {
    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let bytes = serde_json::to_vec(journal)
        .map_err(|_| IcebergBootstrapError::Conflict("Cannot encode Console journal"))?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(path.parent().expect("journal has runtime parent"))?.sync_all()?;
    Ok(())
}
