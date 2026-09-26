use serde_json::Value;

use crate::{
    file::FileLocation,
    table::{TableMetadataError, TableMetadataLimits},
};

use super::{ReachableFile, ReachableKind};

/// Extracts standard metadata roots from already digest-verified canonical JSON.
/// # Errors
/// Rejects malformed root collections, invalid format versions and foreign locations.
pub fn metadata_links(
    bytes: &[u8],
    owner: &FileLocation,
    limits: TableMetadataLimits,
) -> Result<Vec<ReachableFile>, TableMetadataError> {
    let root = crate::table::decode_bounded_json(bytes, limits)?;
    if !matches!(root.get("format-version").and_then(Value::as_u64), Some(1..=3)) {
        return Err(TableMetadataError::Field("format-version"));
    }
    let mut links = Vec::new();
    for entry in array(&root, "metadata-log")? {
        add(
            &mut links,
            entry.get("metadata-file"),
            ReachableKind::File,
            owner,
            limits,
        )?;
    }
    for snapshot in array(&root, "snapshots")? {
        if let Some(location) = snapshot.get("manifest-list") {
            add(
                &mut links,
                Some(location),
                ReachableKind::ManifestList,
                owner,
                limits,
            )?;
        } else {
            let manifests = snapshot
                .get("manifests")
                .and_then(Value::as_array)
                .ok_or(TableMetadataError::Field("manifests"))?;
            if root["format-version"].as_u64() != Some(1) {
                return Err(TableMetadataError::Field("manifest-list"));
            }
            for location in manifests {
                add(&mut links, Some(location), ReachableKind::Manifest, owner, limits)?;
            }
        }
    }
    for (field, path) in [
        ("statistics", "statistics-path"),
        ("partition-statistics", "statistics-path"),
    ] {
        for entry in array(&root, field)? {
            add(&mut links, entry.get(path), ReachableKind::File, owner, limits)?;
        }
    }
    Ok(links)
}

fn array<'value>(root: &'value Value, field: &'static str) -> Result<&'value [Value], TableMetadataError> {
    match root.get(field) {
        None => Ok(&[]),
        Some(Value::Array(values)) => Ok(values),
        _ => Err(TableMetadataError::Field(field)),
    }
}

fn add(
    links: &mut Vec<ReachableFile>,
    value: Option<&Value>,
    kind: ReachableKind,
    owner: &FileLocation,
    limits: TableMetadataLimits,
) -> Result<(), TableMetadataError> {
    if links.len() >= limits.collection_entries {
        return Err(TableMetadataError::Bounds);
    }
    let location: FileLocation = value
        .and_then(Value::as_str)
        .ok_or(TableMetadataError::Field("file location"))?
        .parse()
        .map_err(|_| TableMetadataError::Binding)?;
    if location.table() != owner.table() {
        return Err(TableMetadataError::Binding);
    }
    links.push(ReachableFile { location, kind });
    Ok(())
}
