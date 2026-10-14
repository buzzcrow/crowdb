use crowdb_protocol::iceberg_fb::{FBCatalogAuthority, FBCatalogAuthorityArgs, FBCatalogLifecycle};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::catalog::{
    Capabilities, CatalogAuthority, CatalogLifecycle, CatalogLocatorKey, CatalogStorageLayout, ClearBounds,
};
use crate::error::ValidationError;
use crate::key::CatalogId;

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    authority: &CatalogAuthority,
) -> Result<WIPOffset<FBCatalogAuthority<'buffer>>, ValidationError> {
    authority.validate()?;
    let catalog = builder.create_vector(authority.catalog.as_bytes());
    let display_name = builder.create_string(&authority.display_name);
    let locator_key = authority
        .locator_key
        .map(|key| builder.create_vector(&key.bytes()));
    Ok(FBCatalogAuthority::create(
        builder,
        &FBCatalogAuthorityArgs {
            locator_key,
            storage_layout: authority.storage_layout as u8,
            catalog: Some(catalog),
            display_name: Some(display_name),
            name_generation: authority.name_generation,
            config_generation: authority.config_generation,
            lifecycle: match authority.lifecycle {
                CatalogLifecycle::Ready => FBCatalogLifecycle::Ready,
                CatalogLifecycle::Retired => FBCatalogLifecycle::Retired,
            },
            capabilities: authority.capabilities.bits(),
            request_ms: authority.admission_bounds.request_ms,
            root_lease_ms: authority.admission_bounds.root_lease_ms,
            delegated_access_ms: authority.admission_bounds.delegated_access_ms,
            clock_skew_ms: authority.admission_bounds.clock_skew_ms,
        },
    ))
}

pub(super) fn decode(value: FBCatalogAuthority<'_>) -> Result<CatalogAuthority, ValidationError> {
    let lifecycle = match value.lifecycle() {
        FBCatalogLifecycle::Ready => CatalogLifecycle::Ready,
        FBCatalogLifecycle::Retired => CatalogLifecycle::Retired,
        _ => return Err(ValidationError::Record),
    };
    if value.display_name().len() > 1024 {
        return Err(ValidationError::Text);
    }
    let authority = CatalogAuthority {
        locator_key: value
            .locator_key()
            .map(|key| CatalogLocatorKey::new(key.bytes().try_into().map_err(|_| ValidationError::Record)?))
            .transpose()?,
        storage_layout: CatalogStorageLayout::from_version(value.storage_layout())?,
        catalog: CatalogId::from_bytes(value.catalog().bytes())?,
        display_name: value.display_name().to_owned(),
        name_generation: value.name_generation(),
        config_generation: value.config_generation(),
        lifecycle,
        capabilities: Capabilities::from_bits(value.capabilities())?,
        admission_bounds: ClearBounds {
            request_ms: value.request_ms(),
            root_lease_ms: value.root_lease_ms(),
            delegated_access_ms: value.delegated_access_ms(),
            clock_skew_ms: value.clock_skew_ms(),
        },
    };
    authority.validate()?;
    Ok(authority)
}
