use crowdb_protocol::iceberg_fb::{FBCatalogAuthority, FBCatalogAuthorityArgs, FBCatalogLifecycle};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::catalog::{Capabilities, CatalogAuthority, CatalogLifecycle};
use crate::error::ValidationError;
use crate::key::CatalogId;

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    authority: &CatalogAuthority,
) -> Result<WIPOffset<FBCatalogAuthority<'buffer>>, ValidationError> {
    authority.validate()?;
    let catalog = builder.create_vector(authority.catalog.as_bytes());
    let display_name = builder.create_string(&authority.display_name);
    Ok(FBCatalogAuthority::create(
        builder,
        &FBCatalogAuthorityArgs {
            catalog: Some(catalog),
            display_name: Some(display_name),
            name_generation: authority.name_generation,
            config_generation: authority.config_generation,
            lifecycle: match authority.lifecycle {
                CatalogLifecycle::Ready => FBCatalogLifecycle::Ready,
                CatalogLifecycle::Retired => FBCatalogLifecycle::Retired,
            },
            capabilities: authority.capabilities.bits(),
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
        catalog: CatalogId::from_bytes(value.catalog().bytes())?,
        display_name: value.display_name().to_owned(),
        name_generation: value.name_generation(),
        config_generation: value.config_generation(),
        lifecycle,
        capabilities: Capabilities::from_bits(value.capabilities())?,
    };
    authority.validate()?;
    Ok(authority)
}
