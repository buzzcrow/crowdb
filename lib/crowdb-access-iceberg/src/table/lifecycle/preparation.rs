use super::{TableLifecycleAction, TableLifecycleRequest};
use crate::{catalog::CatalogError, error::ValidationError, key::NameSuffix};

pub(super) fn validate(request: &TableLifecycleRequest) -> Result<(), CatalogError> {
    request.context.validate()?;
    if request.principal.is_empty() || request.principal.len() > 256 || request.principal.contains('\0') {
        return Err(ValidationError::Text.into());
    }
    NameSuffix {
        parent: None,
        name: &request.name,
    }
    .encode()?;
    if let TableLifecycleAction::Rename { name, .. } = &request.action {
        NameSuffix { parent: None, name }.encode()?;
    }
    Ok(())
}
