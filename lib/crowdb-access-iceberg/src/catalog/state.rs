use crate::error::ValidationError;
use crate::key::{CatalogId, OperationId};

use super::{Capabilities, ClearBounds};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogContext {
    pub catalog: CatalogId,
    pub activation_epoch: u64,
}

impl CatalogContext {
    /// # Errors
    /// Rejects the reserved zero epoch.
    pub fn validate(self) -> Result<(), ValidationError> {
        if self.activation_epoch == 0 {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    /// # Errors
    /// Rejects identity reuse or epoch exhaustion.
    pub fn replacement(self, catalog: CatalogId) -> Result<Self, ValidationError> {
        self.validate()?;
        if self.catalog == catalog {
            return Err(ValidationError::IdentityMismatch);
        }
        Ok(Self {
            catalog,
            activation_epoch: self
                .activation_epoch
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogLifecycle {
    Ready,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogAuthority {
    pub catalog: CatalogId,
    pub display_name: String,
    pub name_generation: u64,
    pub config_generation: u64,
    pub lifecycle: CatalogLifecycle,
    pub capabilities: Capabilities,
    pub admission_bounds: ClearBounds,
}

impl CatalogAuthority {
    /// # Errors
    /// Rejects invalid display names; no table format support is enabled initially.
    pub fn new(catalog: CatalogId, display_name: String) -> Result<Self, ValidationError> {
        let authority = Self {
            catalog,
            display_name,
            name_generation: 1,
            config_generation: 1,
            lifecycle: CatalogLifecycle::Ready,
            capabilities: Capabilities::default(),
            admission_bounds: ClearBounds::default(),
        };
        authority.validate()?;
        Ok(authority)
    }

    /// # Errors
    /// Rejects malformed names, zero generations and inconsistent capabilities.
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_display_name(&self.display_name)?;
        if self.name_generation == 0 || self.config_generation == 0 {
            return Err(ValidationError::Record);
        }
        self.admission_bounds.completion_deadline(0)?;
        self.capabilities.validate()
    }

    /// # Errors
    /// Rejects retired authorities, invalid names and generation exhaustion.
    pub fn renamed(&self, display_name: String) -> Result<Self, ValidationError> {
        self.validate()?;
        validate_display_name(&display_name)?;
        if self.lifecycle != CatalogLifecycle::Ready {
            return Err(ValidationError::Record);
        }
        let name_generation = self
            .name_generation
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        Ok(Self {
            catalog: self.catalog,
            display_name,
            name_generation,
            config_generation: self.config_generation,
            lifecycle: self.lifecycle,
            capabilities: self.capabilities,
            admission_bounds: self.admission_bounds,
        })
    }

    /// # Errors
    /// Rejects capability removal, retired authorities and generation exhaustion.
    pub fn activated(&self, capabilities: Capabilities) -> Result<Self, ValidationError> {
        self.validate()?;
        capabilities.validate()?;
        if self.lifecycle != CatalogLifecycle::Ready
            || capabilities.bits() == 0
            || self.capabilities.bits() & !capabilities.bits() != 0
            || self.capabilities == capabilities
        {
            return Err(ValidationError::Capabilities);
        }
        Ok(Self {
            capabilities,
            config_generation: self
                .config_generation
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?,
            ..self.clone()
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClearTransition {
    pub operation: OperationId,
    pub previous: CatalogContext,
    pub replacement: CatalogContext,
    pub maintenance_observed_ms: u64,
    pub complete_after_ms: u64,
    pub bounds: ClearBounds,
}

impl ClearTransition {
    /// # Errors
    /// Rejects identity reuse, epoch exhaustion and deadline overflow.
    pub fn new(
        operation: OperationId,
        previous: CatalogContext,
        replacement: CatalogId,
        maintenance_observed_ms: u64,
        bounds: ClearBounds,
    ) -> Result<Self, ValidationError> {
        Ok(Self {
            operation,
            previous,
            replacement: previous.replacement(replacement)?,
            maintenance_observed_ms,
            complete_after_ms: bounds.completion_deadline(maintenance_observed_ms)?,
            bounds,
        })
    }

    /// # Errors
    /// Rejects inconsistent persisted contexts or shortened recovery deadlines.
    pub fn validate(self) -> Result<(), ValidationError> {
        if self.previous.replacement(self.replacement.catalog)? != self.replacement
            || self.bounds.completion_deadline(self.maintenance_observed_ms)? != self.complete_after_ms
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    /// # Errors
    /// Rejects malformed persisted state or a clock preceding maintenance.
    pub fn grace_elapsed(self, now_ms: u64) -> Result<bool, ValidationError> {
        self.validate()?;
        if now_ms < self.maintenance_observed_ms {
            return Err(ValidationError::Deadline);
        }
        Ok(now_ms >= self.complete_after_ms)
    }
}

fn validate_display_name(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() || name.len() > 1024 || name.contains('\0') {
        return Err(ValidationError::Text);
    }
    Ok(())
}
