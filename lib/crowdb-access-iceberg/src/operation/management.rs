use sha2::{Digest, Sha256};

use crate::catalog::{Capabilities, CatalogAuthority, ClearBounds};
use crate::error::ValidationError;
use crate::key::{CatalogId, OperationId};

use super::RequestIdentity;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ManagementAction {
    Initialize = 0,
    Rename = 1,
    Clear = 2,
    Activate = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ManagementPhase {
    Prepared = 0,
    Published = 1,
    Complete = 2,
    Conflict = 3,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagementRequest {
    pub identity: RequestIdentity,
    pub principal: String,
    pub action: ManagementAction,
    pub expected_epoch: u64,
    pub display_name: String,
    pub confirmation: Option<CatalogId>,
    pub capabilities: Option<Capabilities>,
}

impl ManagementRequest {
    /// # Errors
    /// Rejects oversized principals, invalid names, or invalid action parameters.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.principal.is_empty() || self.principal.len() > 256 || self.principal.contains('\0') {
            return Err(ValidationError::Text);
        }
        if self.display_name.is_empty() || self.display_name.len() > 1024 || self.display_name.contains('\0')
        {
            return Err(ValidationError::Text);
        }
        CatalogAuthority::new(
            CatalogId::from_bytes(self.identity.operation.as_bytes())?,
            self.display_name.clone(),
        )?;
        if (self.action == ManagementAction::Initialize) != (self.expected_epoch == 0)
            || (self.action == ManagementAction::Clear) != self.confirmation.is_some()
            || (self.action == ManagementAction::Activate) != self.capabilities.is_some()
        {
            return Err(ValidationError::Record);
        }
        if let Some(capabilities) = self.capabilities {
            capabilities.validate()?;
            if capabilities.bits() == 0 {
                return Err(ValidationError::Capabilities);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(self.identity.operation.as_bytes());
        digest.update(self.identity.issued_ms.to_be_bytes());
        digest.update([self.action as u8]);
        digest.update(self.expected_epoch.to_be_bytes());
        for text in [&self.principal, &self.display_name] {
            digest.update((text.len() as u64).to_be_bytes());
            digest.update(text.as_bytes());
        }
        digest.update(self.confirmation.as_ref().map_or(&[0; 16], CatalogId::as_bytes));
        if let Some(capabilities) = self.capabilities {
            digest.update(capabilities.bits().to_be_bytes());
        }
        digest.finalize().into()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagementOperation {
    pub request: ManagementRequest,
    pub phase: ManagementPhase,
    pub candidate: CatalogId,
    pub original_root: Vec<u8>,
    pub original_authority: Vec<u8>,
    pub result_authority: Vec<u8>,
    pub bounds: ClearBounds,
    pub retained_until_ms: u64,
    pub publication_proof: Vec<u8>,
    pub grace_completed_ms: u64,
}

impl ManagementOperation {
    #[must_use]
    pub fn id(&self) -> OperationId {
        self.request.identity.operation
    }

    #[must_use]
    pub fn terminal(&self) -> bool {
        matches!(self.phase, ManagementPhase::Complete | ManagementPhase::Conflict)
    }

    /// # Errors
    /// Rejects invalid requests, timing and embedded record bounds.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.request.validate()?;
        self.bounds.completion_deadline(0)?;
        if self.original_root.len() > 4096
            || self.original_authority.len() > 4096
            || self.result_authority.is_empty()
            || self.result_authority.len() > 4096
            || self.publication_proof.len() > 4096
            || self.retained_until_ms <= self.request.identity.issued_ms
        {
            return Err(ValidationError::Record);
        }
        self.validate_publication()?;
        self.validate_grace()?;
        Ok(())
    }

    fn validate_grace(&self) -> Result<(), ValidationError> {
        let needs_proof = self.request.action == ManagementAction::Clear
            && matches!(self.phase, ManagementPhase::Published | ManagementPhase::Complete);
        if !needs_proof {
            return if self.publication_proof.is_empty() && self.grace_completed_ms == 0 {
                Ok(())
            } else {
                Err(ValidationError::Record)
            };
        }
        let key = crate::key::IcebergKey::System {
            scope: crate::key::SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let crate::record::StorageRecord::Active(root) =
            crate::record::StorageRecord::decode(&key, &self.publication_proof)?
        else {
            return Err(ValidationError::Record);
        };
        let crate::catalog::RootState::Published(transition) = root.state else {
            return Err(ValidationError::Record);
        };
        if root.operation != self.id()
            || root.context.catalog != self.candidate
            || transition.bounds != self.bounds
            || transition.previous.activation_epoch != self.request.expected_epoch
            || Some(transition.previous.catalog) != self.request.confirmation
            || !transition.grace_elapsed(self.grace_completed_ms)?
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    fn validate_publication(&self) -> Result<(), ValidationError> {
        use crate::key::{CatalogScope, IcebergKey, SystemScope};
        use crate::record::StorageRecord;
        let key = IcebergKey::Catalog {
            catalog: self.candidate,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let StorageRecord::Authority(result) = StorageRecord::decode(&key, &self.result_authority)? else {
            return Err(ValidationError::Record);
        };
        if result.display_name != self.request.display_name || result.admission_bounds != self.bounds {
            return Err(ValidationError::Record);
        }
        if self.original_root.is_empty() {
            if !self.original_authority.is_empty()
                || (self.request.action != ManagementAction::Initialize
                    && self.phase != ManagementPhase::Conflict)
            {
                return Err(ValidationError::Record);
            }
            return Ok(());
        }
        let key = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let StorageRecord::Active(root) = StorageRecord::decode(&key, &self.original_root)? else {
            return Err(ValidationError::Record);
        };
        let key = IcebergKey::Catalog {
            catalog: root.context.catalog,
            scope: CatalogScope::Authority,
            suffix: Vec::new(),
        };
        let StorageRecord::Authority(original) = StorageRecord::decode(&key, &self.original_authority)?
        else {
            return Err(ValidationError::Record);
        };
        if self.phase == ManagementPhase::Conflict {
            return Ok(());
        }
        if root.state != crate::catalog::RootState::Ready
            || root.context.activation_epoch != self.request.expected_epoch
        {
            return Err(ValidationError::Record);
        }
        match self.request.action {
            ManagementAction::Initialize => return Err(ValidationError::Record),
            ManagementAction::Rename if original.renamed(self.request.display_name.clone())? != result => {
                return Err(ValidationError::Record)
            }
            ManagementAction::Activate
                if original.display_name != self.request.display_name
                    || original.activated(self.request.capabilities.ok_or(ValidationError::Record)?)?
                        != result =>
            {
                return Err(ValidationError::Record)
            }
            ManagementAction::Clear
                if self.request.confirmation != Some(original.catalog)
                    || original.catalog == self.candidate
                    || original.admission_bounds.cover(self.bounds) != self.bounds =>
            {
                return Err(ValidationError::Record)
            }
            _ => {}
        }
        Ok(())
    }
}
