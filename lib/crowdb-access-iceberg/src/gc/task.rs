use crate::{
    catalog::CatalogContext,
    error::ValidationError,
    key::{CatalogScope, IcebergKey, OperationId},
    table::TableHead,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum GcTaskKind {
    RetiredCatalog,
    PurgeTable,
    LiveTable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum GcPhase {
    Discover,
    Roots,
    Mark,
    Fence,
    Sweep,
    Waiting,
    Complete,
    Quarantined,
    Rescan,
    CleanupSystem,
    CleanupCatalog,
    RootsSystem,
    PreSweepSystem,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum GcStalledReason {
    None,
    Retention,
    Protected,
    ChangedAuthority,
    Storage,
    UnsupportedRange,
    Corruption,
    Resource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcTask {
    pub proof: super::GcProofState,
    pub sweep_round: u64,
    pub deferred_ranges: bool,
    pub context: CatalogContext,
    pub identity: OperationId,
    pub kind: GcTaskKind,
    pub phase: GcPhase,
    pub revision: u64,
    pub created_ms: u64,
    pub not_before_ms: u64,
    pub retry_at_ms: u64,
    pub attempts: u32,
    pub paused: bool,
    pub fenced: bool,
    pub stalled: GcStalledReason,
    pub head: Option<TableHead>,
    pub scan_after: Vec<u8>,
    pub queue_read: u64,
    pub queue_write: u64,
    pub marked: u64,
    pub deleted: u64,
    /// Logical bytes of completed file records, not freed disk allocation or parity bytes.
    pub reclaimed_bytes: u64,
}

impl GcTask {
    #[must_use]
    pub fn key(&self) -> IcebergKey {
        IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::GcTask,
            suffix: self.identity.as_bytes().to_vec(),
        }
    }

    /// # Errors
    /// Rejects unbounded continuation, inconsistent ownership and counters.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.context.validate()?;
        self.proof.validate(self)?;
        if self.revision == 0
            || self.created_ms == 0
            || self.not_before_ms < self.created_ms
            || self.scan_after.len() > crate::key::MAX_KEY_BYTES
            || self.queue_read > self.queue_write
            || (self.phase == GcPhase::Sweep && self.sweep_round == 0)
            || (matches!(
                self.phase,
                GcPhase::CleanupSystem
                    | GcPhase::CleanupCatalog
                    | GcPhase::RootsSystem
                    | GcPhase::PreSweepSystem
            ) && self.kind != GcTaskKind::RetiredCatalog)
            || ((self.kind == GcTaskKind::RetiredCatalog) != self.head.is_none())
        {
            return Err(ValidationError::Record);
        }
        if !self.scan_after.is_empty() {
            if matches!(
                self.phase,
                GcPhase::CleanupSystem | GcPhase::RootsSystem | GcPhase::PreSweepSystem
            ) {
                super::GcSystemScan::validate_cursor(&self.scan_after)?;
            } else if !IcebergKey::catalog_range(self.context.catalog).contains(&self.scan_after) {
                return Err(ValidationError::Key);
            }
        }
        if let Some(head) = &self.head {
            head.validate()?;
            if head.catalog != self.context.catalog {
                return Err(ValidationError::IdentityMismatch);
            }
            if self.kind == GcTaskKind::PurgeTable
                && head.lifecycle != crate::table::TableLifecycle::Tombstone
            {
                return Err(ValidationError::Record);
            }
            if self.kind == GcTaskKind::LiveTable && head.lifecycle != crate::table::TableLifecycle::Ready {
                return Err(ValidationError::Record);
            }
        }
        Ok(())
    }

    /// # Errors
    /// Rejects revision exhaustion.
    pub fn advance(&self) -> Result<Self, ValidationError> {
        let mut next = self.clone();
        next.revision = self
            .revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        Ok(next)
    }

    pub(super) fn progress(&self) -> Result<Self, ValidationError> {
        let mut next = self.advance()?;
        if self.stalled != GcStalledReason::UnsupportedRange {
            next.stalled = GcStalledReason::None;
            next.attempts = 0;
            next.retry_at_ms = 0;
        }
        Ok(next)
    }
}
