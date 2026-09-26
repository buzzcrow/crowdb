use super::StorageRecord;
use crate::{error::ValidationError, key::IcebergKey};

impl StorageRecord {
    pub(super) fn validate_gc_key(&self, key: &IcebergKey) -> Option<Result<(), ValidationError>> {
        Some(match (self, key) {
            (Self::FileWriteIntent(intent), key)
                if *key == intent.key()
                    || (intent.deleting && *key == crate::file::FileWriteIntent::fence_key(intent.owner)) =>
            {
                Ok(())
            }
            (Self::GcNode(node), key) if *key == node.key() || *key == node.pending_key() => Ok(()),
            (Self::GcTask(task), key) if *key == task.key() => Ok(()),
            (Self::GcTask(task), key)
                if *key == crate::gc::GcTask::retirement_key(task.context.catalog)
                    && task.kind == crate::gc::GcTaskKind::RetiredCatalog
                    && task.phase == crate::gc::GcPhase::CleanupGc =>
            {
                Ok(())
            }
            (Self::GcCandidate(candidate), key) if *key == candidate.key() => Ok(()),
            (Self::GcCandidate(candidate), key)
                if (*key == candidate.claim_key()
                    || candidate.assembly_claim_key().as_ref() == Some(key))
                    && candidate.phase == crate::gc::CandidatePhase::Retained
                    && candidate.revision == 1 =>
            {
                Ok(())
            }
            (Self::GcPage(page), key) if *key == page.key() => Ok(()),
            (Self::GcPin(pin), key) if *key == pin.key() => Ok(()),
            _ => return None,
        })
    }
}
