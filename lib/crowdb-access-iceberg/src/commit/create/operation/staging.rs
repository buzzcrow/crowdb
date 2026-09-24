use super::{TableCreateOperation, TableCreatePhase};
use crate::{
    error::ValidationError,
    operation::{PayloadReference, RequestIdentity},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableStageBinding {
    pub identity: RequestIdentity,
    pub input: PayloadReference,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableCreateStage {
    pub created_ms: i64,
    pub expires_ms: i64,
    pub response: PayloadReference,
    pub binding: Option<TableStageBinding>,
}

impl TableCreateOperation {
    pub(super) fn validate_stage(&self) -> Result<(), ValidationError> {
        let Some(stage) = &self.stage else {
            return if self.phase == TableCreatePhase::Staged {
                Err(ValidationError::Record)
            } else {
                Ok(())
            };
        };
        self.reference(&stage.response)?;
        if stage.created_ms < 0
            || stage.expires_ms <= stage.created_ms
            || self.candidate.table.as_bytes() != self.identity.operation.as_bytes()
        {
            return Err(ValidationError::Record);
        }
        if let Some(binding) = &stage.binding {
            self.reference(&binding.input)?;
            if self.phase == TableCreatePhase::Staged
                || self.timestamp_ms < stage.created_ms
                || self.timestamp_ms >= stage.expires_ms
                || binding.identity.operation == self.identity.operation
            {
                return Err(ValidationError::Record);
            }
        } else if !matches!(self.phase, TableCreatePhase::Staged | TableCreatePhase::Aborted)
            || self.response != stage.response
            || self.timestamp_ms != stage.created_ms
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }

    pub(in crate::commit::create) fn binding_transition(
        &self,
        after: &Self,
    ) -> Result<bool, ValidationError> {
        if self.phase != TableCreatePhase::Staged || after.phase != TableCreatePhase::Prepared {
            return Ok(false);
        }
        let before_stage = self.stage.as_ref().ok_or(ValidationError::Record)?;
        let after_stage = after.stage.as_ref().ok_or(ValidationError::Record)?;
        let mut unchanged = after_stage.clone();
        unchanged.binding.clone_from(&before_stage.binding);
        let mut head = after.candidate.clone();
        head.metadata_digest = self.candidate.metadata_digest;
        head.format_version = self.candidate.format_version;
        if unchanged != *before_stage
            || before_stage.binding.is_some()
            || after_stage.binding.is_none()
            || head != self.candidate
        {
            return Err(ValidationError::Record);
        }
        Ok(true)
    }
}
