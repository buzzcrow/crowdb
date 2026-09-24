use super::{advance, CommitPublicationError, Phase, Publisher};
use crate::{
    commit::{
        CommitPreparationError, CommitProofError, EvaluationError, RequirementError, TableCommitOperation,
        TableCommitOutcome,
    },
    operation::PayloadStore,
    wire::IcebergErrorResponse,
};

pub(super) fn response(error: &CommitProofError) -> Option<IcebergErrorResponse> {
    let (status, kind, message) = match error {
        CommitProofError::Preparation(CommitPreparationError::Evaluation(EvaluationError::Requirement(
            RequirementError::Failed(_),
        ))) => (409, "CommitFailedException", "Table requirement failed"),
        CommitProofError::Preparation(CommitPreparationError::Evaluation(EvaluationError::Unsupported(
            _,
        )))
        | CommitProofError::UnsupportedPartitionStatistics => (
            406,
            "UnsupportedOperationException",
            "Selected operation is not supported",
        ),
        CommitProofError::Preparation(CommitPreparationError::Evaluation(_)) => {
            (400, "BadRequestException", "Invalid table update")
        }
        CommitProofError::Files(error) if crate::commit::proof::invalid_files(error) => {
            (400, "BadRequestException", "Invalid selected table files")
        }
        _ => return None,
    };
    Some(IcebergErrorResponse::new(status, kind, message))
}

impl Publisher {
    pub(super) async fn reject_validation(
        &self,
        operation: &TableCommitOperation,
        response: IcebergErrorResponse,
    ) -> Result<TableCommitOutcome, CommitPublicationError> {
        self.current(operation).await?;
        let bytes = serde_json::to_vec(&response).map_err(|_| crate::error::ValidationError::Record)?;
        let body = PayloadStore::new(self.store.clone())
            .put(operation.context.catalog, operation.identity.operation, &bytes)
            .await?;
        let mut next = advance(operation, Phase::Rejected)?;
        next.outcome = Some(TableCommitOutcome {
            status: response.error.code,
            body,
        });
        self.change(operation, &next).await?;
        self.finish(next).await
    }
}
