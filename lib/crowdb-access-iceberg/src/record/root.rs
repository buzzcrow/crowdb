use crowdb_protocol::iceberg_fb::{
    FBActiveCatalog, FBActiveCatalogArgs, FBClearTransition, FBClearTransitionArgs, FBRootPhase,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::catalog::{ActiveCatalogRecord, CatalogContext, ClearBounds, ClearTransition, RootState};
use crate::error::ValidationError;
use crate::key::{CatalogId, OperationId};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    root: ActiveCatalogRecord,
) -> Result<WIPOffset<FBActiveCatalog<'buffer>>, ValidationError> {
    root.validate()?;
    let catalog = builder.create_vector(root.context.catalog.as_bytes());
    let operation = builder.create_vector(root.operation.as_bytes());
    let (phase, transition) = match root.state {
        RootState::Ready => (FBRootPhase::Ready, None),
        RootState::Initializing => (FBRootPhase::Initializing, None),
        RootState::Fencing => (FBRootPhase::Fencing, None),
        RootState::Maintenance(transition) => (
            FBRootPhase::Maintenance,
            Some(encode_transition(builder, transition)),
        ),
        RootState::Published(transition) => (
            FBRootPhase::Published,
            Some(encode_transition(builder, transition)),
        ),
    };
    Ok(FBActiveCatalog::create(
        builder,
        &FBActiveCatalogArgs {
            catalog: Some(catalog),
            activation_epoch: root.context.activation_epoch,
            operation: Some(operation),
            phase,
            transition,
        },
    ))
}

pub(super) fn decode(value: FBActiveCatalog<'_>) -> Result<ActiveCatalogRecord, ValidationError> {
    let operation = OperationId::from_bytes(value.operation().bytes())?;
    let state = match (value.phase(), value.transition()) {
        (FBRootPhase::Ready, None) => RootState::Ready,
        (FBRootPhase::Initializing, None) => RootState::Initializing,
        (FBRootPhase::Fencing, None) => RootState::Fencing,
        (FBRootPhase::Maintenance, Some(transition)) => {
            RootState::Maintenance(decode_transition(transition, operation)?)
        }
        (FBRootPhase::Published, Some(transition)) => {
            RootState::Published(decode_transition(transition, operation)?)
        }
        _ => return Err(ValidationError::Record),
    };
    let root = ActiveCatalogRecord {
        context: CatalogContext {
            catalog: CatalogId::from_bytes(value.catalog().bytes())?,
            activation_epoch: value.activation_epoch(),
        },
        operation,
        state,
    };
    root.validate()?;
    Ok(root)
}

fn encode_transition<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    transition: ClearTransition,
) -> WIPOffset<FBClearTransition<'buffer>> {
    let previous_catalog = builder.create_vector(transition.previous.catalog.as_bytes());
    let replacement_catalog = builder.create_vector(transition.replacement.catalog.as_bytes());
    FBClearTransition::create(
        builder,
        &FBClearTransitionArgs {
            previous_catalog: Some(previous_catalog),
            previous_epoch: transition.previous.activation_epoch,
            replacement_catalog: Some(replacement_catalog),
            replacement_epoch: transition.replacement.activation_epoch,
            maintenance_observed_ms: transition.maintenance_observed_ms,
            complete_after_ms: transition.complete_after_ms,
            root_lease_ms: transition.bounds.root_lease_ms,
            request_ms: transition.bounds.request_ms,
            delegated_access_ms: transition.bounds.delegated_access_ms,
            clock_skew_ms: transition.bounds.clock_skew_ms,
        },
    )
}

fn decode_transition(
    value: FBClearTransition<'_>,
    operation: OperationId,
) -> Result<ClearTransition, ValidationError> {
    let transition = ClearTransition {
        operation,
        previous: CatalogContext {
            catalog: CatalogId::from_bytes(value.previous_catalog().bytes())?,
            activation_epoch: value.previous_epoch(),
        },
        replacement: CatalogContext {
            catalog: CatalogId::from_bytes(value.replacement_catalog().bytes())?,
            activation_epoch: value.replacement_epoch(),
        },
        maintenance_observed_ms: value.maintenance_observed_ms(),
        complete_after_ms: value.complete_after_ms(),
        bounds: ClearBounds {
            root_lease_ms: value.root_lease_ms(),
            request_ms: value.request_ms(),
            delegated_access_ms: value.delegated_access_ms(),
            clock_skew_ms: value.clock_skew_ms(),
        },
    };
    transition.validate()?;
    Ok(transition)
}
