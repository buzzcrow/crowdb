use crowdb_protocol::iceberg_fb::{
    FBGcCandidate, FBGcCandidateArgs, FBGcEntry, FBGcEntryArgs, FBGcFrame, FBGcFrameArgs, FBGcPage,
    FBGcPageArgs, FBGcPin, FBGcPinArgs, FBGcTask, FBGcTaskArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::{
    catalog::CatalogContext,
    error::ValidationError,
    file::FileIdentity,
    gc::{
        CandidatePhase, GcCandidate, GcPage, GcPhase, GcPin, GcStalledReason, GcTask, GcTaskKind,
        ReclaimFrame, TreeReclaimCursor,
    },
    key::{CatalogId, OperationId},
};

pub(super) fn encode_task<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    task: &GcTask,
) -> Result<WIPOffset<FBGcTask<'buffer>>, ValidationError> {
    task.validate()?;
    let catalog = builder.create_vector(task.context.catalog.as_bytes());
    let identity = builder.create_vector(task.identity.as_bytes());
    let head = task
        .head
        .as_ref()
        .map(|head| super::table::encode_head(builder, head))
        .transpose()?;
    let scan_after = builder.create_vector(&task.scan_after);
    let mark_root = task
        .proof
        .root
        .as_ref()
        .map(|value| super::payload::encode_reference(builder, value))
        .transpose()?;
    let mark_pending = task
        .proof
        .pending
        .as_ref()
        .map(|value| super::payload::encode_reference(builder, value))
        .transpose()?;
    Ok(FBGcTask::create(
        builder,
        &FBGcTaskArgs {
            discovery_scope: task.discovery_scope,
            mark_root,
            mark_pending,
            proof_complete: task.proof.complete,
            sweep_round: task.sweep_round,
            deferred_ranges: task.deferred_ranges,
            catalog: Some(catalog),
            activation_epoch: task.context.activation_epoch,
            identity: Some(identity),
            kind: task.kind as u8,
            phase: task.phase as u8,
            revision: task.revision,
            created_ms: task.created_ms,
            not_before_ms: task.not_before_ms,
            retry_at_ms: task.retry_at_ms,
            attempts: task.attempts,
            paused: task.paused,
            fenced: task.fenced,
            stalled: task.stalled as u8,
            head,
            scan_after: Some(scan_after),
            queue_read: task.queue_read,
            queue_write: task.queue_write,
            marked: task.marked,
            deleted: task.deleted,
            reclaimed_bytes: task.reclaimed_bytes,
            quarantined_from: task.quarantined_from.map_or(255, |phase| phase as u8),
        },
    ))
}

pub(super) fn decode_task(value: FBGcTask<'_>) -> Result<GcTask, ValidationError> {
    let task = GcTask {
        discovery_scope: value.discovery_scope(),
        proof: crate::gc::GcProofState {
            root: value
                .mark_root()
                .map(super::payload::decode_reference)
                .transpose()?,
            pending: value
                .mark_pending()
                .map(super::payload::decode_reference)
                .transpose()?,
            complete: value.proof_complete(),
        },
        sweep_round: value.sweep_round(),
        deferred_ranges: value.deferred_ranges(),
        context: CatalogContext {
            catalog: CatalogId::from_bytes(value.catalog().bytes())?,
            activation_epoch: value.activation_epoch(),
        },
        identity: OperationId::from_bytes(value.identity().bytes())?,
        kind: match value.kind() {
            0 => GcTaskKind::RetiredCatalog,
            1 => GcTaskKind::PurgeTable,
            2 => GcTaskKind::LiveTable,
            _ => return Err(ValidationError::Record),
        },
        phase: decode_phase(value.phase())?,
        quarantined_from: (value.quarantined_from() != 255)
            .then(|| decode_phase(value.quarantined_from()))
            .transpose()?,
        revision: value.revision(),
        created_ms: value.created_ms(),
        not_before_ms: value.not_before_ms(),
        retry_at_ms: value.retry_at_ms(),
        attempts: value.attempts(),
        paused: value.paused(),
        fenced: value.fenced(),
        stalled: match value.stalled() {
            0 => GcStalledReason::None,
            1 => GcStalledReason::Retention,
            2 => GcStalledReason::Protected,
            3 => GcStalledReason::ChangedAuthority,
            4 => GcStalledReason::Storage,
            5 => GcStalledReason::UnsupportedRange,
            6 => GcStalledReason::Corruption,
            7 => GcStalledReason::Resource,
            _ => return Err(ValidationError::Record),
        },
        head: value.head().map(super::table::decode_head).transpose()?,
        scan_after: value.scan_after().bytes().to_vec(),
        queue_read: value.queue_read(),
        queue_write: value.queue_write(),
        marked: value.marked(),
        deleted: value.deleted(),
        reclaimed_bytes: value.reclaimed_bytes(),
    };
    task.validate()?;
    Ok(task)
}

fn decode_phase(value: u8) -> Result<GcPhase, ValidationError> {
    Ok(match value {
        0 => GcPhase::Discover,
        1 => GcPhase::Roots,
        2 => GcPhase::Mark,
        3 => GcPhase::Fence,
        4 => GcPhase::Sweep,
        5 => GcPhase::Waiting,
        6 => GcPhase::Complete,
        7 => GcPhase::Quarantined,
        8 => GcPhase::Rescan,
        9 => GcPhase::CleanupSystem,
        10 => GcPhase::CleanupCatalog,
        11 => GcPhase::RootsSystem,
        12 => GcPhase::PreSweepSystem,
        13 => GcPhase::SweepWrites,
        14 => GcPhase::VerifyCleanup,
        15 => GcPhase::CleanupGc,
        16 => GcPhase::Unseal,
        _ => return Err(ValidationError::Record),
    })
}

pub(super) fn encode_candidate<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    candidate: &GcCandidate,
) -> Result<WIPOffset<FBGcCandidate<'buffer>>, ValidationError> {
    candidate.validate()?;
    let task = builder.create_vector(candidate.task.as_bytes());
    let file = super::file::encode(builder, &candidate.file)?;
    let assembly = candidate
        .assembly
        .as_ref()
        .map(|session| super::multipart::encode_session(builder, session))
        .transpose()?;
    let part = candidate
        .part
        .as_ref()
        .map(|part| super::multipart::encode_part(builder, part))
        .transpose()?;
    let frames = candidate
        .cursor
        .frames
        .iter()
        .map(|frame| {
            let root = super::file::encode_root(builder, &frame.root);
            FBGcFrame::create(
                builder,
                &FBGcFrameArgs {
                    root: Some(root),
                    length: frame.length,
                    next_child: frame.next_child,
                },
            )
        })
        .collect::<Vec<_>>();
    let frames = builder.create_vector(&frames);
    let pending = candidate
        .cursor
        .pending
        .as_ref()
        .map(|root| super::file::encode_root(builder, root));
    Ok(FBGcCandidate::create(
        builder,
        &FBGcCandidateArgs {
            completed_round: candidate.completed_round,
            task: Some(task),
            generation: candidate.generation,
            first_seen_ms: candidate.first_seen_ms,
            not_before_ms: candidate.not_before_ms,
            revision: candidate.revision,
            phase: candidate.phase as u8,
            file: Some(file),
            part,
            assembly,
            next_root: candidate.next_root,
            frames: Some(frames),
            pending,
        },
    ))
}

pub(super) fn decode_candidate(value: FBGcCandidate<'_>) -> Result<GcCandidate, ValidationError> {
    if value.frames().len() > 9 {
        return Err(ValidationError::RecordTooLarge);
    }
    let file = super::file::decode(value.file())?;
    let assembly = value
        .assembly()
        .map(super::multipart::decode_session)
        .transpose()?
        .map(Box::new);
    let owner = assembly.as_ref().map_or(
        FileIdentity {
            table: file.location.table(),
            file: file.file,
        },
        |session| session.owner,
    );
    let frames = value
        .frames()
        .iter()
        .map(|frame| {
            Ok(ReclaimFrame {
                root: super::file::decode_root(frame.root())?,
                length: frame.length(),
                next_child: frame.next_child(),
            })
        })
        .collect::<Result<Vec<_>, ValidationError>>()?;
    let candidate = GcCandidate {
        completed_round: value.completed_round(),
        task: OperationId::from_bytes(value.task().bytes())?,
        generation: value.generation(),
        first_seen_ms: value.first_seen_ms(),
        not_before_ms: value.not_before_ms(),
        revision: value.revision(),
        phase: match value.phase() {
            0 => CandidatePhase::Retained,
            1 => CandidatePhase::Deleting,
            2 => CandidatePhase::Deferred,
            3 => CandidatePhase::Complete,
            4 => CandidatePhase::Sealing,
            _ => return Err(ValidationError::Record),
        },
        file,
        part: value.part().map(super::multipart::decode_part).transpose()?,
        assembly,
        next_root: value.next_root(),
        cursor: TreeReclaimCursor {
            owner,
            frames,
            pending: value.pending().map(super::file::decode_root).transpose()?,
        },
    };
    candidate.validate()?;
    Ok(candidate)
}

pub(super) fn encode_page<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    page: &GcPage,
) -> Result<WIPOffset<FBGcPage<'buffer>>, ValidationError> {
    page.validate()?;
    let catalog = builder.create_vector(page.catalog.as_bytes());
    let task = builder.create_vector(page.task.as_bytes());
    let entries = page
        .entries
        .iter()
        .map(|key| {
            let key = builder.create_vector(key);
            FBGcEntry::create(builder, &FBGcEntryArgs { key: Some(key) })
        })
        .collect::<Vec<_>>();
    let entries = builder.create_vector(&entries);
    Ok(FBGcPage::create(
        builder,
        &FBGcPageArgs {
            catalog: Some(catalog),
            task: Some(task),
            kind: page.kind,
            sequence: page.sequence,
            entries: Some(entries),
        },
    ))
}

pub(super) fn decode_page(value: FBGcPage<'_>) -> Result<GcPage, ValidationError> {
    if value.entries().len() > 256 {
        return Err(ValidationError::RecordTooLarge);
    }
    let page = GcPage {
        catalog: CatalogId::from_bytes(value.catalog().bytes())?,
        task: OperationId::from_bytes(value.task().bytes())?,
        kind: value.kind(),
        sequence: value.sequence(),
        entries: value
            .entries()
            .iter()
            .map(|entry| entry.key().bytes().to_vec())
            .collect(),
    };
    page.validate()?;
    Ok(page)
}

pub(super) fn encode_pin<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    pin: &GcPin,
) -> Result<WIPOffset<FBGcPin<'buffer>>, ValidationError> {
    pin.validate()?;
    let catalog = builder.create_vector(pin.context.catalog.as_bytes());
    let identity = builder.create_vector(pin.identity.as_bytes());
    let head = super::table::encode_head(builder, &pin.head)?;
    let principal = builder.create_string(&pin.principal);
    Ok(FBGcPin::create(
        builder,
        &FBGcPinArgs {
            catalog: Some(catalog),
            activation_epoch: pin.context.activation_epoch,
            identity: Some(identity),
            head: Some(head),
            principal: Some(principal),
            expires_ms: pin.expires_ms,
            released: pin.released,
            operator_pin: pin.operator,
            protects_uploads: pin.protects_uploads,
        },
    ))
}

pub(super) fn decode_pin(value: FBGcPin<'_>) -> Result<GcPin, ValidationError> {
    let pin = GcPin {
        context: CatalogContext {
            catalog: CatalogId::from_bytes(value.catalog().bytes())?,
            activation_epoch: value.activation_epoch(),
        },
        identity: OperationId::from_bytes(value.identity().bytes())?,
        head: super::table::decode_head(value.head())?,
        principal: value.principal().to_owned(),
        expires_ms: value.expires_ms(),
        released: value.released(),
        operator: value.operator_pin(),
        protects_uploads: value.protects_uploads(),
    };
    pin.validate()?;
    Ok(pin)
}
