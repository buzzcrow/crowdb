use crowdb_protocol::iceberg_fb::{
    FBMultipartLimits, FBMultipartLimitsArgs, FBMultipartPart, FBMultipartPartArgs, FBMultipartPartMutation,
    FBMultipartPartMutationArgs, FBMultipartSession, FBMultipartSessionArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::catalog::CatalogContext;
use crate::error::ValidationError;
use crate::file::{MultipartLimits, MultipartPart, MultipartPartMutation, MultipartPhase, MultipartSession};
use crate::key::{FileId, OperationId};

mod completion;
mod fields;

pub(super) fn encode_session<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    session: &MultipartSession,
) -> Result<WIPOffset<FBMultipartSession<'buffer>>, ValidationError> {
    session.validate()?;
    let upload = builder.create_vector(session.upload.as_bytes());
    let owner = fields::encode_owner(builder, session.owner);
    let location = builder.create_string(&session.location.to_string());
    let principal = builder.create_vector(&session.principal);
    let limits = FBMultipartLimits::create(
        builder,
        &FBMultipartLimitsArgs {
            max_parts: session.limits.max_parts,
            max_part_bytes: session.limits.max_part_bytes,
            max_file_bytes: session.limits.max_file_bytes,
            max_staged_bytes: session.limits.max_staged_bytes,
            ttl_ms: session.limits.ttl_ms,
        },
    );
    let completion = session
        .completion
        .as_ref()
        .map(|value| completion::encode(builder, value))
        .transpose()?;
    let published = session
        .published
        .map(|file| builder.create_vector(file.as_bytes()));
    let pending = session
        .pending
        .as_ref()
        .map(|pending| {
            let before = pending
                .before
                .as_ref()
                .map(|part| encode_part(builder, part))
                .transpose()?;
            let after = encode_part(builder, &pending.after)?;
            Ok::<_, ValidationError>(FBMultipartPartMutation::create(
                builder,
                &FBMultipartPartMutationArgs {
                    before,
                    after: Some(after),
                },
            ))
        })
        .transpose()?;
    Ok(FBMultipartSession::create(
        builder,
        &FBMultipartSessionArgs {
            activation_epoch: session.context.activation_epoch,
            upload: Some(upload),
            owner: Some(owner),
            location: Some(location),
            principal: Some(principal),
            revision: session.revision,
            created_ms: session.created_ms,
            expires_ms: session.expires_ms,
            limits: Some(limits),
            phase: match session.phase {
                MultipartPhase::Open => 0,
                MultipartPhase::Completing => 1,
                MultipartPhase::Publishing => 2,
                MultipartPhase::Published => 3,
                MultipartPhase::Aborted => 4,
                MultipartPhase::Conflicted => 5,
            },
            part_count: session.part_count,
            staged_bytes: session.staged_bytes,
            completion,
            published,
            pending,
        },
    ))
}

pub(super) fn decode_session(value: FBMultipartSession<'_>) -> Result<MultipartSession, ValidationError> {
    let owner = fields::decode_owner(value.owner())?;
    let limits = value.limits();
    let session = MultipartSession {
        context: CatalogContext {
            catalog: owner.table.catalog,
            activation_epoch: value.activation_epoch(),
        },
        upload: OperationId::from_bytes(value.upload().bytes())?,
        owner,
        location: value.location().parse()?,
        principal: value
            .principal()
            .bytes()
            .try_into()
            .map_err(|_| ValidationError::Record)?,
        revision: value.revision(),
        created_ms: value.created_ms(),
        expires_ms: value.expires_ms(),
        limits: MultipartLimits {
            max_parts: limits.max_parts(),
            max_part_bytes: limits.max_part_bytes(),
            max_file_bytes: limits.max_file_bytes(),
            max_staged_bytes: limits.max_staged_bytes(),
            ttl_ms: limits.ttl_ms(),
        },
        phase: match value.phase() {
            0 => MultipartPhase::Open,
            1 => MultipartPhase::Completing,
            2 => MultipartPhase::Publishing,
            3 => MultipartPhase::Published,
            4 => MultipartPhase::Aborted,
            5 => MultipartPhase::Conflicted,
            _ => return Err(ValidationError::Record),
        },
        part_count: value.part_count(),
        staged_bytes: value.staged_bytes(),
        completion: value.completion().map(completion::decode).transpose()?,
        published: value
            .published()
            .map(|bytes| FileId::from_bytes(bytes.bytes()))
            .transpose()?,
        pending: value
            .pending()
            .map(|pending| {
                Ok::<_, ValidationError>(MultipartPartMutation {
                    before: pending.before().map(decode_part).transpose()?,
                    after: decode_part(pending.after())?,
                })
            })
            .transpose()?,
    };
    session.validate()?;
    Ok(session)
}

pub(super) fn encode_part<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    part: &MultipartPart,
) -> Result<WIPOffset<FBMultipartPart<'buffer>>, ValidationError> {
    part.validate()?;
    let upload = builder.create_vector(part.upload.as_bytes());
    let owner = fields::encode_owner(builder, part.owner);
    let tree = fields::encode_tree(builder, &part.tree);
    Ok(FBMultipartPart::create(
        builder,
        &FBMultipartPartArgs {
            upload: Some(upload),
            number: part.number,
            revision: part.revision,
            owner: Some(owner),
            tree: Some(tree),
        },
    ))
}

pub(super) fn decode_part(value: FBMultipartPart<'_>) -> Result<MultipartPart, ValidationError> {
    let part = MultipartPart {
        upload: OperationId::from_bytes(value.upload().bytes())?,
        number: value.number(),
        revision: value.revision(),
        owner: fields::decode_owner(value.owner())?,
        tree: fields::decode_tree(value.tree())?,
    };
    part.validate()?;
    Ok(part)
}
