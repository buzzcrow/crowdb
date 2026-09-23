use crowdb_protocol::iceberg_fb::{
    FBMultipartCompletion, FBMultipartCompletionArgs, FBPartFingerprint, FBPartFingerprintArgs,
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use super::fields::{decode_owner, decode_tree, encode_owner, encode_tree};
use crate::error::ValidationError;
use crate::file::{AssemblyProgress, FileWriterCheckpoint, MultipartCompletion, PartFingerprint};
use crate::record::{
    file::{decode_root, encode_root},
    payload::{decode_reference, encode_reference},
};

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    value: &MultipartCompletion,
) -> Result<WIPOffset<FBMultipartCompletion<'buffer>>, ValidationError> {
    let selection = encode_reference(builder, &value.selection)?;
    let progress = &value.progress;
    let writer = progress
        .writer
        .as_ref()
        .map(|writer| encode_root(builder, &writer.root));
    let active = progress.active.as_ref().map(|part| {
        let owner = encode_owner(builder, part.owner);
        let digest = builder.create_vector(&part.digest);
        FBPartFingerprint::create(
            builder,
            &FBPartFingerprintArgs {
                owner: Some(owner),
                length: part.length,
                digest: Some(digest),
            },
        )
    });
    let part_digest = progress
        .part_digest
        .as_ref()
        .map(|bytes| builder.create_vector(bytes));
    let candidate = value.candidate.as_ref().map(|tree| encode_tree(builder, tree));
    Ok(FBMultipartCompletion::create(
        builder,
        &FBMultipartCompletionArgs {
            selection: Some(selection),
            selected_parts: value.selected_parts,
            next_part: progress.next_part,
            part_offset: progress.part_offset,
            completed_bytes: progress.completed_bytes,
            writer,
            active,
            part_digest,
            candidate,
        },
    ))
}

pub(super) fn decode(value: FBMultipartCompletion<'_>) -> Result<MultipartCompletion, ValidationError> {
    let selection = decode_reference(value.selection())?;
    let part_digest = value
        .part_digest()
        .map(|bytes| {
            if bytes.len() != 189 {
                return Err(ValidationError::Record);
            }
            Ok(bytes.bytes().to_vec())
        })
        .transpose()?;
    let progress = AssemblyProgress {
        selection: selection.digest,
        next_part: value.next_part(),
        part_offset: value.part_offset(),
        completed_bytes: value.completed_bytes(),
        writer: value
            .writer()
            .map(|root| decode_root(root).map(|root| FileWriterCheckpoint { root }))
            .transpose()?,
        active: value
            .active()
            .map(|part| {
                Ok::<_, ValidationError>(PartFingerprint {
                    owner: decode_owner(part.owner())?,
                    length: part.length(),
                    digest: part
                        .digest()
                        .bytes()
                        .try_into()
                        .map_err(|_| ValidationError::Record)?,
                })
            })
            .transpose()?,
        part_digest,
    };
    Ok(MultipartCompletion {
        selection,
        selected_parts: value.selected_parts(),
        progress,
        candidate: value.candidate().map(decode_tree).transpose()?,
    })
}
