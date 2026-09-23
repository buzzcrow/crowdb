use crowdb_protocol::iceberg_fb::{FBMultipartCredit, FBMultipartCreditArgs};
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::error::ValidationError;
use crate::file::MultipartCredit;
use crate::key::OperationId;

pub(super) fn encode<'buffer>(
    builder: &mut FlatBufferBuilder<'buffer>,
    credit: MultipartCredit,
) -> WIPOffset<FBMultipartCredit<'buffer>> {
    let policy = builder.create_vector(credit.policy.as_bytes());
    FBMultipartCredit::create(
        builder,
        &FBMultipartCreditArgs {
            policy: Some(policy),
            sequence: credit.sequence,
            released: credit.released,
        },
    )
}

pub(super) fn decode(value: FBMultipartCredit<'_>) -> Result<MultipartCredit, ValidationError> {
    Ok(MultipartCredit {
        policy: OperationId::from_bytes(value.policy().bytes())?,
        sequence: value.sequence(),
        released: value.released(),
    })
}
