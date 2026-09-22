use crate::error::ValidationError;
use crate::key::OperationId;
use crate::operation::RequestIdentity;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestKey {
    Client(RequestIdentity),
    Internal(RequestIdentity),
}

impl RequestKey {
    /// # Errors
    /// Rejects malformed or future client keys; absent keys create fresh recovery identities.
    pub fn parse(header: Option<&str>, now_ms: u64) -> Result<Self, ValidationError> {
        header.map_or_else(
            || {
                Ok(Self::Internal(RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now_ms,
                }))
            },
            |value| RequestIdentity::parse(value, now_ms).map(Self::Client),
        )
    }

    #[must_use]
    pub const fn identity(self) -> RequestIdentity {
        match self {
            Self::Client(identity) | Self::Internal(identity) => identity,
        }
    }
}
