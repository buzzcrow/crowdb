use crowdb_common::hash_slot::HashSlotFallback;
use crowdb_protocol::chunk_kv::{ClientRequestId, Id128};
use sha2::{Digest, Sha256};

use crate::error::ValidationError;
use crate::key::{IcebergKey, OperationId, SystemScope};

pub const RETRY_WINDOW_MS: u64 = 86_400_000;
const MAX_FUTURE_SKEW_MS: u64 = 30_000;
const LEDGER_SLOTS: u16 = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestIdentity {
    pub operation: OperationId,
    pub issued_ms: u64,
}

impl RequestIdentity {
    /// # Errors
    /// Rejects noncanonical `UUIDv7` keys and future timestamps.
    pub fn parse(key: &str, now_ms: u64) -> Result<Self, ValidationError> {
        if key.len() != 36 {
            return Err(ValidationError::Identity);
        }
        let uuid = uuid::Uuid::parse_str(key).map_err(|_| ValidationError::Identity)?;
        let bytes = uuid.as_bytes();
        if bytes[6] >> 4 != 7 || bytes[8] >> 6 != 2 {
            return Err(ValidationError::Identity);
        }
        let mut timestamp = [0; 8];
        timestamp[2..].copy_from_slice(&bytes[..6]);
        let identity = Self {
            operation: OperationId::from_bytes(bytes)?,
            issued_ms: u64::from_be_bytes(timestamp),
        };
        if identity.issued_ms > now_ms.saturating_add(MAX_FUTURE_SKEW_MS) {
            return Err(ValidationError::Deadline);
        }
        Ok(identity)
    }

    /// # Errors
    /// Rejects timestamps outside the bounded retry window.
    pub fn validate(self, now_ms: u64) -> Result<(), ValidationError> {
        if self.issued_ms > now_ms.saturating_add(MAX_FUTURE_SKEW_MS)
            || now_ms
                >= self
                    .issued_ms
                    .checked_add(RETRY_WINDOW_MS)
                    .ok_or(ValidationError::Deadline)?
        {
            return Err(ValidationError::Deadline);
        }
        Ok(())
    }
}

/// # Errors
/// Rejects a non-ledger scope or malformed internal key.
pub fn ledger_key(scope: SystemScope, operation: OperationId) -> Result<IcebergKey, ValidationError> {
    if !matches!(
        scope,
        SystemScope::ManagementOperation | SystemScope::Audit | SystemScope::RetryBinding
    ) {
        return Err(ValidationError::Key);
    }
    let slot = HashSlotFallback::new(LEDGER_SLOTS)
        .ok_or(ValidationError::Key)?
        .slot(operation.as_bytes())
        + 1;
    let mut suffix = vec![0; 16];
    suffix[14..].copy_from_slice(&slot.to_be_bytes());
    Ok(IcebergKey::System { scope, suffix })
}

pub(crate) fn overflow_key(
    scope: SystemScope,
    operation: OperationId,
) -> Result<IcebergKey, ValidationError> {
    let overflow_scope = match scope {
        SystemScope::ManagementOperation => SystemScope::ManagementOverflow,
        SystemScope::Audit => SystemScope::AuditOverflow,
        SystemScope::RetryBinding => SystemScope::RetryOverflow,
        _ => return Err(ValidationError::Key),
    };
    Ok(IcebergKey::System {
        scope: overflow_scope,
        suffix: operation.as_bytes().to_vec(),
    })
}

pub(crate) fn ledger_key_matches(scope: SystemScope, operation: OperationId, key: &IcebergKey) -> bool {
    let IcebergKey::System { scope: key_scope, .. } = key else {
        return false;
    };
    let expected = match key_scope {
        SystemScope::ManagementOperation | SystemScope::Audit | SystemScope::RetryBinding => {
            ledger_key(scope, operation)
        }
        SystemScope::ManagementOverflow | SystemScope::AuditOverflow | SystemScope::RetryOverflow => {
            overflow_key(scope, operation)
        }
        SystemScope::ActiveRoot => return false,
    };
    expected.is_ok_and(|candidate| &candidate == key)
}

#[must_use]
pub fn mutation_identity(key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> ClientRequestId {
    let mut digest = Sha256::new();
    for field in [key, expected.unwrap_or_default(), value] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    let hash: [u8; 32] = digest.finalize().into();
    let high = u64::from_be_bytes(hash[..8].try_into().unwrap_or_default());
    let low = u64::from_be_bytes(hash[8..16].try_into().unwrap_or_default());
    ClientRequestId {
        client_instance_id: Id128 { high: high | 1, low },
        client_sequence: 1,
    }
}
