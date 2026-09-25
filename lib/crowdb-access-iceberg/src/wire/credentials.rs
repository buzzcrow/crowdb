use std::collections::BTreeMap;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::catalog::{CatalogAuthority, CatalogContext, CatalogLifecycle, FormatAction};
use crate::file::{
    FileCredentials, FileGrant, FileGrantError, FileGrantIssuer, FileOperation, FileOperations, TableLocation,
};
use crate::key::{OperationId, TableId};

use super::Principal;

#[derive(Serialize)]
pub struct StorageCredential {
    prefix: String,
    config: BTreeMap<&'static str, String>,
}

impl From<FileCredentials> for StorageCredential {
    fn from(credentials: FileCredentials) -> Self {
        let grant = credentials.grant();
        Self {
            prefix: TableLocation {
                catalog: grant.context.catalog,
                table: grant.table,
            }
            .to_string(),
            config: BTreeMap::from([
                ("s3.access-key-id", credentials.access_key_id().to_owned()),
                ("s3.secret-access-key", credentials.secret_access_key().to_owned()),
                ("s3.session-token", credentials.session_token().to_owned()),
                ("s3.session-token-expires-at-ms", grant.expires_ms.to_string()),
            ]),
        }
    }
}

#[derive(Serialize)]
pub struct LoadCredentialsResponse {
    #[serde(rename = "storage-credentials")]
    credentials: [StorageCredential; 1],
}

impl From<FileCredentials> for LoadCredentialsResponse {
    fn from(credentials: FileCredentials) -> Self {
        Self {
            credentials: [credentials.into()],
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FileDelegationLimits {
    pub ttl_ms: u64,
    pub max_request_bytes: u64,
    pub max_file_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct FileDelegationTarget {
    pub table: TableId,
    pub format_version: u8,
    pub staged: bool,
}

impl FileDelegationLimits {
    /// Requires a fresh Ready root/authority pair and live table or draft authorization.
    /// Refresh must reauthorize the bearer, never exchange an old file token.
    /// # Errors
    /// Rejects foreign/retired authorities and windows exceeding persisted delegation bounds.
    /// Also rejects invalid time windows and budgets, including issuer TTL violations.
    pub fn issue(
        self,
        issuer: &FileGrantIssuer,
        principal: Principal,
        context: CatalogContext,
        authority: &CatalogAuthority,
        target: FileDelegationTarget,
        now_ms: u64,
    ) -> Result<FileCredentials, FileGrantError> {
        authority.validate().map_err(|_| FileGrantError::Invalid)?;
        if authority.catalog != context.catalog || authority.lifecycle != CatalogLifecycle::Ready {
            return Err(FileGrantError::Forbidden);
        }
        if !authority
            .capabilities
            .supports(target.format_version, FormatAction::Read)
        {
            return Err(FileGrantError::Forbidden);
        }
        if self.ttl_ms > authority.admission_bounds.delegated_access_ms {
            return Err(FileGrantError::Bounds);
        }
        let expires_ms = now_ms.checked_add(self.ttl_ms).ok_or(FileGrantError::Invalid)?;
        if expires_ms > i64::MAX as u64 {
            return Err(FileGrantError::Invalid);
        }
        let action = if target.staged {
            FormatAction::Create
        } else {
            FormatAction::Write
        };
        let operations =
            if principal.namespace_write && authority.capabilities.supports(target.format_version, action) {
                FileOperations::new(&[
                    FileOperation::Head,
                    FileOperation::Get,
                    FileOperation::Put,
                    FileOperation::CreateMultipart,
                    FileOperation::UploadPart,
                    FileOperation::ListParts,
                    FileOperation::CompleteMultipart,
                    FileOperation::AbortMultipart,
                ])?
            } else {
                FileOperations::new(&[FileOperation::Head, FileOperation::Get])?
            };
        let mut digest = Sha256::new();
        digest.update(b"crowdb-iceberg-file-principal-v1");
        digest.update(principal.name.as_bytes());
        issuer.issue(FileGrant {
            context,
            table: target.table,
            principal: digest.finalize().into(),
            nonce: OperationId::random(),
            issued_ms: now_ms,
            expires_ms,
            operations,
            max_request_bytes: self.max_request_bytes,
            max_file_bytes: self.max_file_bytes,
        })
    }
}
