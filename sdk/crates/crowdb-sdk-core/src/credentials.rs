// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::Error;
/// Supplies a current credential before each request attempt. Implementations
/// must be nonblocking; refresh remote credentials outside the request path.
/// Returned credentials never appear in SDK diagnostics.
pub trait CredentialProvider: Send + Sync {
    /// # Errors
    /// Returns credential availability failures without sensitive details.
    fn bearer(&self) -> Result<Option<String>, Error>;
}
pub(crate) struct StaticCredential(pub Option<String>);
impl CredentialProvider for StaticCredential {
    fn bearer(&self) -> Result<Option<String>, Error> {
        Ok(self.0.clone())
    }
}
