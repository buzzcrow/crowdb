//! Durable management requests and HTTP retry identities.

mod identity;
mod ledger;
mod management;
mod payload;
mod result;
mod retry;

pub(crate) use identity::ledger_key_matches;
pub use identity::{ledger_key, mutation_identity, RequestIdentity, RETRY_WINDOW_MS};
pub(crate) use ledger::{ledger_locate, LedgerLocation};
pub use management::{ManagementAction, ManagementOperation, ManagementPhase, ManagementRequest};
pub use payload::{PayloadPage, PayloadReference, PayloadStore, MAX_PAYLOAD_BYTES, PAYLOAD_PAGE_BYTES};
pub use result::RetryResult;
pub use retry::{RetryAdmission, RetryLedger, RetryRecord};
