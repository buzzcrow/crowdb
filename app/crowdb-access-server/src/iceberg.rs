//! Independent Iceberg listener and catalog-management runtime.

mod body;
mod connection;
mod file_admission;
mod file_auth;
mod file_body;
mod file_complete;
mod file_encoding;
mod file_http;
mod file_recovery;
mod file_request;
mod file_response;
mod file_selection;
mod file_upload;
mod gc_control;
mod gc_runtime;
mod http;
mod metrics;
mod namespace_read;
mod namespace_request;
mod namespace_write;
mod recovery;
mod routes;
mod runtime;
mod table_credentials;
mod table_limits;
mod table_read;
mod table_recovery;
mod table_write;

pub use file_admission::{FileAdmissionError, FileServiceLimits, FileTransferAdmission};
pub use file_auth::authenticate_file_request;
pub use file_body::{FileBodyError, FileReadBody, FileResponseBudget};
pub use file_complete::FileCompleteBody;
pub use file_encoding::{FileEncodingError, FileUploadBody};
pub use file_request::{FileRequest, FileRequestError, MultipartRequest};
pub use file_response::{FileResponseError, FileS3ErrorCode, MultipartResponses};
pub use file_selection::{CompletePart, CompleteRequestError, CompleteResolveError, CompleteSelection};
pub use file_upload::{FileUploadBudget, FileUploadConstraints, FileUploadError};
pub use http::{serve, IcebergHttpService};
pub use metrics::{IcebergMetricsSnapshot, MetricCounts, ICEBERG_OUTCOME_NAMES, ICEBERG_ROUTE_NAMES};
pub use runtime::{run, IcebergRuntimeConfig};

#[cfg(feature = "test-util")]
pub use connection::active_io_for_tests;
#[cfg(feature = "test-util")]
pub use gc_runtime::budget::{BudgetedGcBlocks, BudgetedGcStore, GcIoBudget};
