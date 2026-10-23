// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::Error;
use crowdb_access_dataset::{AuthorityError, DatasetError, UploadError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureKind {
    Invalid,
    NotFound,
    Conflict,
    Integrity,
    Capacity,
    Pending,
    Storage,
}

fn error(kind: FailureKind, detail: impl std::fmt::Display) -> Error {
    Error::Dataset {
        kind,
        detail: detail.to_string(),
    }
}

impl From<DatasetError> for Error {
    fn from(value: DatasetError) -> Self {
        if matches!(value, DatasetError::ReadCancelled) {
            Self::Cancelled
        } else {
            error(FailureKind::Invalid, value)
        }
    }
}
impl From<AuthorityError> for Error {
    fn from(value: AuthorityError) -> Self {
        let kind = match value {
            AuthorityError::Invalid(value) => return value.into(),
            AuthorityError::NotFound | AuthorityError::SnapshotNotFound => FailureKind::NotFound,
            AuthorityError::AncestryCycle | AuthorityError::Corrupt => FailureKind::Integrity,
            AuthorityError::Store(_) => FailureKind::Storage,
            AuthorityError::ReclaimPending { .. } => FailureKind::Pending,
            _ => FailureKind::Conflict,
        };
        error(kind, value)
    }
}
impl From<UploadError> for Error {
    fn from(value: UploadError) -> Self {
        let kind = match &value {
            UploadError::Invalid => FailureKind::Invalid,
            UploadError::NotFound => FailureKind::NotFound,
            UploadError::Conflict => FailureKind::Conflict,
            UploadError::Integrity => FailureKind::Integrity,
            UploadError::Pending => FailureKind::Pending,
            UploadError::Capacity => FailureKind::Capacity,
            UploadError::Storage(_) | UploadError::Store(_) => FailureKind::Storage,
        };
        error(kind, value)
    }
}
impl From<crowdb_access_dataset::composition::CompositionError> for Error {
    fn from(value: crowdb_access_dataset::composition::CompositionError) -> Self {
        use crowdb_access_dataset::composition::CompositionError as E;
        let kind = match value {
            E::Authority(value) => return value.into(),
            E::Upload(value) => return value.into(),
            E::Split(value) => return value.into(),
            E::Inspection(value) => return value.into(),
            E::NotFound => FailureKind::NotFound,
            E::Conflict | E::Frozen => FailureKind::Conflict,
            E::Corrupt => FailureKind::Integrity,
            E::Limit => FailureKind::Capacity,
            E::Store(_) => FailureKind::Storage,
            E::Record(_) => FailureKind::Invalid,
        };
        error(kind, value)
    }
}
impl From<crowdb_access_dataset::split::SplitLifecycleError> for Error {
    fn from(value: crowdb_access_dataset::split::SplitLifecycleError) -> Self {
        use crowdb_access_dataset::split::SplitLifecycleError as E;
        let kind = match value {
            E::Authority(value) => return value.into(),
            E::Inspection(value) => return value.into(),
            E::NotFound => FailureKind::NotFound,
            E::Conflict | E::Frozen => FailureKind::Conflict,
            E::Limit => FailureKind::Capacity,
            E::Corrupt => FailureKind::Integrity,
            E::Store(_) => FailureKind::Storage,
            _ => FailureKind::Invalid,
        };
        error(kind, value)
    }
}

impl From<crowdb_access_dataset::inspection::InspectionError> for Error {
    fn from(value: crowdb_access_dataset::inspection::InspectionError) -> Self {
        use crowdb_access_dataset::inspection::InspectionError as E;
        let kind = match value {
            E::Authority(value) => return value.into(),
            E::Cancelled => return Self::Cancelled,
            E::Store(_) => FailureKind::Storage,
            E::Limit => FailureKind::Capacity,
            E::NotPrepared => FailureKind::Pending,
            E::Unsupported(_) => FailureKind::Invalid,
            E::Corrupt => FailureKind::Integrity,
        };
        error(kind, value)
    }
}
