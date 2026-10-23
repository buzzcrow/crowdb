// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_sdk_native::{dataset::FailureKind, Error};
use pyo3::{create_exception, exceptions::PyException, prelude::*};

create_exception!(crowdb_dataset_native, NativeError, PyException);
create_exception!(crowdb_dataset_native, CapacityError, NativeError);
create_exception!(crowdb_dataset_native, IntegrityError, NativeError);
create_exception!(crowdb_dataset_native, CancelledError, NativeError);
create_exception!(crowdb_dataset_native, ClosedError, NativeError);
create_exception!(crowdb_dataset_native, DeadlineError, NativeError);
create_exception!(crowdb_dataset_native, ConflictError, NativeError);
create_exception!(crowdb_dataset_native, UncertainMutationError, NativeError);

pub fn failure(error: &Error) -> PyErr {
    let detail = error.to_string();
    match error {
        Error::Capacity(_)
        | Error::Dataset {
            kind: FailureKind::Capacity,
            ..
        } => CapacityError::new_err(detail),
        Error::Integrity
        | Error::Length { .. }
        | Error::Dataset {
            kind: FailureKind::Integrity,
            ..
        } => IntegrityError::new_err(detail),
        Error::Cancelled => CancelledError::new_err(detail),
        Error::Shutdown => ClosedError::new_err(detail),
        Error::Deadline => DeadlineError::new_err(detail),
        Error::Dataset {
            kind: FailureKind::Conflict,
            ..
        } => ConflictError::new_err(detail),
        Error::UncertainMutation { .. } => UncertainMutationError::new_err(detail),
        _ => NativeError::new_err(detail),
    }
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("NativeError", py.get_type::<NativeError>())?;
    module.add("CapacityError", py.get_type::<CapacityError>())?;
    module.add("IntegrityError", py.get_type::<IntegrityError>())?;
    module.add("CancelledError", py.get_type::<CancelledError>())?;
    module.add("ClosedError", py.get_type::<ClosedError>())?;
    module.add("DeadlineError", py.get_type::<DeadlineError>())?;
    module.add("ConflictError", py.get_type::<ConflictError>())?;
    module.add("UncertainMutationError", py.get_type::<UncertainMutationError>())
}
