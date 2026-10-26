// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_sdk_native::{dataset::FailureKind, Error};
use pyo3::{
    create_exception,
    exceptions::PyException,
    prelude::*,
    types::{PyDict, PyList},
};

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
        Error::SnapshotHeadConflict { observed } => head_conflict(detail, |head| {
            head.set_item(
                "latest",
                bytes_list(
                    head.py(),
                    observed.latest.as_ref().map(|id| id.as_bytes().as_slice()),
                )?,
            )?;
            head.set_item(
                "stable",
                bytes_list(
                    head.py(),
                    observed.stable.as_ref().map(|id| id.as_bytes().as_slice()),
                )?,
            )?;
            head.set_item(
                "generation",
                bytes_list(
                    head.py(),
                    observed.generation.as_ref().map(|id| id.as_bytes().as_slice()),
                )?,
            )
        }),
        Error::SplitHeadConflict { snapshot, observed } => head_conflict(detail, |head| {
            head.set_item("snapshot", PyList::new(head.py(), snapshot.as_bytes())?)?;
            head.set_item(
                "revision",
                bytes_list(
                    head.py(),
                    observed.revision.as_ref().map(|id| id.as_bytes().as_slice()),
                )?,
            )?;
            head.set_item(
                "generation",
                bytes_list(
                    head.py(),
                    observed.generation.as_ref().map(|id| id.as_bytes().as_slice()),
                )?,
            )
        }),
        Error::Dataset {
            kind: FailureKind::Conflict,
            ..
        } => ConflictError::new_err(detail),
        Error::UncertainMutation { .. } => UncertainMutationError::new_err(detail),
        _ => NativeError::new_err(detail),
    }
}

fn head_conflict(detail: String, fill: impl FnOnce(&Bound<'_, PyDict>) -> PyResult<()>) -> PyErr {
    Python::attach(|py| {
        let error = ConflictError::new_err(detail);
        let head = PyDict::new(py);
        match fill(&head).and_then(|()| error.value(py).setattr("observed_head", head)) {
            Ok(()) => error,
            Err(error) => error,
        }
    })
}

fn bytes_list<'py>(py: Python<'py>, value: Option<&[u8]>) -> PyResult<Option<Bound<'py, PyList>>> {
    value.map(|bytes| PyList::new(py, bytes)).transpose()
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
