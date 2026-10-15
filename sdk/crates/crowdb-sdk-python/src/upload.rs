// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{operation::NativeOperation, Resources};
use crowdb_sdk_core::Error;
use pyo3::{exceptions::PyRuntimeError, PyResult};
use std::sync::Arc;

pub(super) fn part(
    resources: &Arc<Resources>,
    id: &str,
    index: u32,
    data: &[u8],
    control: bool,
) -> PyResult<NativeOperation> {
    resources.check()?;
    if data.is_empty() || data.len() > 4 * 1024 * 1024 {
        return Err(PyRuntimeError::new_err(
            "upload part must contain 1 byte through 4 MiB",
        ));
    }
    let id: crowdb_sdk_dataset::v2::OpaqueId = match id.parse() {
        Ok(id) => id,
        Err(error) => return Ok(NativeOperation::failed(resources.clone(), &error)),
    };
    let client = resources.client();
    let credit = match resources.budget(data.len()) {
        Ok(credit) => credit,
        Err(error) => return Ok(NativeOperation::failed(resources.clone(), &error)),
    };
    let data = bytes::Bytes::copy_from_slice(data);
    Ok(NativeOperation::spawn_future(resources.clone(), async move {
        let _credit = credit;
        if control {
            Ok::<_, Error>(serde_json::to_string(
                &client.control().upload_part(&id, index, data).await?,
            )?)
        } else {
            Ok(serde_json::to_string(
                &client.upload_part(id.as_str(), index, data).await?,
            )?)
        }
    }))
}
