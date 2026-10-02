// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP body receive ownership shared by the S3 and Iceberg listeners.

use std::sync::Arc;

use crowdb_access_s3::native_buffer::{NativeBodyAllocator, NativeBodyReceiver};
use hyper::body::{Http1BodyReceiveProvider, Incoming};
use hyper::Request;

#[derive(Clone)]
pub(crate) struct DeferredBodyReceiveProvider {
    provider: Arc<dyn Http1BodyReceiveProvider>,
    native: Option<Arc<NativeBodyReceiver>>,
}

impl DeferredBodyReceiveProvider {
    pub(crate) fn generic(provider: Arc<dyn Http1BodyReceiveProvider>) -> Self {
        Self {
            provider,
            native: None,
        }
    }

    pub(crate) fn native(receiver: Arc<NativeBodyReceiver>) -> Self {
        Self {
            provider: receiver.clone(),
            native: Some(receiver),
        }
    }
}

/// Installs a provider only after the request has passed authentication and admission.
pub fn install_body_receive_provider(request: &mut Request<Incoming>) -> Option<Arc<NativeBodyReceiver>> {
    let deferred = request.extensions_mut().remove::<DeferredBodyReceiveProvider>()?;
    request
        .body_mut()
        .set_http1_body_receive_provider(deferred.provider);
    deferred.native
}

/// Selects a bounded native owner for one admitted upload.
pub(crate) fn install_native_body_receive_provider(
    request: &mut Request<Incoming>,
    allocator: &NativeBodyAllocator,
) -> Arc<NativeBodyReceiver> {
    let receiver = Arc::new(allocator.object_receiver());
    request
        .body_mut()
        .set_http1_body_receive_provider(receiver.clone());
    receiver
}

/// Select a receive owner sized for one complete small object.
pub(crate) fn install_small_body_receive_provider(
    request: &mut Request<Incoming>,
    allocator: &NativeBodyAllocator,
    payload_bytes: usize,
) -> Option<Arc<NativeBodyReceiver>> {
    let receiver = Arc::new(allocator.object_receiver_for_payload(payload_bytes).ok()?);
    request
        .body_mut()
        .set_http1_body_receive_provider(receiver.clone());
    Some(receiver)
}
