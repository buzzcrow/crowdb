// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production HTTP body transfer with injected storage and placement.

use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use crowdb_chunk_client::ChunkIoWriter;
use hyper::body::Incoming;
use hyper::Request;

/// Exercise the production S3 body transfer through a real HTTP listener.
///
/// # Errors
/// Returns the same framing, integrity or write failure as the production flow.
pub async fn write_large_body_for_tests(
    mut request: Request<Incoming>,
    writer: &mut impl ChunkIoWriter,
    allocator: &NativeBodyAllocator,
) -> Result<(String, Vec<u8>), String> {
    let receiver = crate::http_receive::install_native_body_receive_provider(&mut request, allocator);
    receiver.enable_owner_handoff();
    let (parts, incoming) = request.into_parts();
    let mut body =
        crate::upload_flow::body_encoding::UploadBody::new(incoming, &parts.headers, None, 64 << 20)
            .map_err(|error| error.to_string())?;
    body.defer_md5();
    let length = body.decoded_length();
    super::operations::write_object_body(&mut body, writer, Some(&receiver), length, None, None, 4, None)
        .await
        .map_err(|error| format!("{error:?}"))
}
