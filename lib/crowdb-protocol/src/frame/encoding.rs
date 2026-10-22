// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Public self-validating chunk frame encoding and location arithmetic.

use super::{frame_length, ChunkId, FrameError, FrameHeaderPrefix, FrameMagic, FRAME_FOOTER_BYTES};

/// Encode one ordinary frame with the selected revision.
///
/// # Errors
///
/// Returns [`FrameError::PayloadTooLarge`] when `payload` cannot fit one frame.
pub fn encode_frame(
    magic: FrameMagic,
    chunk_id: ChunkId,
    payload: &[u8],
    write_time_ms: u64,
) -> Result<Vec<u8>, FrameError> {
    let mut header = vec![0; magic.ordinary_header_bytes()];
    let mut footer = [0; FRAME_FOOTER_BYTES];
    encode_frame_regions(magic, chunk_id, payload, write_time_ms, &mut header, &mut footer)?;
    let length = header.len() + payload.len() + footer.len();
    let mut frame = Vec::with_capacity(length);
    frame.extend_from_slice(&header);
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&footer);
    Ok(frame)
}

/// Encode one canonical frame into caller-owned header and footer regions.
///
/// The payload is read in place and is never copied. Native receive owners use
/// this to fill framing bytes reserved around socket-written payload.
///
/// # Errors
///
/// Returns an error for an oversized payload or incorrectly sized regions.
pub fn encode_frame_regions(
    magic: FrameMagic,
    chunk_id: ChunkId,
    payload: &[u8],
    write_time_ms: u64,
    header_region: &mut [u8],
    footer_region: &mut [u8],
) -> Result<(), FrameError> {
    prepare_frame_regions(magic, payload, write_time_ms, header_region, footer_region)?;
    set_frame_chunk_id(chunk_id, footer_region)
}

/// Prepare the frame header and CRC before its destination chunk is known.
/// The CRC covers the header and payload; the chunk ID is checked separately
/// against the expected location when the frame is read.
///
/// # Errors
/// Returns an error for an oversized payload or incorrectly sized regions.
pub fn prepare_frame_regions(
    magic: FrameMagic,
    payload: &[u8],
    write_time_ms: u64,
    header_region: &mut [u8],
    footer_region: &mut [u8],
) -> Result<(), FrameError> {
    if magic == FrameMagic::StreamV2 {
        return Err(FrameError::InvalidRole);
    }
    if header_region.len() != magic.ordinary_header_bytes() || footer_region.len() != FRAME_FOOTER_BYTES {
        return Err(FrameError::InvalidRegionLength);
    }
    let payload_size = u16::try_from(payload.len()).map_err(|_| FrameError::PayloadTooLarge)?;
    let header = FrameHeaderPrefix {
        magic,
        payload_offset: u16::try_from(magic.ordinary_header_bytes())
            .map_err(|_| FrameError::LengthOverflow)?,
        payload_size,
        write_time_ms,
    };
    frame_length(header)?;
    write_header_region(header_region, header);
    let checksum = crc32c_parts([header_region, payload]);
    footer_region[..4].copy_from_slice(&checksum.to_le_bytes());
    Ok(())
}

/// Frame payload views without copying their bytes into a contiguous allocation.
///
/// # Errors
/// Returns an error when the total payload cannot fit one canonical frame.
pub fn encode_frame_views(
    magic: FrameMagic,
    chunk_id: ChunkId,
    payload: Vec<bytes::Bytes>,
    write_time_ms: u64,
) -> Result<Vec<bytes::Bytes>, FrameError> {
    if magic == FrameMagic::StreamV2 {
        return Err(FrameError::InvalidRole);
    }
    let length = payload.iter().try_fold(0usize, |total, view| {
        total.checked_add(view.len()).ok_or(FrameError::LengthOverflow)
    })?;
    let header = FrameHeaderPrefix {
        magic,
        payload_offset: u16::try_from(magic.ordinary_header_bytes())
            .map_err(|_| FrameError::LengthOverflow)?,
        payload_size: u16::try_from(length).map_err(|_| FrameError::PayloadTooLarge)?,
        write_time_ms,
    };
    frame_length(header)?;
    let mut header_region = vec![0; magic.ordinary_header_bytes()];
    let mut footer_region = [0; FRAME_FOOTER_BYTES];
    write_header_region(&mut header_region, header);
    let checksum = crc32c_parts(
        std::iter::once(header_region.as_slice()).chain(payload.iter().map(bytes::Bytes::as_ref)),
    );
    footer_region[..4].copy_from_slice(&checksum.to_le_bytes());
    set_frame_chunk_id(chunk_id, &mut footer_region)?;
    let mut views = Vec::with_capacity(payload.len() + 2);
    views.push(bytes::Bytes::copy_from_slice(&header_region));
    views.extend(payload);
    views.push(bytes::Bytes::copy_from_slice(&footer_region));
    Ok(views)
}

/// Set the destination chunk after the frame header and CRC are prepared.
///
/// # Errors
/// Returns an error for an incorrectly sized footer region.
pub fn set_frame_chunk_id(chunk_id: ChunkId, footer_region: &mut [u8]) -> Result<(), FrameError> {
    if footer_region.len() != FRAME_FOOTER_BYTES {
        return Err(FrameError::InvalidRegionLength);
    }
    footer_region[4..12].copy_from_slice(&chunk_id.high.to_be_bytes());
    footer_region[12..20].copy_from_slice(&chunk_id.low.to_be_bytes());
    Ok(())
}

/// Encode an object as its consecutive frame sequence.
///
/// Every interior frame has the maximum payload and the final frame is
/// variable sized. An empty payload has no frame because it has no durable
/// object location.
///
/// # Errors
///
/// Returns an error if an individual frame cannot be encoded.
pub fn encode_frames(
    magic: FrameMagic,
    chunk_id: ChunkId,
    payload: &[u8],
    write_time_ms: u64,
) -> Result<Vec<Vec<u8>>, FrameError> {
    payload
        .chunks(magic.ordinary_payload_capacity())
        .map(|part| encode_frame(magic, chunk_id, part, write_time_ms))
        .collect()
}

pub(super) fn write_header_region(region: &mut [u8], header: FrameHeaderPrefix) {
    region[0..2].copy_from_slice(&(header.magic as u16).to_le_bytes());
    region[2..4].copy_from_slice(&header.payload_offset.to_le_bytes());
    region[4..6].copy_from_slice(&header.payload_size.to_le_bytes());
    region[6..14].copy_from_slice(&header.write_time_ms.to_le_bytes());
    if header.magic.is_v2() {
        region[14] = 0;
    }
}

pub(super) fn crc32c_parts<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> u32 {
    let mut crc = 0_u32;
    for bytes in parts {
        crc = crowdb_common::ec_isal::crc32c_update(crc, bytes);
    }
    crc
}
