// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::frame::{
    FramePrefixScanner, FrameScanConfig, FrameScanStop, MAX_FRAME_SCAN_EXTENTS, MAX_FRAME_SCAN_INPUT_BYTES,
};
use tokio::time::{timeout_at, Instant};

use super::{
    FrameExtentSink, FrameObservation, FrameObservationError, FrameObservationResult, FrameWindowReader,
};

/// Observes one complete physical prefix within the caller's original deadline.
pub async fn observe_frame_prefix(
    reader: &dyn FrameWindowReader,
    config: FrameScanConfig,
    window_bytes: u32,
    deadline: Instant,
) -> FrameObservationResult<FrameObservation> {
    scan_prefix(reader, config, window_bytes, deadline, None).await
}

/// Rebuilds bounded mappings from an observed source and verifies the exact
/// complete-prefix digest before returning success. The original deadline applies.
pub async fn replay_frame_prefix(
    reader: &dyn FrameWindowReader,
    expected: &FrameObservation,
    deadline: Instant,
    sink: &mut dyn FrameExtentSink,
) -> FrameObservationResult<FrameObservation> {
    let mut config = expected.config;
    config.physical_limit = expected.summary.physical_end;
    config.required_end = config.physical_limit;
    let actual = scan_prefix(reader, config, expected.window_bytes, deadline, Some(sink)).await?;
    if actual.summary.physical_end != expected.summary.physical_end
        || actual.summary.logical_end != expected.summary.logical_end
        || actual.summary.digest != expected.summary.digest
    {
        return Err(FrameObservationError::ChangedPrefix);
    }
    Ok(actual)
}

async fn scan_prefix(
    reader: &dyn FrameWindowReader,
    config: FrameScanConfig,
    window_bytes: u32,
    deadline: Instant,
    mut sink: Option<&mut dyn FrameExtentSink>,
) -> FrameObservationResult<FrameObservation> {
    if window_bytes == 0 || window_bytes as usize > MAX_FRAME_SCAN_INPUT_BYTES {
        return Err(FrameObservationError::InvalidRequest);
    }
    let mut scanner = FramePrefixScanner::new(config)?;
    let mut offset = config.physical_start;
    let mut reads = 0;
    let mut read_bytes = 0;
    while offset < config.physical_limit && scanner.stop() == FrameScanStop::Running {
        check_deadline(deadline)?;
        let length = u32::try_from((config.physical_limit - offset).min(u64::from(window_bytes)))
            .expect("bounded window");
        let bytes = timeout_at(deadline, reader.read_window(offset, length))
            .await
            .map_err(|_| FrameObservationError::DeadlineExpired)?
            .map_err(|source| FrameObservationError::Read { offset, source })?;
        reads += 1;
        read_bytes += bytes.len() as u64;
        if bytes.len() != length as usize {
            return Err(FrameObservationError::ShortRead {
                offset,
                expected: length,
                actual: bytes.len(),
            });
        }
        let mut consumed = 0;
        while consumed < bytes.len() && scanner.stop() == FrameScanStop::Running {
            check_deadline(deadline)?;
            let batch = scanner.push(
                offset + consumed as u64,
                &bytes[consumed..],
                MAX_FRAME_SCAN_EXTENTS,
            )?;
            if let Some(sink) = &mut sink {
                if !batch.extents.is_empty() {
                    timeout_at(deadline, sink.append_extents(&batch.extents))
                        .await
                        .map_err(|_| FrameObservationError::DeadlineExpired)?
                        .map_err(FrameObservationError::Sink)?;
                }
            }
            consumed += batch.consumed;
        }
        offset += consumed as u64;
        tokio::task::yield_now().await;
    }
    check_deadline(deadline)?;
    Ok(FrameObservation {
        config,
        window_bytes,
        summary: scanner.finish()?,
        reads,
        read_bytes,
    })
}

fn check_deadline(deadline: Instant) -> FrameObservationResult<()> {
    if Instant::now() >= deadline {
        Err(FrameObservationError::DeadlineExpired)
    } else {
        Ok(())
    }
}
