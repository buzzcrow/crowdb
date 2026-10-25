// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use sha2::Digest;

use super::{
    frame_error, FramePrefixScanner, FrameScanError, FrameScanMode, PendingGroup, ScannedFrameExtent,
};
use crate::frame::{parse_frame, AppendGroupValidator, FrameError, FrameLayout, FrameMagic, FrameRole};

impl FramePrefixScanner {
    pub(super) fn accept_scan_frame(
        &mut self,
        offset: u64,
        bytes: &[u8],
    ) -> Result<Option<ScannedFrameExtent>, FrameScanError> {
        let frame = parse_frame(bytes, self.config.chunk_id).map_err(|error| frame_error(offset, error))?;
        self.frames_parsed += 1;
        if self.config.mode == FrameScanMode::Ordinary {
            if frame.role != FrameRole::Ordinary || frame.header.magic == FrameMagic::StreamV2 {
                return Err(frame_error(offset, FrameError::InvalidRole));
            }
            self.hash.update(bytes);
            return self
                .complete_scan_extent(offset, frame.physical_length as u64, frame.payload.len() as u64)
                .map(Some);
        }
        if frame.header.magic != FrameMagic::StreamV2 {
            return Err(frame_error(offset, FrameError::InvalidRole));
        }
        if let Some(group) = &mut self.group {
            group
                .validator
                .push(offset, &frame)
                .map_err(|error| frame_error(offset, error))?;
            group.hash.update(bytes);
        } else {
            let FrameRole::Head { logical_length, .. } = frame.role else {
                return Err(frame_error(offset, FrameError::InvalidGroup));
            };
            let physical_length = FrameLayout::StreamGroupV2
                .physical_length(logical_length)
                .map_err(|error| frame_error(offset, error))?;
            if logical_length > self.config.max_append_bytes
                || offset
                    .checked_add(physical_length)
                    .map_or(true, |end| end > self.config.physical_limit)
            {
                return Err(frame_error(offset, FrameError::InvalidGroup));
            }
            let validator =
                AppendGroupValidator::new(offset, &frame).map_err(|error| frame_error(offset, error))?;
            let mut hash = self.hash.clone();
            hash.update(bytes);
            self.group = Some(PendingGroup {
                validator,
                physical_start: offset,
                logical_length,
                hash,
            });
        }
        if !self
            .group
            .as_ref()
            .is_some_and(|group| group.validator.is_complete())
        {
            return Ok(None);
        }
        let group = self.group.take().expect("completed group exists");
        let parsed = group
            .validator
            .finish()
            .map_err(|error| frame_error(offset, error))?;
        self.hash = group.hash;
        self.complete_scan_extent(group.physical_start, parsed.physical_length, group.logical_length)
            .map(Some)
    }

    fn complete_scan_extent(
        &mut self,
        physical_offset: u64,
        physical_length: u64,
        logical_length: u64,
    ) -> Result<ScannedFrameExtent, FrameScanError> {
        let end = physical_offset
            .checked_add(physical_length)
            .ok_or_else(|| frame_error(physical_offset, FrameError::LengthOverflow))?;
        if self.config.mode == FrameScanMode::Grouped
            && self.physical_end < self.config.required_end
            && end > self.config.required_end
        {
            return Err(FrameScanError::CheckpointBoundary {
                required: self.config.required_end,
                observed: end,
            });
        }
        let logical_offset = self.logical_end;
        self.logical_end = logical_offset
            .checked_add(logical_length)
            .ok_or_else(|| frame_error(physical_offset, FrameError::LengthOverflow))?;
        self.physical_end = end;
        self.complete_extents += 1;
        Ok(ScannedFrameExtent {
            physical_offset,
            physical_length,
            logical_offset,
            logical_length,
        })
    }
}
