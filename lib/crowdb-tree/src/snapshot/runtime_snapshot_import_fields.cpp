// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "runtime_snapshot_import.h"

namespace crowdb::tree
{
Status RuntimeSnapshotImport::advance_portable()
{
    switch (part_) {
    case Part::KeyLength:
        required_ = frame_u32(fixed_.data(), 0);
        if (required_ > max_key_) {
            return Status::resource_exhausted("snapshot key exceeds Tree limit");
        }
        key_.resize(required_);
        expect(Part::Key, required_);
        break;
    case Part::Key:
        expect(Part::Slot, 8);
        break;
    case Part::Slot:
        slot_ = frame_u64(fixed_.data(), 0);
        if (slot_ > coverage_) {
            return Status::corruption("snapshot cell exceeds its coverage");
        }
        expect(Part::Kind, 1);
        break;
    case Part::Kind:
        kind_ = fixed_[0];
        if (kind_ > 1) {
            return Status::corruption("snapshot cell kind is invalid");
        }
        expect(Part::ValueLength, 4);
        break;
    case Part::ValueLength:
        required_ = frame_u32(fixed_.data(), 0);
        if (required_ > loads_.max_value_bytes) {
            return Status::resource_exhausted("snapshot value exceeds Tree limit");
        }
        if (kind_ != 0 && required_ != 0) {
            return Status::corruption("snapshot tombstone contains a value");
        }
        window_.resize(kCellHeaderSize + required_);
        frame_put_u64(window_.data(), 0, slot_);
        window_[sizeof(uint64_t)] = kind_ != 0 ? kFlagTombstone : 0;
        expect(Part::Value, required_);
        break;
    case Part::Value: {
        auto status = portable_->append(Slice(key_.data(), key_.size()), Slice(window_.data(), window_.size()));
        if (!status.ok()) {
            return status;
        }
        completed_entry();
        break;
    }
    default:
        return Status::internal_error("invalid portable parser state");
    }
    return Status::Ok();
}

Status RuntimeSnapshotImport::advance_native(TreeCore &core)
{
    switch (part_) {
    case Part::PageId:
        pid_ = frame_u64(fixed_.data(), 0);
        expect(Part::FrameLength, 4);
        break;
    case Part::FrameLength:
        required_ = frame_u32(fixed_.data(), 0);
        if (required_ > loads_.max_frame_bytes || required_ < kFrameHeaderSize + kFrameTrailerSize) {
            return Status::resource_exhausted("snapshot frame size exceeds Tree limit");
        }
        window_.resize(required_);
        expect(Part::Frame, required_);
        break;
    case Part::Frame: {
        auto status = append_native_frame(*catalog_, pid_, window_, core.config(), loads_, {}, metadata_);
        if (!status.ok()) {
            return status;
        }
        completed_entry();
        break;
    }
    default:
        return Status::internal_error("invalid native parser state");
    }
    return Status::Ok();
}
} // namespace crowdb::tree
