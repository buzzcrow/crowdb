// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "runtime_snapshot_import.h"
#include "snapshot_wire.h"

namespace crowdb::tree
{
void RuntimeSnapshotImport::expect(Part part, size_t bytes)
{
    part_       = part;
    required_   = bytes;
    fixed_used_ = 0;
    copied_     = 0;
}

void RuntimeSnapshotImport::consume_fixed(std::span<const uint8_t> &bytes, bool include_crc)
{
    const auto take = std::min(bytes.size(), required_ - fixed_used_);
    std::copy_n(bytes.data(), take, fixed_.data() + fixed_used_);
    if (include_crc) {
        crc_ = snapshot_detail::crc_bytes(crc_, bytes.first(take));
    }
    fixed_used_ += take;
    bytes = bytes.subspan(take);
}

void RuntimeSnapshotImport::consume_window(std::span<const uint8_t>                                  &bytes,
                                           detail::ReservedVector<uint8_t, AllocationKind::kScratch> &window,
                                           size_t                                                     offset)
{
    const auto take = std::min(bytes.size(), required_ - copied_);
    if (take == 0) {
        return;
    }
    std::copy_n(bytes.data(), take, window.data() + offset + copied_);
    crc_ = snapshot_detail::crc_bytes(crc_, bytes.first(take));
    copied_ += take;
    bytes = bytes.subspan(take);
}

void RuntimeSnapshotImport::expect_entry()
{
    if (parsed_ == expected_) {
        expect(Part::Trailer, 4);
    }
    else {
        expect(native_ ? Part::PageId : Part::KeyLength, native_ ? 8 : 4);
    }
}

void RuntimeSnapshotImport::completed_entry()
{
    ++parsed_;
    expect_entry();
}

Status RuntimeSnapshotImport::begin(TreeCore &core)
{
    coverage_              = frame_u64(fixed_.data(), 9);
    expected_              = frame_u64(fixed_.data(), native_ ? 25 : 17);
    const auto max_entries = uint64_t{NativeImportLimits{}.max_frames} * loads_.max_leaf_entries;
    if (expected_ > (native_ ? NativeImportLimits{}.max_frames : max_entries)) {
        return Status::resource_exhausted("snapshot import count exceeds its frame limits");
    }
    if (native_) {
        catalog_               = detail::reserved_shared<ReadablePageCatalog>(metadata_, metadata_);
        catalog_->coverage     = coverage_;
        catalog_->generation   = 1;
        catalog_->root_page_id = frame_u64(fixed_.data(), 17);
    }
    else {
        portable_ =
            detail::reserved_unique<PortableImportBuilder>(metadata_, core.config(), loads_, coverage_, metadata_);
    }
    expect_entry();
    return Status::Ok();
}

Status RuntimeSnapshotImport::advance(TreeCore &core)
{
    switch (part_) {
    case Part::Header:
        if (required_ == 9) {
            if (frame_u32(fixed_.data(), 0) != 0x4E535443) {
                return Status::corruption("snapshot: bad magic");
            }
            if (frame_u32(fixed_.data(), 4) != 1) {
                return Status::not_supported("snapshot: version");
            }
            if (fixed_[8] > 1) {
                return Status::not_supported("snapshot: unknown format");
            }
            native_   = fixed_[8] == 1;
            required_ = native_ ? 33 : 25;
            return Status::Ok();
        }
        return begin(core);
    case Part::KeyLength:
    case Part::Key:
    case Part::Slot:
    case Part::Kind:
    case Part::ValueLength:
    case Part::Value:
        return advance_portable();
    case Part::PageId:
    case Part::FrameLength:
    case Part::Frame:
        return advance_native(core);
    case Part::Trailer:
        if (frame_u32(fixed_.data(), 0) != crc_) {
            return Status::corruption("snapshot: CRC mismatch");
        }
        expect(Part::Complete, 0);
        break;
    case Part::Complete:
        return Status::corruption("snapshot: trailing bytes");
    }
    return Status::Ok();
}

Status RuntimeSnapshotImport::parse(std::span<const uint8_t> bytes, TreeCore &core)
{
    for (;;) {
        const bool window = part_ == Part::Key || part_ == Part::Value || part_ == Part::Frame;
        if (part_ == Part::Complete) {
            return bytes.empty() ? Status::Ok() : Status::corruption("snapshot: trailing bytes");
        }
        if (bytes.empty() && (window ? copied_ : fixed_used_) != required_) {
            return Status::Ok();
        }
        if (window) {
            consume_window(bytes, part_ == Part::Key ? key_ : window_, part_ == Part::Value ? kCellHeaderSize : 0);
        }
        else {
            consume_fixed(bytes, part_ != Part::Trailer);
        }
        if ((window ? copied_ : fixed_used_) != required_) {
            return Status::Ok();
        }
        auto status = advance(core);
        if (!status.ok()) {
            return status;
        }
    }
}
} // namespace crowdb::tree
