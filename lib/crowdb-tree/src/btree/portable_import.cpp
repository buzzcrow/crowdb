// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "portable_import.h"

namespace crowdb::tree
{
PortableImportBuilder::PortableImportBuilder(const Config &config, PageLoadLimits loads, uint64_t coverage,
                                             std::shared_ptr<detail::AllocationReservation> metadata)
    : loads_(loads),
      metadata_(std::move(metadata)),
      catalog_(detail::reserved_shared<ReadablePageCatalog>(metadata_, metadata_)),
      leaf_frame_(config.frame_bytes, detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(metadata_)),
      scratch_(config.frame_bytes, detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(metadata_)),
      level_(detail::ReservationAllocator<Node>(metadata_))
{
    config_.frame_bytes      = config.frame_bytes;
    config_.max_key_size     = config.max_key_size;
    config_.max_inline_value = config.max_inline_value;
    config_.inner_max_keys   = config.inner_max_keys;
    if (config.key_range.start()) {
        range_.lower = OwnedKey(Slice(*config.key_range.start()), metadata_);
    }
    if (config.key_range.end()) {
        range_.upper = OwnedKey(Slice(*config.key_range.end()), metadata_);
    }
    catalog_->coverage   = coverage;
    catalog_->generation = 1;
}

std::optional<size_t> PortableImportBuilder::allocation_bound(const NativeImportShape &shape) noexcept
{
    // Each level's vector may coexist with its geometric-growth replacement
    // and the next parent level. Boundary keys share storage between levels.
    const auto key = detail::AllocationBound(OwnedKey::allocation_overhead()).add(shape.max_key_bytes).add(1).value();
    if (!key) {
        return {};
    }
    return detail::AllocationBound()
        .add_product(2, shape.max_frame_bytes)
        .add_product(shape.leaf_pages, 4 * sizeof(Node))
        .add_product(shape.leaf_pages, 2 * *key)
        .add_product(4, *key)
        .add_product(shape.primary_pages, sizeof(uint64_t) + sizeof(Slice))
        .add_product(3, detail::AllocationBound(kCellHeaderSize).add(shape.max_value_bytes).value().value_or(SIZE_MAX))
        .value();
}

Status PortableImportBuilder::reserve_pid(uint64_t *out)
{
    if (next_pid_ >= MappingView::kPageLimit || next_pid_ == NativeImportLimits{}.max_frames) {
        return Status::resource_exhausted("portable import frame namespace is full");
    }
    *out = next_pid_++;
    return Status::Ok();
}

void PortableImportBuilder::begin_leaf(uint64_t pid)
{
    leaf_pid_ = pid;
    leaf_.emplace(leaf_frame_.data(), leaf_frame_.size());
    first_     = {};
    used_      = kFrameHeaderSize + kFrameTrailerSize;
    key_bytes_ = 0;
}

Status PortableImportBuilder::validate(Slice key, Slice encoded) const
{
    const CellView cell(encoded);
    if (!cell.valid() || cell.slot() > catalog_->coverage || cell.is_overflow() ||
        (cell.flags() != 0 && cell.flags() != kFlagTombstone) ||
        (cell.is_tombstone() && encoded.size() != kCellHeaderSize) || !range_.contains(key) ||
        (have_previous_ && Slice(previous_).compare(key) >= 0)) {
        return Status::invalid_argument("portable import requires sorted cells within its range and coverage");
    }
    const auto key_limit = config_.max_key_size != 0 ? config_.max_key_size : config_.frame_bytes / 2;
    if (key.size() > key_limit || key.size() > loads_.max_leaf_key_bytes ||
        cell.value().size() > loads_.max_value_bytes) {
        return Status::resource_exhausted("portable import key or value exceeds the Tree read/write limit");
    }
    return Status::Ok();
}

Status PortableImportBuilder::save_leaf(uint64_t right)
{
    if (!leaf_) {
        return Status::internal_error("portable import leaf is absent");
    }
    leaf_->finish(leaf_pid_, right);
    auto status = append_native_frame(*catalog_, leaf_pid_, leaf_frame_, config_, loads_, {}, metadata_);
    if (!status.ok()) {
        return status;
    }
    level_.push_back(
        {.pid = leaf_pid_, .first = first_, .last = previous_, .first_leaf = leaf_pid_, .last_leaf = leaf_pid_});
    return Status::Ok();
}

Status PortableImportBuilder::append_impl(Slice key, Slice encoded)
{
    auto status = validate(key, encoded);
    if (!status.ok()) {
        return status;
    }
    const CellView cell(encoded);
    const auto     inline_limit = config_.max_inline_value != 0 ? config_.max_inline_value : config_.frame_bytes / 4;
    const bool     spill  = !cell.is_tombstone() && (cell.value().size() > inline_limit ||
                                                     key.size() + encoded.size() + kLeafSlotSize >
                                                         config_.frame_bytes - kFrameHeaderSize - kFrameTrailerSize);
    const size_t   needed = kLeafSlotSize + key.size() + (spill ? kOverflowCellSize : encoded.size());
    if (needed > config_.frame_bytes - kFrameHeaderSize - kFrameTrailerSize) {
        return Status::resource_exhausted("portable import entry cannot fit a primary frame");
    }
    if (!leaf_ || leaf_->count() == loads_.max_leaf_entries || key.size() > loads_.max_leaf_key_bytes - key_bytes_ ||
        needed > config_.frame_bytes - used_) {
        uint64_t next = kInvalidPageId;
        status        = reserve_pid(&next);
        if (!status.ok()) {
            return status;
        }
        if (leaf_) {
            status = save_leaf(next);
            if (!status.ok()) {
                return status;
            }
        }
        begin_leaf(next);
    }
    std::array<uint8_t, kOverflowCellSize> overflow{};
    if (spill) {
        status = spill_value(cell, &overflow);
        if (!status.ok()) {
            return status;
        }
        encoded = Slice(overflow.data(), overflow.size());
    }
    auto owned_key = OwnedKey(key, metadata_);
    if (!leaf_) {
        return Status::internal_error("portable import leaf was not initialized");
    }
    if (!leaf_->try_append_sorted(key, encoded)) {
        return Status::internal_error("portable import frame preflight disagreed with its builder");
    }
    if (leaf_->count() == 1) {
        first_ = owned_key;
    }
    previous_      = std::move(owned_key);
    have_previous_ = true;
    used_ += needed;
    key_bytes_ += key.size();
    return Status::Ok();
}

Status PortableImportBuilder::append(Slice key, Slice encoded)
try {
    if (finished_) {
        return Status::unavailable("portable import is already finished");
    }
    if (status_.ok()) {
        status_ = append_impl(key, encoded);
    }
    return status_;
}
catch (const std::bad_alloc &) {
    status_ = Status::resource_exhausted();
    return status_;
}

Status PortableImportBuilder::finish(std::shared_ptr<ReadablePageCatalog> *out)
try {
    if (out == nullptr || *out || finished_) {
        return Status::invalid_argument();
    }
    finished_ = true;
    if (!status_.ok()) {
        return status_;
    }
    if (leaf_) {
        status_ = save_leaf(kInvalidPageId);
        if (status_.ok()) {
            status_ = build_index();
        }
    }
    if (status_.ok()) {
        status_ = attach_native_dependencies(*catalog_, {}, metadata_);
    }
    if (status_.ok()) {
        *out = std::move(catalog_);
    }
    return status_;
}
catch (const std::bad_alloc &) {
    status_ = Status::resource_exhausted();
    return status_;
}
} // namespace crowdb::tree
