// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "runtime_snapshot_import.h"

namespace crowdb::tree
{
Status RuntimeSnapshotImport::create(TreeCore &core, std::shared_ptr<RuntimeSnapshotImport> *out)
try {
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    std::shared_ptr<detail::AllocationReservation> metadata;
    auto                                           status = core.prepare_import_credit(&metadata);
    if (!status.ok()) {
        return status;
    }
    *out = detail::reserved_shared<RuntimeSnapshotImport>(metadata, core, metadata);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot import control allocation failed");
}

RuntimeSnapshotImport::RuntimeSnapshotImport(TreeCore &core, std::shared_ptr<detail::AllocationReservation> metadata)
    : core_(core.weak_from_this()),
      metadata_(std::move(metadata)),
      loads_(core.load_limits()),
      max_key_(core.config().max_key_size != 0 ? core.config().max_key_size : core.config().frame_bytes / 2),
      key_(detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(metadata_)),
      window_(detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(metadata_))
{
}

RuntimeSnapshotImport::~RuntimeSnapshotImport()
{
    metadata_->seal();
}

Status RuntimeSnapshotImport::fail(Status status)
{
    if (status_.ok()) {
        status_ = std::move(status);
    }
    portable_.reset();
    catalog_.reset();
    key_    = detail::ReservedVector<uint8_t, AllocationKind::kScratch>(key_.get_allocator());
    window_ = detail::ReservedVector<uint8_t, AllocationKind::kScratch>(window_.get_allocator());
    metadata_->seal();
    return status_;
}

Status RuntimeSnapshotImport::feed(Slice bytes)
try {
    if (!status_.ok()) {
        return status_;
    }
    if (finishing_) {
        return Status::unavailable("snapshot import is already finished");
    }
    auto core = core_.lock();
    if (!core || core->closed()) {
        return fail(Status::unavailable("snapshot import Tree is closed"));
    }
    auto status = parse({reinterpret_cast<const uint8_t *>(bytes.data()), bytes.size()}, *core);
    return status.ok() ? status : fail(std::move(status));
}
catch (const std::bad_alloc &) {
    return fail(Status::resource_exhausted("snapshot import staging allocation failed"));
}

Status RuntimeSnapshotImport::finish(uint64_t *coverage)
try {
    if (!status_.ok()) {
        return status_;
    }
    if (finishing_) {
        return Status::unavailable("snapshot import is already finished");
    }
    finishing_ = true;
    if (part_ != Part::Complete || parsed_ != expected_) {
        return fail(Status::invalid_argument("snapshot import stream is truncated"));
    }
    auto core = core_.lock();
    if (!core || core->closed()) {
        return fail(Status::unavailable("snapshot import Tree is closed"));
    }
    if (native_) {
        status_ = attach_native_dependencies(*catalog_, {}, metadata_);
    }
    else {
        status_ = portable_->finish(&catalog_);
    }
    portable_.reset();
    key_    = detail::ReservedVector<uint8_t, AllocationKind::kScratch>(key_.get_allocator());
    window_ = detail::ReservedVector<uint8_t, AllocationKind::kScratch>(window_.get_allocator());
    if (status_.ok()) {
        status_ = core->install_readable(std::move(catalog_), metadata_);
    }
    if (!status_.ok()) {
        return fail(status_);
    }
    if (coverage != nullptr) {
        *coverage = coverage_;
    }
    metadata_->seal();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return fail(Status::resource_exhausted("snapshot import validation allocation failed"));
}
} // namespace crowdb::tree
