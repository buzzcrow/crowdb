// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_frame.h"
#include "chunk_physical_registry_internal.h"

namespace crowdb::tree
{
Status ChunkPhysicalRegistry::prepare(detail::ChunkStageId stage, detail::ChunkId chunk,
                                      const std::shared_ptr<AllocationBudget> &metadata,
                                      std::shared_ptr<ChunkPhysicalPack> *out, ChunkPhysicalAdmission *admission)
try {
    if ((admission != nullptr && admission->rollback_) || stage.owner_epoch == 0 ||
        (stage.operation_high == 0 && stage.operation_low == 0) || chunk.empty() || !metadata || out == nullptr ||
        *out || metadata->retirement_queue() != state_->queue) {
        return Status::invalid_argument("physical pack requires the same paid host queue");
    }
    auto request      = std::allocate_shared<State::Request>(MetadataAllocator<State::Request>(metadata));
    request->action   = State::Action::Prepare;
    request->stage    = stage;
    request->chunk    = chunk;
    request->metadata = metadata;
    std::shared_ptr<State::Request> rollback;
    if (admission != nullptr) {
        rollback = std::allocate_shared<State::Request>(MetadataAllocator<State::Request>(metadata));
    }
    auto status = state_->execute(request);
    if (status.ok()) {
        if (admission != nullptr) {
            rollback->state      = state_;
            rollback->action     = State::Action::Forget;
            rollback->stage      = stage;
            rollback->chunk      = chunk;
            rollback->pack       = request->pack;
            rollback->metadata   = metadata;
            admission->rollback_ = std::move(rollback);
            admission->abort_    = &State::abort;
        }
        *out = std::move(request->pack);
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("physical registry command credit unavailable");
}

Status ChunkPhysicalRegistry::observe(const std::shared_ptr<ChunkPhysicalPack> &pack,
                                      detail::ChunkAllocationIdentity           receipt,
                                      const std::shared_ptr<AllocationBudget>  &metadata)
try {
    if (!pack || !metadata || metadata->retirement_queue() != state_->queue) {
        return Status::invalid_argument();
    }
    auto request      = std::allocate_shared<State::Request>(MetadataAllocator<State::Request>(metadata));
    request->action   = State::Action::Observe;
    request->stage    = pack->stage;
    request->chunk    = pack->chunk;
    request->pack     = pack;
    request->receipt  = receipt;
    request->metadata = metadata;
    return state_->execute(request);
}

catch (const std::bad_alloc &) {
    return Status::resource_exhausted("physical registry command credit unavailable");
}

Status ChunkPhysicalRegistry::deleted(detail::ChunkStageId stage, detail::ChunkId chunk,
                                      const std::optional<detail::ChunkAllocationIdentity> &receipt,
                                      const std::shared_ptr<AllocationBudget>              &metadata)
try {
    if (!metadata || metadata->retirement_queue() != state_->queue) {
        return Status::invalid_argument();
    }
    auto request      = std::allocate_shared<State::Request>(MetadataAllocator<State::Request>(metadata));
    request->action   = State::Action::Delete;
    request->stage    = stage;
    request->chunk    = chunk;
    request->receipt  = receipt;
    request->metadata = metadata;
    return state_->execute(request);
}

catch (const std::bad_alloc &) {
    return Status::resource_exhausted("physical registry command credit unavailable");
}

std::shared_ptr<BackingMetricExtent>
ChunkPhysicalRegistry::extent(const std::shared_ptr<ChunkPhysicalPack> &pack) noexcept
{
    return pack ? std::shared_ptr<BackingMetricExtent>(pack, &pack->physical) : nullptr;
}
} // namespace crowdb::tree
