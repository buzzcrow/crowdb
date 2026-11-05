// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write_internal.h"

namespace crowdb::tree
{
bool captured_write_detail::same_durable_frame(const DurableCapturedFrame &left,
                                               const DurableCapturedFrame &right) noexcept
{
    if (!left.backing || !right.backing || left.page_id != right.page_id || left.type != right.type ||
        left.frame_bytes != right.frame_bytes || left.logical_bytes != right.logical_bytes ||
        left.next_overflow != right.next_overflow || left.overflow_bytes != right.overflow_bytes) {
        return false;
    }
    if (left.backing == right.backing) {
        return true;
    }
    const auto &a = *left.backing;
    const auto &b = *right.backing;
    return a.physical_owner && a.physical_owner == b.physical_owner && a.backend_id == b.backend_id &&
           a.storage == b.storage && a.address == b.address && a.length == b.length && a.content_id == b.content_id &&
           a.generation == b.generation && a.checksum == b.checksum && a.encoding == b.encoding &&
           same_chunk_backing(a.chunk, b.chunk);
}

void CapturedWriteOperation::State::use_relocated_dependency(DurableCapturedFrame value)
{
    const auto found = pages->frames.find(value.page_id);
    if (found == pages->frames.end()) {
        begin_relocated_read(std::move(value));
        return;
    }
    const auto original = resident_sources.find(value.page_id);
    if (original == resident_sources.end() || original->second.backing != value.backing ||
        found->second.next_overflow != value.next_overflow || found->second.overflow_bytes != value.overflow_bytes ||
        found->second.frame_bytes != value.frame_bytes) {
        finish(Status::corruption("relocation encountered conflicting overflow identities"));
        return;
    }
    attach_dependency(found->second);
}

void CapturedWriteOperation::State::begin_relocated_read(DurableCapturedFrame value)
{
    PageLoadLimits loads;
    loads.max_frame_bytes = limits.write.max_frame_bytes;
    loads.max_blob_bytes =
        ((limits.write.max_frame_bytes + kDurableBlobHeader + limits.write.iu_size - 1) / limits.write.iu_size) *
        limits.write.iu_size;
    loads.max_inflight = 1;
    OverflowBackingPage source{.backing       = value.backing,
                               .next          = value.next_overflow,
                               .bytes         = value.overflow_bytes,
                               .frame_bytes   = value.frame_bytes,
                               .logical_bytes = value.logical_bytes};
    resident_sources.emplace(value.page_id, ResidentSource{.backing = value.backing, .dependencies = dependencies});
    resident_dependency = std::move(value);
    auto status = NativeFrameRead::create(resident_dependency.page_id, std::move(source), store, runtime, lane, loads,
                                          budget, &relocation_read);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    begin_child(relocation_read->sender(), Stage::RelocateRead);
}

void CapturedWriteOperation::State::received_relocated_read()
{
    const auto &raw = relocation_read->result().frame;
    if (!raw) {
        finish(Status::corruption("relocation read returned no frame"));
        return;
    }
    PageWriteInput input{
        .info                = {.page_id = resident_dependency.page_id,
                                .type    = resident_dependency.type,
                                .bytes   = resident_dependency.frame_bytes},
        .frame               = raw->bytes,
        .owner               = raw->owner,
        .reload_dependencies = dependencies
    };
    auto status = PageWriteOperation::create(store, runtime, lane, std::move(input), limits.write, budget,
                                             reserve_page(true), &write);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    begin_child(write->sender(), Stage::ResidentWrite);
}

void CapturedWriteOperation::State::reuse_durable_dependency(DurableCapturedFrame value)
{
    const auto found = pages->frames.find(value.page_id);
    if (found != pages->frames.end()) {
        if (!captured_write_detail::same_durable_frame(found->second, value)) {
            finish(Status::corruption("captured write encountered conflicting reused overflow identities"));
            return;
        }
        if (found->second.backing->owner == dependencies) {
            attach_dependency(found->second, false);
            return;
        }
    }
    if (!value.backing->physical_owner) {
        finish(Status::not_supported("dependency rebind requires independent physical extent ownership"));
        return;
    }
    auto backing   = detail::reserved_shared<BackingRef>(budget.bytes, *value.backing);
    backing->owner = dependencies;
    value.backing  = std::move(backing);
    if (found == pages->frames.end()) {
        auto status = insert(value, true);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
    }
    attach_dependency(value);
}

Status CapturedWriteOperation::State::rebind_primary()
{
    if (!descriptor.backing->physical_owner) {
        return Status::not_supported("relocation requires independent primary extent ownership");
    }
    auto backing = detail::reserved_shared<BackingRef>(budget.bytes, *descriptor.backing);
    dependencies->owners.push_back(backing->physical_owner);
    backing->owner     = dependencies;
    backing->overflow  = overflow_backing;
    descriptor.backing = std::move(backing);
    return Status::Ok();
}
} // namespace crowdb::tree
