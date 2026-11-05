// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write_internal.h"

namespace crowdb::tree
{
void CapturedWriteOperation::State::attach_dependency(const DurableCapturedFrame &value, bool retain_owner)
{
    if (retain_owner) {
        dependencies->owners.push_back(value.backing->physical_owner ? value.backing->physical_owner
                                                                     : value.backing->owner);
    }
    overflow_backing->pages.emplace(dependency_pid, OverflowBackingPage{.backing       = value.backing,
                                                                        .next          = value.next_overflow,
                                                                        .bytes         = value.overflow_bytes,
                                                                        .frame_bytes   = value.frame_bytes,
                                                                        .logical_bytes = value.logical_bytes});
    dependency_bytes -= value.overflow_bytes;
    dependency_pid = value.next_overflow;
    if (dependency_bytes == 0) {
        ++dependency_index;
    }
    stage = Stage::Dependencies;
    wake();
}

void CapturedWriteOperation::State::use_resident_dependency(DurableCapturedFrame value)
{
    const auto written = pages->frames.find(value.page_id);
    if (written == pages->frames.end()) {
        begin_resident_write(std::move(value));
        return;
    }
    const auto  original = resident_sources.find(value.page_id);
    const auto &backing  = value.backing;
    if (!backing || original == resident_sources.end() || !backing->readable() ||
        original->second.backing->owner.owner_before(backing->owner) ||
        backing->owner.owner_before(original->second.backing->owner) ||
        original->second.backing->resident_frame.data() != backing->resident_frame.data() ||
        original->second.backing->resident_frame.size() != backing->resident_frame.size() ||
        written->second.type != value.type || written->second.next_overflow != value.next_overflow ||
        written->second.overflow_bytes != value.overflow_bytes || written->second.frame_bytes != value.frame_bytes) {
        finish(Status::corruption("resident overflow PID conflicts with an already written frame"));
        return;
    }
    // A frame written in this group already contributed its extent lease.
    // Retaining its BackingRef owner would create a dependency cycle.
    attach_dependency(written->second, original->second.dependencies != dependencies);
}

void CapturedWriteOperation::State::begin_resident_write(DurableCapturedFrame value)
{
    const auto &backing = value.backing;
    if (!backing || !backing->readable() || backing->storage != BackingStorage::kResidentFrame ||
        backing->resident_frame.size() != value.frame_bytes || value.frame_bytes > limits.write.max_frame_bytes ||
        !frame_validate(backing->resident_frame.data(), value.frame_bytes)) {
        finish(Status::corruption("resident overflow frame has no valid independent raw owner"));
        return;
    }
    const OverflowFrameView raw(backing->resident_frame.data(), value.frame_bytes);
    if (frame_page_type(backing->resident_frame.data()) != page_type::kOverflowFrame ||
        raw.self_page_id() != value.page_id || raw.next_page_id() != value.next_overflow ||
        raw.chunk_len() != value.overflow_bytes) {
        finish(Status::corruption("resident overflow descriptor differs from its frame"));
        return;
    }
    PageWriteInput input{
        .info                = {.page_id = value.page_id, .type = value.type, .bytes = value.frame_bytes},
        .frame               = backing->resident_frame,
        .owner               = backing->owner,
        .reload_dependencies = dependencies
    };
    resident_sources.emplace(value.page_id, ResidentSource{.backing = backing, .dependencies = dependencies});
    resident_dependency = std::move(value);
    auto status         = PageWriteOperation::create(store, runtime, lane, std::move(input), limits.write, budget,
                                                     reserve_page(true), &write);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    begin_child(write->sender(), Stage::ResidentWrite);
}

void CapturedWriteOperation::State::received_resident_write()
{
    const auto &result                = write->result();
    resident_dependency.backing       = result.backing;
    resident_dependency.logical_bytes = result.logical_bytes;
    auto status                       = insert(resident_dependency, false);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    ++report.written_frames;
    report.written_bytes += result.physical_bytes;
    attach_dependency(resident_dependency, false);
    write.reset();
    relocation_read.reset();
    resident_dependency.backing.reset();
}
} // namespace crowdb::tree
