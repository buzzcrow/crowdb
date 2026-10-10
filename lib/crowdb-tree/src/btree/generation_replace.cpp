// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/btree/delta.h"
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/maptable/mapping_slot.h"
#include "native_frames.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
// Owns either a private candidate or the detached previous mapping. All
// retirement bookkeeping exists before the mapping publication can succeed.
struct MappingGeneration : EpochManager::Deferred
{
    MappingTable mapping;

    MappingGeneration()
        : EpochManager::Deferred{
              .destroy = [](EpochManager::Deferred *entry) noexcept { delete static_cast<MappingGeneration *>(entry); }}
    {
    }

    ~MappingGeneration()
    {
        for (uint64_t index = 0; index < MappingTable::kMaxSegments; ++index) {
            auto *segment = mapping.segment_at(index);
            if (segment == nullptr) {
                continue;
            }
            for (uint64_t slot = 0; slot < MappingTable::kSegmentSize; ++slot) {
                const auto word = segment->slots[slot].load();
                if (!slot_word::is_resident(word)) {
                    continue;
                }
                for (auto *page = slot_word::resident_ptr(word); page != nullptr;) {
                    auto *next = page->next;
                    page->retire_with_pins();
                    page = next;
                }
            }
        }
    }
};

PageBase *copy_native(const NativeFrame &frame, const std::shared_ptr<BufferPool> &pool, uint32_t frame_bytes)
{
    const auto *data = frame.frame.data();
    const auto  size = static_cast<uint32_t>(frame.frame.size());
    switch (frame_page_type(data)) {
    case page_type::kLeafBase:
        return LeafBase::from_frame_copy(data, size, pool, frame_bytes);
    case page_type::kInnerBase:
        return InnerBase::from_frame_copy(data, size, pool, frame_bytes);
    case page_type::kOverflowFrame:
        return OverflowBase::from_frame_copy(data, size, pool, frame_bytes);
    default:
        return nullptr;
    }
}

Status inherit_mapping(const MappingTable &source, MappingTable &destination, PageStore &store)
{
    for (uint64_t index = 0; index < MappingTable::kMaxSegments; ++index) {
        auto *segment = source.segment_at(index);
        if (segment == nullptr) {
            continue;
        }
        std::vector<uint64_t> words(MappingTable::kSegmentSize);
        for (uint64_t slot = 0; slot < words.size(); ++slot) {
            auto word = segment->slots[slot].load();
            if (slot_word::is_resident(word)) {
                auto *page = slot_word::resident_ptr(word);
                if (page->durable_addr == kNoAddr || page->durable_plen == 0) {
                    return Status::invalid_argument("inherited mapping must refer to persisted pages");
                }
                auto status = store.encode_mapping_location(page->durable_addr, page->durable_plen, &word);
                if (!status.ok()) {
                    return status;
                }
            }
            words[slot] = word;
        }
        destination.install_recovered_segment(index, segment->generation.load(), segment->live_count.load(), words,
                                              segment->image_addr, segment->image_len, segment->image_crc);
        auto *copy = destination.segment_at(index);
        copy->write_seq.store(segment->write_seq.load());
        copy->persisted_seq.store(segment->persisted_seq.load());
    }
    return Status::Ok();
}

void assign_parents(MappingTable &mapping, const std::vector<NativeFrame> &frames)
{
    for (const auto &frame : frames) {
        if (frame_page_type(frame.frame.data()) != page_type::kInnerBase) {
            continue;
        }
        InnerFrameView view(frame.frame.data(), static_cast<uint32_t>(frame.frame.size()));
        for (uint32_t i = 0; i < view.num_children(); ++i) {
            if (auto *child = mapping.get_resident(view.child_at(i)); child != nullptr) {
                child->parent_page_id = frame.page_id;
            }
        }
    }
}
} // namespace

Status Crowdbtree::install_snapshot_native(std::vector<NativeFrame> frames, uint64_t root_page_id, uint64_t at_slot,
                                           uint64_t next_page_id)
{
    return install_range_snapshot_native(std::move(frames), root_page_id, at_slot, next_page_id, false);
}

Status Crowdbtree::install_range_snapshot_native(std::vector<NativeFrame> frames, uint64_t root_page_id,
                                                 uint64_t at_slot, uint64_t next_page_id, bool mapping_inherited)
try {
    acquire_snapshot_slot();
    auto release_snapshot = [this](Crowdbtree *) { release_snapshot_slot(); };
    std::unique_ptr<Crowdbtree, decltype(release_snapshot)> snapshot_slot(this, release_snapshot);
    std::scoped_lock                                        writer(write_mutex_);
    for (const auto &frame : frames) {
        if (frame.page_id == kInvalidPageId || frame.frame.empty() ||
            !frame_validate_key_range(frame.frame.data(), static_cast<uint32_t>(frame.frame.size()), opt_.key_range)) {
            return Status::corruption("native snapshot: frame CRC, structure, or range invalid");
        }
    }
    auto status = detail::validate_native_snapshot_graph(&frames, root_page_id);
    if (!status.ok()) {
        return status;
    }
    auto staged = std::make_unique<MappingGeneration>();
    if (mapping_inherited) {
        status = inherit_mapping(mapping_, staged->mapping, *opt_.page_store);
        if (!status.ok()) {
            return status;
        }
    }
    uint64_t leaves = 0;
    uint64_t inners = 0;
    uint64_t next   = next_page_id;
    bool all_frames_inherited = mapping_inherited;
    for (const auto &frame : frames) {
        all_frames_inherited = all_frames_inherited && frame.inherited;
        next            = std::max(next, frame.page_id + 1);
        const auto type = frame_page_type(frame.frame.data());
        leaves += static_cast<uint64_t>(type == page_type::kLeafBase);
        inners += static_cast<uint64_t>(type == page_type::kInnerBase);
        uint64_t word = slot_word::kEmpty;
        if (mapping_inherited && frame.inherited && frame.durable_addr != kNoAddr && frame.durable_plen != 0 &&
            opt_.page_store->encode_mapping_location(frame.durable_addr, frame.durable_plen, &word).ok() &&
            staged->mapping.get_word(frame.page_id) == word) {
            continue;
        }
        std::unique_ptr<PageBase> page(copy_native(frame, pool_, opt_.frame_bytes));
        if (page == nullptr) {
            return Status::corruption("native snapshot: unknown frame type");
        }
        staged->mapping.store(frame.page_id, page.get());
        [[maybe_unused]] auto *published = page.release();
    }
    staged->mapping.set_next_page_id(next);
    assign_parents(staged->mapping, frames);
    auto successor = std::make_shared<MemTable>(memtable_next_id_.fetch_add(1), &epoch_);
    successor->set_durable_floor(at_slot);
    preserve_native_generation_locked();
    GenerationGate::Replacement replacement(generation_);
    std::unique_lock            catalog(memtable_mutex_);
    active_->close();
    for (const auto &table : frozen_) {
        table->close();
    }
    for (const auto &table : split_shared_memtables_) {
        table->close();
    }
    // Generation admission has drained all old batches before slots/mappings
    // change. The remaining publication consists only of non-allocating swaps.
    mapping_.swap_quiescent(staged->mapping);
    if (all_frames_inherited) {
        for (uint64_t segment_index = 0; segment_index < MappingTable::kMaxSegments; ++segment_index) {
            auto *segment = mapping_.segment_at(segment_index);
            if (segment != nullptr) {
                segment->persisted_seq.store(segment->write_seq.load(std::memory_order_relaxed),
                                             std::memory_order_relaxed);
            }
        }
    }
    root_page_id_.store(root_page_id);
    active_ = std::move(successor);
    frozen_.clear();
    split_shared_memtables_.clear();
    split_overlay_source_.store(nullptr);
    split_overlay_frontier_.store(0);
    leaf_count_.store(leaves);
    inner_count_.store(inners);
    last_applied_slot_.store(at_slot);
    contiguous_slot_.store(at_slot);
    gc_floor_.store(0);
    auto_slot_.store(at_slot);
    received_slots_.clear();
    max_seen_slot_ = at_slot;
    version_.fetch_add(1);
    routing_fences_trusted_.store(true);
    publication_incomplete_ = false;
    epoch_.defer(staged.release());
    epoch_.try_reclaim();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("operation allocation failed");
}
catch (const std::exception &error) {
    return Status::internal_error(error.what());
}

Status Crowdbtree::clear()
{
    return install_snapshot({}, 0);
}
} // namespace crowdb::tree
