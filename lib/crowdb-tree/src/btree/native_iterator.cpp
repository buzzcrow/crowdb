// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/btree/delta.h"
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/maptable/mapping_slot.h"
#include "native_frames.h"

#include <algorithm>
#include <ranges>
#include <unordered_map>

namespace crowdb::tree
{
using detail::NativeBounds;
using detail::set_native_frame_fences;

struct NativeFrameIterator::Impl
{
    struct SavedPage
    {
        PageBase               *head = nullptr;
        std::vector<PageBase *> pins;
    };

    struct Task
    {
        uint64_t                   page_id = kInvalidPageId;
        std::optional<std::string> lower;
        std::optional<std::string> upper;
        bool                       is_root  = false;
        bool                       overflow = false;
    };

    ~Impl()
    {
        for (const auto &[page_id, saved] : saved_pages) {
            (void)page_id;
            for (PageBase *page : saved.pins) {
                page->unpin();
            }
        }
    }

    [[nodiscard]] bool consumed(uint64_t page_id) const
    {
        return page_id < next_page_id && (consumed_pages[page_id / 64] & (uint64_t{1} << (page_id % 64))) != 0;
    }

    [[nodiscard]] bool intersects(const Task &task) const
    {
        if (!filter.has_value() || !filter->is_bounded() || task.is_root || task.overflow) {
            return true;
        }
        if (filter->start().has_value() && filter->end().has_value() &&
            Slice(*filter->start()).compare(Slice(*filter->end())) == 0) {
            return false;
        }
        const bool below_end   = !filter->end().has_value() || !task.lower.has_value() ||
                                 Slice(*task.lower).compare(Slice(*filter->end())) < 0;
        const bool above_start = !filter->start().has_value() || !task.upper.has_value() ||
                                 Slice(*task.upper).compare(Slice(*filter->start())) > 0;
        return below_end && above_start;
    }

    void schedule(Task task)
    {
        pending_tasks.insert_or_assign(task.page_id, task);
        tasks.push_back(std::move(task));
    }

    void preserve(Crowdbtree &source, uint64_t page_id, PageBase *head)
    {
        (void)source;
        const auto pending = pending_tasks.find(page_id);
        if (!active || !terminal_status.ok() || head == nullptr || page_id >= next_page_id || consumed(page_id) ||
            saved_pages.contains(page_id) || (pending != pending_tasks.end() && !intersects(pending->second))) {
            return;
        }
        size_t chain_pages = 0;
        for (PageBase *page = head; page != nullptr; page = page->next) {
            ++chain_pages;
        }
        if (saved_pin_count + chain_pages > max_saved_pages) {
            terminal_status = Status::resource_exhausted("native frame iterator preservation budget exceeded");
            return;
        }
        SavedPage saved{.head = head, .pins = {}};
        for (PageBase *page = head; page != nullptr; page = page->next) {
            page->pin();
            saved.pins.push_back(page);
        }
        saved_pin_count += saved.pins.size();
        saved_pages.emplace(page_id, std::move(saved));
    }

    PageBase *resolve(Crowdbtree &source, uint64_t page_id) const
    {
        const auto saved = saved_pages.find(page_id);
        return saved == saved_pages.end() ? source.resident(page_id) : saved->second.head;
    }

    void consume(uint64_t page_id)
    {
        if (page_id < next_page_id) {
            consumed_pages[page_id / 64] |= uint64_t{1} << (page_id % 64);
        }
        const auto saved = saved_pages.find(page_id);
        if (saved == saved_pages.end()) {
            return;
        }
        for (PageBase *page : saved->second.pins) {
            page->unpin();
        }
        saved_pin_count -= saved->second.pins.size();
        saved_pages.erase(saved);
    }

    Crowdbtree                             *source = nullptr;
    std::optional<KeyRange>                 filter;
    std::vector<Task>                       tasks;
    std::unordered_map<uint64_t, Task>      pending_tasks;
    std::unordered_map<uint64_t, SavedPage> saved_pages;
    std::vector<NativeFrame>                owned_frames;
    bool                                    active           = true;
    size_t                                  cursor           = 0;
    size_t                                  saved_pin_count  = 0;
    size_t                                  max_saved_pages  = 1;
    uint64_t                                root_page_id     = kInvalidPageId;
    uint64_t                                at_slot          = 0;
    uint64_t                                next_page_id     = 0;
    uint64_t                                subtrees_skipped = 0;
    std::vector<uint64_t>                   consumed_pages;
    Status                                  terminal_status;
};

NativeFrameIterator::NativeFrameIterator(std::shared_ptr<Impl> impl) : impl_(std::move(impl))
{
}

NativeFrameIterator::~NativeFrameIterator()                                          = default;
NativeFrameIterator::NativeFrameIterator(NativeFrameIterator &&) noexcept            = default;
NativeFrameIterator &NativeFrameIterator::operator=(NativeFrameIterator &&) noexcept = default;

Status NativeFrameIterator::next(size_t max_frames, std::vector<NativeFrame> *out, bool *complete)
{
    if (impl_ == nullptr || out == nullptr || complete == nullptr || max_frames == 0) {
        return Status::invalid_argument("native frame iterator requires a non-empty output batch");
    }
    out->clear();
    if (!impl_->owned_frames.empty()) {
        const size_t total = impl_->owned_frames.size();
        const size_t end   = std::min(total, impl_->cursor + std::min(max_frames, total - impl_->cursor));
        out->reserve(end - impl_->cursor);
        while (impl_->cursor < end) {
            out->push_back(std::move(impl_->owned_frames[impl_->cursor++]));
        }
        *complete = impl_->cursor == total;
        return Status::Ok();
    }
    if (impl_->source == nullptr) {
        if (impl_->terminal_status.ok()) {
            impl_->terminal_status = Status::internal_error("native frame iterator source no longer exists");
        }
        return impl_->terminal_status;
    }

    Crowdbtree      &source = *impl_->source;
    std::scoped_lock lock(source.write_mutex_);
    if (!impl_->terminal_status.ok()) {
        return impl_->terminal_status;
    }
    out->reserve(max_frames);
    while (out->size() < max_frames && !impl_->tasks.empty()) {
        Impl::Task task = std::move(impl_->tasks.back());
        impl_->tasks.pop_back();
        impl_->pending_tasks.erase(task.page_id);
        if (!impl_->intersects(task)) {
            ++impl_->subtrees_skipped;
            impl_->consume(task.page_id);
            continue;
        }
        PageBase *page = impl_->resolve(source, task.page_id);
        if (page == nullptr) {
            impl_->terminal_status = Status::internal_error("native frame iterator encountered a missing page");
            return impl_->terminal_status;
        }
        if (page->type == page_type::kBatchDelta ||
            (page->type == page_type::kLeafBase && static_cast<LeafBase *>(page)->view().delta_count() != 0)) {
            impl_->terminal_status =
                Status::internal_error("native frame iterator requires a flushed immutable source generation");
            return impl_->terminal_status;
        }

        const uint8_t *bytes  = nullptr;
        uint32_t       length = 0;
        if (page->type == page_type::kLeafBase) {
            auto *leaf = static_cast<LeafBase *>(page);
            bytes      = leaf->frame();
            length     = leaf->page_bytes();
            std::vector<uint64_t> overflow_heads;
            LeafFrameView         view = leaf->view();
            for (uint32_t index = 0; index < view.count(); ++index) {
                CellView cell{view.cell(index)};
                if (cell.is_overflow()) {
                    overflow_heads.push_back(cell.overflow_head());
                }
            }
            for (unsigned long &overflow_head : std::views::reverse(overflow_heads)) {
                impl_->schedule(
                    {.page_id = overflow_head, .lower = std::nullopt, .upper = std::nullopt, .overflow = true});
            }
        }
        else if (page->type == page_type::kInnerBase) {
            auto *inner         = static_cast<InnerBase *>(page);
            bytes               = inner->frame();
            length              = inner->page_bytes();
            InnerFrameView view = inner->view();
            for (uint32_t reverse = view.num_children(); reverse != 0; --reverse) {
                const uint32_t index = reverse - 1;
                impl_->schedule({
                    .page_id = view.child_at(index),
                    .lower =
                        index == 0 ? task.lower : std::optional<std::string>(view.separator_at(index - 1).to_string()),
                    .upper = index == view.num_separators()
                               ? task.upper
                               : std::optional<std::string>(view.separator_at(index).to_string()),
                });
            }
        }
        else if (page->type == page_type::kOverflowFrame) {
            auto *overflow = static_cast<OverflowBase *>(page);
            bytes          = overflow->frame();
            length         = overflow->page_bytes();
            if (overflow->next_page_id() != kInvalidPageId) {
                impl_->schedule({
                    .page_id  = overflow->next_page_id(),
                    .lower    = std::nullopt,
                    .upper    = std::nullopt,
                    .overflow = true,
                });
            }
        }
        else {
            impl_->terminal_status =
                Status::internal_error("native frame iterator encountered an unexpected page type");
            return impl_->terminal_status;
        }
        out->push_back({
            .page_id      = task.page_id,
            .frame        = std::vector<uint8_t>(bytes, bytes + length),
            .durable_addr = page->durable_addr,
            .durable_plen = page->durable_plen,
        });
        impl_->consume(task.page_id);
    }
    *complete = impl_->tasks.empty();
    if (*complete) {
        impl_->active = false;
    }
    return Status::Ok();
}

uint64_t NativeFrameIterator::root_page_id() const
{
    return impl_ == nullptr ? kInvalidPageId : impl_->root_page_id;
}

uint64_t NativeFrameIterator::at_slot() const
{
    return impl_ == nullptr ? 0 : impl_->at_slot;
}

uint64_t NativeFrameIterator::next_page_id() const
{
    return impl_ == nullptr ? 0 : impl_->next_page_id;
}

uint64_t NativeFrameIterator::subtrees_skipped() const
{
    return impl_ == nullptr ? 0 : impl_->subtrees_skipped;
}

void Crowdbtree::preserve_native_page_locked(uint64_t page_id, PageBase *page)
{
    std::erase_if(native_frame_iterators_, [](const auto &iterator) { return iterator.expired(); });
    for (const std::weak_ptr<NativeFrameIterator::Impl> &weak : native_frame_iterators_) {
        if (auto iterator = weak.lock()) {
            iterator->preserve(*this, page_id, page);
        }
    }
}

Status Crowdbtree::open_native_frame_iterator(const KeyRange *filter, std::unique_ptr<NativeFrameIterator> *out)
{
    if (out == nullptr) {
        return Status::invalid_argument("native frame iterator requires an output");
    }
    out->reset();
    if (filter != nullptr) {
        Status range_status = filter->validate();
        if (!range_status.ok()) {
            return range_status;
        }
    }
    if (filter != nullptr && !routing_fences_trusted_.load(std::memory_order_acquire)) {
        auto     impl    = std::make_shared<NativeFrameIterator::Impl>();
        uint64_t skipped = 0;
        Status   status  = collect_native_frames(&impl->owned_frames, &impl->root_page_id, &impl->at_slot,
                                                 &impl->next_page_id, filter, &skipped);
        if (!status.ok()) {
            return status;
        }
        impl->subtrees_skipped = skipped;
        *out                   = std::unique_ptr<NativeFrameIterator>(new NativeFrameIterator(std::move(impl)));
        return Status::Ok();
    }

    std::scoped_lock lk(write_mutex_);
    if (publication_incomplete_) {
        return Status::unavailable("flush publication requires repair");
    }
    const uint64_t                  gc      = gc_floor_.load();
    std::function<Status(uint64_t)> prepare = [&](uint64_t page_id) -> Status {
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            return Status::internal_error("native frame iterator: missing page during preparation");
        }
        const bool has_inframe_deltas =
            head->type == page_type::kLeafBase && static_cast<LeafBase *>(head)->view().delta_count() != 0;
        if (head->type == page_type::kBatchDelta || has_inframe_deltas) {
            PageBase *base = head;
            while (base != nullptr && base->type == page_type::kBatchDelta) {
                base = base->next;
            }
            if (base == nullptr || base->type != page_type::kLeafBase) {
                return Status::internal_error("native frame iterator: delta chain without leaf base");
            }
            const uint64_t        right = static_cast<LeafBase *>(base)->right_sibling();
            std::vector<uint64_t> dead_overflow;
            LeafBase             *fresh =
                build_leaf_spilling_locked(resolve_leaf_chain_for_rebuild(head, gc, &dead_overflow), right);
            store_preserving_parent_locked(page_id, fresh);
            for (PageBase *node = head; node != nullptr;) {
                PageBase *next = node->next;
                retire_page(node);
                node = next;
            }
            for (uint64_t overflow_head : dead_overflow) {
                retire_overflow_chain_locked(overflow_head);
            }
            head = fresh;
        }
        if (head->type != page_type::kInnerBase) {
            return head->type == page_type::kLeafBase ? Status::Ok()
                                                      : Status::internal_error("native frame iterator: bad base page");
        }
        for (uint64_t child : static_cast<InnerBase *>(head)->children()) {
            Status child_status = prepare(child);
            if (!child_status.ok()) {
                return child_status;
            }
        }
        return Status::Ok();
    };
    const uint64_t prepared_root  = root_page_id_.load();
    Status         prepare_status = prepare(prepared_root);
    if (!prepare_status.ok()) {
        return prepare_status;
    }
    auto state    = std::make_shared<NativeFrameIterator::Impl>();
    state->source = this;
    if (filter != nullptr) {
        state->filter = *filter;
    }
    state->root_page_id    = prepared_root;
    state->next_page_id    = mapping_.next_page_id();
    state->max_saved_pages = std::max<size_t>(1, (4U * 1024U * 1024U) / opt_.frame_bytes);
    state->consumed_pages.resize((state->next_page_id + 63) / 64);
    PageBase *root = resident(state->root_page_id);
    if (root == nullptr) {
        return Status::internal_error("native frame iterator: missing root page");
    }
    state->preserve(*this, state->root_page_id, root);
    state->schedule({.page_id = state->root_page_id, .lower = std::nullopt, .upper = std::nullopt, .is_root = true});
    native_frame_iterators_.push_back(state);
    state->at_slot = last_applied_slot_.load();
    *out           = std::unique_ptr<NativeFrameIterator>(new NativeFrameIterator(std::move(state)));
    return Status::Ok();
}

void Crowdbtree::detach_native_iterators()
{
    for (const std::weak_ptr<NativeFrameIterator::Impl> &weak : native_frame_iterators_) {
        if (auto iterator = weak.lock()) {
            iterator->source = nullptr;
        }
    }
}

void Crowdbtree::preserve_native_generation_locked()
{
    std::erase_if(native_frame_iterators_, [](const auto &iterator) { return iterator.expired(); });
    if (native_frame_iterators_.empty()) {
        return;
    }
    for (uint64_t segment_index = 0; segment_index < MappingTable::kMaxSegments; ++segment_index) {
        auto *segment = mapping_.segment_at(segment_index);
        if (segment == nullptr) {
            continue;
        }
        for (uint64_t index = 0; index < MappingTable::kSegmentSize; ++index) {
            if (slot_word::is_empty(segment->slots[index].load())) {
                continue;
            }
            const auto page_id = (segment_index * MappingTable::kSegmentSize) + index;
            preserve_native_page_locked(page_id, resident(page_id));
        }
    }
}
} // namespace crowdb::tree
