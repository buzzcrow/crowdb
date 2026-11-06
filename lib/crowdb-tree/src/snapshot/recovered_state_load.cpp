// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/recovered_frame_identity.h"
#include "snapshot/recovered_state_internal.h"

namespace crowdb::tree
{
void RecoveredStateOperation::State::begin_load()
{
    auto event       = detail::reserved_shared<Load>(metadata);
    load             = event;
    auto        self = shared_from_this();
    const auto &top  = path.back();
    ++report.loads;
    try {
        if (top.descriptor->type == page_type::kInnerBase) {
            loader->load_inner(top.descriptor->backing, [self, event](Status status, auto inner) {
                if (!event->claimed.exchange(true, std::memory_order_acq_rel)) {
                    event->status = std::move(status);
                    event->inner  = std::move(inner);
                    event->events.fetch_or(Load::kCallback, std::memory_order_release);
                    self->wake();
                }
            });
        }
        else {
            BaseRef base{.identity   = top.pid + 1,
                         .generation = captured->generation,
                         .range      = top.range,
                         .max_slot   = captured->coverage,
                         .backing    = top.descriptor->backing};
            loader->load_leaf(std::move(base), [self, event](Status status, auto leaf) {
                if (!event->claimed.exchange(true, std::memory_order_acq_rel)) {
                    event->status = std::move(status);
                    event->leaf   = std::move(leaf);
                    event->events.fetch_or(Load::kCallback, std::memory_order_release);
                    self->wake();
                }
            });
        }
    }
    catch (...) {
        event->submission = Status::io_error();
        event->events.fetch_or(Load::kFailed, std::memory_order_release);
    }
    event->events.fetch_or(Load::kReturned, std::memory_order_release);
    wake();
}

void RecoveredStateOperation::State::loaded()
{
    auto       event = std::move(load);
    const auto flags = event->events.load(std::memory_order_acquire);
    if ((flags & Load::kFailed) != 0 || !event->status.ok()) {
        finish((flags & Load::kFailed) != 0 ? event->submission : event->status);
        return;
    }
    auto &top = path.back();
    if (top.descriptor->type == page_type::kInnerBase) {
        if (!event->inner || !event->inner->frame_proof || event->inner->children.empty() ||
            event->inner->children.size() != event->inner->separators.size() + 1 ||
            !recovered_frame_identity_matches(*top.descriptor->backing, page_type::kInnerBase,
                                              event->inner->frame_proof->self_page_id, top.pid) ||
            event->inner->frame_proof->frame_bytes != top.descriptor->frame_bytes) {
            finish(Status::corruption());
            return;
        }
        if (event->inner->children.size() > limits.max_children) {
            finish(Status::resource_exhausted());
            return;
        }
        top.inner                            = std::move(event->inner);
        top.phase                            = Phase::Inner;
        top.metrics.leaf                     = false;
        top.metrics.page_size_estimate_bytes = top.descriptor->frame_bytes;
    }
    else {
        if (!event->leaf ||
            !recovered_frame_identity_matches(*top.descriptor->backing, page_type::kLeafBase,
                                              event->leaf->view().self_page_id(), top.pid) ||
            event->leaf->page_bytes() != top.descriptor->frame_bytes || event->leaf->view().delta_count() != 0 ||
            !top.descriptor->stored_overflow) {
            finish(Status::corruption());
            return;
        }
        if (event->leaf->count() > limits.max_leaf_entries ||
            top.descriptor->stored_overflow->size() > limits.max_leaf_entries) {
            finish(Status::resource_exhausted());
            return;
        }
        if (have_leaf && previous_sibling != top.pid) {
            finish(Status::corruption());
            return;
        }
        have_leaf                   = true;
        previous_sibling            = event->leaf->right_sibling();
        top.leaf                    = std::move(event->leaf);
        top.phase                   = Phase::Leaf;
        top.metrics.page_size_known = true;
        top.metrics.page_size_bytes = kFrameHeaderSize + kFrameTrailerSize;
        top.metrics.kv_known        = true;
    }
    wake();
}

void RecoveredStateOperation::State::pump()
{
    ++report.cpu_steps;
    if (load) {
        const auto flags = load->events.load(std::memory_order_acquire);
        if ((flags & Load::kReturned) == 0 || (flags & (Load::kCallback | Load::kFailed)) == 0) {
            return;
        }
    }
    if (stopped.load(std::memory_order_acquire)) {
        finish(Status::unavailable());
        return;
    }
    if (load) {
        loaded();
        return;
    }
    if (!initialized) {
        initialized = true;
        if (captured->root_page_id != kInvalidPageId) {
            push(captured->root_page_id, {});
            return;
        }
        if (!captured->frames.empty()) {
            finish(Status::corruption());
            return;
        }
    }
    if (path.empty()) {
        verify_reachable();
        return;
    }
    switch (path.back().phase) {
    case Phase::Load:
        begin_load();
        break;
    case Phase::Inner:
        inner_step();
        break;
    case Phase::Children:
        child_step();
        break;
    case Phase::Leaf:
        leaf_step();
        break;
    case Phase::Dependencies:
        dependency_step();
        break;
    case Phase::Finish:
        complete_page();
        break;
    }
}
} // namespace crowdb::tree
