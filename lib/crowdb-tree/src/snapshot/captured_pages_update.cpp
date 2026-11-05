// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write.h"

namespace crowdb::tree
{
Status CapturedDurablePages::initialize_overflow_counts()
{
    if (overflow_counts_known) {
        return Status::Ok();
    }
    stored_reference_count = 0;
    for (const auto &[pid, frame] : std::as_const(frames)) {
        (void)pid;
        if (frame.type == page_type::kOverflowFrame) {
            if (frame.next_overflow != kInvalidPageId) {
                if (!frames.contains(frame.next_overflow)) {
                    return Status::corruption("missing overflow successor");
                }
                ++frames.at(frame.next_overflow).incoming_references;
            }
        }
        else if (frame.stored_overflow) {
            stored_reference_count += frame.stored_overflow->size();
            for (const auto &head : *frame.stored_overflow) {
                if (!frames.contains(head.head)) {
                    return Status::corruption("missing overflow head");
                }
                ++frames.at(head.head).incoming_references;
            }
        }
    }
    overflow_counts_known = true;
    frames.refresh_counts();
    return Status::Ok();
}

Status CapturedDurablePages::reference(uint64_t pid, bool add)
{
    while (pid != kInvalidPageId) {
        if (!frames.contains(pid)) {
            return Status::corruption("missing referenced overflow");
        }
        auto &frame = frames.at(pid);
        if (frame.type != page_type::kOverflowFrame ||
            (add ? frame.incoming_references == UINT64_MAX : frame.incoming_references == 0)) {
            return Status::corruption("invalid overflow reference count");
        }
        const auto next = frame.next_overflow;
        if (add) {
            if (frame.incoming_references++ != 0) {
                return Status::Ok();
            }
        }
        else {
            if (--frame.incoming_references != 0) {
                return Status::Ok();
            }
            frames.erase(pid);
        }
        pid = next;
    }
    return Status::Ok();
}

Status CapturedDurablePages::erase_primary(uint64_t pid)
{
    const auto found = std::as_const(frames).find(pid);
    if (found == frames.end()) {
        return Status::Ok();
    }
    if (found->second.type == page_type::kOverflowFrame) {
        return Status::corruption("logical PID names overflow");
    }
    auto references = found->second.stored_overflow;
    stored_reference_count -= references ? references->size() : 0;
    frames.erase(pid);
    if (references) {
        for (const auto &head : *references) {
            auto status = reference(head.head, false);
            if (!status.ok()) {
                return status;
            }
        }
    }
    return Status::Ok();
}

Status CapturedDurablePages::replace_primary(const CapturedDurablePages &page)
{
    const DurableCapturedFrame *primary = nullptr;
    for (const auto &[pid, frame] : page.frames) {
        if (frame.type == page_type::kOverflowFrame) {
            if (!frames.contains(pid)) {
                auto added                = frame;
                added.incoming_references = 0;
                frames.emplace(pid, std::move(added));
            }
            else if (std::as_const(frames).at(pid).backing != frame.backing) {
                const auto &old = std::as_const(frames).at(pid);
                if (old.type != frame.type || old.frame_bytes != frame.frame_bytes ||
                    old.next_overflow != frame.next_overflow || old.overflow_bytes != frame.overflow_bytes) {
                    return Status::corruption("staged overflow changed its immutable PID contents");
                }
                auto updated                = frame;
                updated.incoming_references = old.incoming_references;
                frames.insert_or_assign(pid, std::move(updated));
            }
        }
        else {
            if (primary != nullptr) {
                return Status::invalid_argument("expected one final primary");
            }
            primary = &frame;
        }
    }
    if (primary == nullptr) {
        return Status::corruption("missing final primary");
    }
    if (primary->stored_overflow) {
        for (const auto &head : *primary->stored_overflow) {
            auto status = reference(head.head, true);
            if (!status.ok()) {
                return status;
            }
        }
    }
    auto status = erase_primary(primary->page_id);
    if (!status.ok()) {
        return status;
    }
    frames.insert_or_assign(primary->page_id, *primary);
    stored_reference_count += primary->stored_overflow ? primary->stored_overflow->size() : 0;
    return Status::Ok();
}
} // namespace crowdb::tree
