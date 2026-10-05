// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/btree/tree.h"

namespace crowdb::tree
{
Status Crowdbtree::inspect_page(const std::vector<uint32_t> &path, uint64_t expected_version, NativeFrame *out,
                                uint64_t *out_version, uint64_t *out_root, uint32_t *out_deltas) const
{
    if (path.size() > 32 || out == nullptr || out_version == nullptr || out_root == nullptr || out_deltas == nullptr) {
        return Status::invalid_argument("page inspection path exceeds 32 levels");
    }
    auto       guard = epoch_.enter();
    const auto rev   = version();
    const auto root  = root_page_id();
    auto       id    = root;
    if (expected_version != kInvalidPageId && expected_version != rev) {
        return Status::unavailable("tree changed; restart page inspection");
    }
    std::vector<std::pair<uint64_t, PageBase *>> observed;
    for (size_t depth = 0; depth <= path.size(); ++depth) {
        PageBase *head = resident(id);
        if (head == nullptr) {
            return io_failed() ? Status::io_error("page load failed") : Status::not_found("page unavailable");
        }
        observed.emplace_back(id, head);
        PageBase *base   = head;
        uint32_t  deltas = 0;
        while (base->next != nullptr && deltas < 256) {
            base = base->next;
            ++deltas;
        }
        if (base->next != nullptr) {
            return Status::resource_exhausted("page delta chain exceeds inspection budget");
        }
        if (depth < path.size()) {
            if (base->type != page_type::kInnerBase) {
                return Status::unavailable("page path changed; restart inspection");
            }
            const auto view = static_cast<InnerBase *>(base)->view();
            if (path[depth] >= view.num_children()) {
                return Status::invalid_argument("child outside page");
            }
            id = view.child_at(path[depth]);
            continue;
        }
        const uint8_t *frame = nullptr;
        uint32_t       bytes = 0;
        if (base->type == page_type::kInnerBase) {
            const auto *inner = static_cast<InnerBase *>(base);
            frame             = inner->frame();
            bytes             = inner->page_bytes();
        }
        else if (base->type == page_type::kLeafBase) {
            const auto *leaf = static_cast<LeafBase *>(base);
            frame            = leaf->frame();
            bytes            = leaf->page_bytes();
        }
        else {
            return Status::not_supported("not a tree base page");
        }
        if (bytes > 1024 * 1024) {
            return Status::resource_exhausted("page exceeds 1 MiB inspection budget");
        }
        out->page_id = id;
        out->frame.assign(frame, frame + bytes);
        *out_deltas = deltas;
    }
    for (const auto &[page_id, head] : observed) {
        if (mapping_.get_resident(page_id) != head) {
            return Status::unavailable("page changed during inspection");
        }
    }
    if (version() != rev || root_page_id() != root) {
        return Status::unavailable("tree changed during inspection");
    }
    *out_version = rev;
    *out_root    = root;
    return Status::Ok();
}
} // namespace crowdb::tree
