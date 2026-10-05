// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/btree/tree.h"

namespace crowdb::tree
{

std::optional<std::string> Crowdbtree::approximate_split_key() const
{
    auto       guard   = epoch_.enter();
    auto       page_id = root_page_id_.load(std::memory_order_acquire);
    const auto valid   = [this](Slice key) {
        return opt_.key_range.contains(key) &&
               (!opt_.key_range.start() || key.compare(Slice(*opt_.key_range.start())) > 0);
    };
    for (size_t depth = 0; depth < 8 && page_id != kInvalidPageId; ++depth) {
        PageBase *page = mapping_.get_resident(page_id);
        for (size_t delta = 0; page != nullptr && page->next != nullptr && delta < 16; ++delta) {
            page = page->next;
        }
        if (page == nullptr || page->next != nullptr) {
            return std::nullopt;
        }
        if (page->type == page_type::kInnerBase) {
            auto      *inner = static_cast<InnerBase *>(page);
            const auto view  = inner->view();
            size_t     first = 0;
            size_t     end   = view.num_separators();
            while (first < end && !valid(view.separator_at(static_cast<uint32_t>(first)))) {
                ++first;
            }
            while (first < end && !valid(view.separator_at(static_cast<uint32_t>(end - 1)))) {
                --end;
            }
            if (first < end) {
                return view.separator_at(static_cast<uint32_t>(first + ((end - first) / 2))).to_string();
            }
            page_id = inner->child_for(opt_.key_range.start() ? Slice(*opt_.key_range.start()) : Slice());
        }
        else if (page->type == page_type::kLeafBase) {
            auto  *leaf  = static_cast<LeafBase *>(page);
            size_t first = 0;
            size_t end   = leaf->count();
            while (first < end && !valid(leaf->key(first))) {
                ++first;
            }
            while (first < end && !valid(leaf->key(end - 1))) {
                --end;
            }
            return first < end ? std::optional(leaf->key(first + ((end - first) / 2)).to_string()) : std::nullopt;
        }
        else {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

} // namespace crowdb::tree
