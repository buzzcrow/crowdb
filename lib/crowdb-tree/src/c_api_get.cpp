// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_internal.h"
#include "crowdb-tree/crowdb-tree.h"

#include <cstdlib>
#include <cstring>

using namespace crowdb::tree;

ct_status ct_get(ct_tree *t, const uint8_t *key, size_t klen, int32_t *found, uint64_t *slot, ct_buf *value)
try {
    if (t == nullptr || found == nullptr || (key == nullptr && klen != 0)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *found = 0;
    if (slot != nullptr) {
        *slot = 0;
    }
    if (value != nullptr) {
        *value = {};
    }
    auto  *tree         = c_api_tree(t);
    Status range_status = tree->validate_key(Slice(reinterpret_cast<const char *>(key), klen));
    if (!range_status.ok()) {
        return static_cast<ct_status>(range_status.code());
    }
    GetView    result;
    const auto status = tree->get_view(Slice(reinterpret_cast<const char *>(key), klen), &result);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    if (!result.found()) {
        return static_cast<ct_status>(Code::kOk);
    }
    if (value != nullptr) {
        const auto bytes = result.value();
        if (!bytes.empty()) {
            auto *copied = static_cast<uint8_t *>(std::malloc(bytes.size()));
            if (copied == nullptr) {
                return static_cast<ct_status>(Code::kResourceExhausted);
            }
            std::memcpy(copied, bytes.data(), bytes.size());
            *value = {.data = copied, .len = bytes.size()};
        }
    }
    if (slot != nullptr) {
        *slot = result.slot();
    }
    *found = 1;
    return static_cast<ct_status>(Code::kOk);
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}
