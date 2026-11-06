// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_future_internal.h"
#include "c_api_internal.h"
#include "c_api_owned_budget.h"

#include <optional>

using namespace crowdb::tree;

ct_status ct_future_poll_scan_v2(ct_future *future, int32_t *done, ct_scan_result **out)
{
    if (future == nullptr || done == nullptr || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *done              = 0;
    *out               = nullptr;
    auto       *handle = reinterpret_cast<ct_future_handle *>(future);
    const auto &impl   = **handle;
    if (impl.kind != ct_future_impl::Kind::kScan) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    if (!impl.done.load(std::memory_order_acquire)) {
        return 0;
    }
    *done             = 1;
    const auto status = impl.status;
    if (status == 0) {
        *out = reinterpret_cast<ct_scan_result *>(future);
    }
    else {
        ct_future_free(future);
    }
    return status;
}

ct_status ct_scan_result_data(const ct_scan_result *result, const uint8_t **data, size_t *len, uint64_t *count,
                              int32_t *truncated)
{
    if (result == nullptr || data == nullptr || len == nullptr || count == nullptr || truncated == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    const auto &impl = **reinterpret_cast<const ct_future_handle *>(result);
    if (impl.kind != ct_future_impl::Kind::kScan || !impl.done.load(std::memory_order_acquire) || impl.status != 0) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *data      = impl.scan_packed.data();
    *len       = impl.scan_packed.size();
    *count     = impl.scan_count;
    *truncated = impl.scan_truncated ? 1 : 0;
    return 0;
}

void ct_scan_result_free(ct_scan_result *result)
{
    ct_future_free(reinterpret_cast<ct_future *>(result));
}

namespace
{
ct_status prepare_scan(ct_tree *tree, const ct_scan_options_v2 *options, CoreScanOptions *scan, TreeCore **core)
{
    if (tree == nullptr || options == nullptr || options->struct_size != sizeof(*options)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    const auto &o = *options;
    if ((o.prefix == nullptr && o.prefix_len != 0) || (o.start == nullptr && o.start_len != 0) ||
        (o.end == nullptr && o.end_len != 0) || (o.begin == nullptr && o.begin_len != 0) ||
        (o.strict_byte_budget != 0 && o.byte_budget == 0)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (*core == nullptr) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    *scan = {.prefix             = Slice(o.prefix, o.prefix_len),
             .start              = Slice(o.start, o.start_len),
             .end                = Slice(o.end, o.end_len),
             .begin              = Slice(o.begin, o.begin_len),
             .limit              = o.limit,
             .byte_budget        = o.byte_budget,
             .deadline_ms        = o.deadline_ms,
             .reverse            = o.reverse != 0,
             .keys_only          = o.keys_only != 0,
             .include_tombstones = o.include_tombstones != 0,
             .has_start_bound    = o.has_start_bound != 0,
             .start_inclusive    = o.start_inclusive != 0,
             .strict_byte_budget = o.strict_byte_budget != 0};
    return 0;
}

// Validate the internal packed record framing while counting the bounded page.
std::optional<size_t> count_entries(const ScanPackedBuf &packed)
{
    size_t     offset      = 0;
    size_t     count       = 0;
    const auto skip_string = [&] {
        if (packed.size() - offset < 4) {
            return false;
        }
        uint32_t length = 0;
        for (size_t i = 0; i < 4; ++i) {
            length |= uint32_t{packed.data()[offset + i]} << (8 * i);
        }
        offset += 4;
        if (length > packed.size() - offset) {
            return false;
        }
        offset += length;
        return true;
    };
    while (offset != packed.size()) {
        if (!skip_string() || packed.size() - offset < 9) {
            return {};
        }
        offset += 9;
        if (!skip_string()) {
            return {};
        }
        ++count;
    }
    return count;
}
} // namespace

ct_status ct_scan_v2(ct_tree *tree, const ct_scan_options_v2 *options, ct_scan_result **out)
try {
    if (out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *out = nullptr;
    CoreScanOptions scan{};
    TreeCore       *core = nullptr;
    if (auto status = prepare_scan(tree, options, &scan, &core); status != 0) {
        return status;
    }
    auto impl             = make_ct_future(core->binding()->lifecycle_budget());
    impl->kind            = ct_future_impl::Kind::kScan;
    impl->result_controls = c_api_result_controls(core->binding()->account());
    size_t count          = 0;
    auto   status         = core->scan(scan, nullptr, &impl->scan_truncated, &impl->scan_packed, &count);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    impl->scan_count = count;
    impl->done.store(true, std::memory_order_release);
    *out = reinterpret_cast<ct_scan_result *>(make_ct_future_handle(std::move(impl)));
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

ct_status ct_scan_async_v2(ct_tree *tree, const ct_scan_options_v2 *options, ct_future **out)
try {
    if (out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *out = nullptr;
    CoreScanOptions scan{};
    TreeCore       *core = nullptr;
    if (auto status = prepare_scan(tree, options, &scan, &core); status != 0) {
        return status;
    }
    auto impl             = make_ct_future(core->binding()->lifecycle_budget());
    impl->kind            = ct_future_impl::Kind::kScan;
    impl->result_controls = c_api_result_controls(core->binding()->account());
    std::unique_ptr<ct_future_handle, decltype(&free_ct_future_handle)> handle(make_ct_future_handle(impl),
                                                                               free_ct_future_handle);
    core->scan_async(
        scan, [impl, signal = c_api_completion_signal(tree)](Status status, ScanPackedBuf packed, bool truncated) {
            impl->status = static_cast<ct_status>(status.code());
            if (status.ok()) {
                const auto count = count_entries(packed);
                if (count) {
                    impl->scan_count     = *count;
                    impl->scan_packed    = std::move(packed);
                    impl->scan_truncated = truncated;
                }
                else {
                    impl->status = static_cast<ct_status>(Code::kCorruption);
                }
            }
            impl->done.store(true, std::memory_order_release);
            c_api_notify_completion(signal);
        });
    *out = reinterpret_cast<ct_future *>(handle.release());
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}
