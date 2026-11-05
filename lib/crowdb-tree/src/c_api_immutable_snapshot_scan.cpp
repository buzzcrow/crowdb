// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "c_api_future_internal.h"
#include "c_api_immutable_snapshot_internal.h"
#include "c_api_owned_budget.h"

using namespace crowdb::tree;

ct_status ct_immutable_snapshot_scan(ct_immutable_snapshot *snapshot, const ct_scan_options_v2 *options,
                                     ct_scan_result **out)
try {
    if (snapshot == nullptr || options == nullptr || options->struct_size != sizeof(*options) || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *out          = nullptr;
    const auto &o = *options;
    if ((o.prefix == nullptr && o.prefix_len != 0) || (o.start == nullptr && o.start_len != 0) ||
        (o.end == nullptr && o.end_len != 0) || (o.begin == nullptr && o.begin_len != 0)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    CoreScanOptions scan{.prefix             = Slice(o.prefix, o.prefix_len),
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
    const auto     &context = snapshot->context;
    auto            value   = make_ct_future(context->binding()->lifecycle_budget());
    value->kind             = ct_future_impl::Kind::kScan;
    value->result_controls  = c_api_result_controls(context->binding()->account());
    size_t count            = 0;
    auto   status           = context->scan(scan, &value->scan_packed, &count, &value->scan_truncated);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    value->scan_count = count;
    value->done.store(true, std::memory_order_release);
    *out = reinterpret_cast<ct_scan_result *>(make_ct_future_handle(std::move(value)));
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}
