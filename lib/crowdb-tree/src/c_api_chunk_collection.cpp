// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_collector.h"
#include "c_api_maintenance_internal.h"

namespace crowdb::tree::c_api_detail
{
void store_report(ct_future_impl &result, const detail::ChunkCollectionProgress &report,
                  const Status &terminal) noexcept
{
    result.maintenance = ct_chunk_collection_result_v1{.struct_size            = sizeof(ct_chunk_collection_result_v1),
                                                       .version                = 1,
                                                       .terminal_status        = static_cast<int32_t>(terminal.code()),
                                                       .more                   = report.more ? 1U : 0U,
                                                       .candidates_examined    = report.candidates_examined,
                                                       .reference_windows      = report.reference_windows,
                                                       .stages_examined        = report.stages_examined,
                                                       .pins_examined          = report.pins_examined,
                                                       .pins_released          = report.pins_released,
                                                       .stages_removed         = report.stages_removed,
                                                       .logical_capacity_bytes = report.logical_capacity_bytes};
}
} // namespace crowdb::tree::c_api_detail

ct_status ct_future_poll_chunk_collection(ct_future *future, int32_t *done, ct_chunk_collection_result_v1 *out)
{
    using namespace crowdb::tree;
    if (future == nullptr || done == nullptr || out == nullptr || out->version != 1 ||
        out->struct_size != sizeof(*out)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto       *handle = reinterpret_cast<ct_future_handle *>(future);
    const auto &result = **handle;
    if (result.kind != ct_future_impl::Kind::kChunkCollection) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    if (!result.done.load(std::memory_order_acquire)) {
        *done = 0;
        return 0;
    }
    const auto *report = std::get_if<ct_chunk_collection_result_v1>(&result.maintenance);
    if (report == nullptr) {
        return static_cast<ct_status>(Code::kInternal);
    }
    *out  = *report;
    *done = 1;
    free_ct_future_handle(handle);
    return 0;
}
