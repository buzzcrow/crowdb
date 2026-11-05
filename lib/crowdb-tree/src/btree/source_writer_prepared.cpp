// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/external_allocation.h"
#include "source_admission.h"

#include <cassert>

namespace crowdb::tree
{
Status SourceAdmission::Writer::prepare_write(Slice key, uint8_t flags, buffer value, Prepared *out)
try {
    if (!table_ || !credit_ || out == nullptr || out->table_) {
        return Status::invalid_argument("prepared write needs an admitted permit and empty output");
    }
    const auto    *external = value.external_allocation();
    const bool     split    = external != nullptr && external->charged_to(credit_);
    const CellView cell(value.slice());
    if (!split && (!value.owned() || !cell.valid())) {
        return Status::invalid_argument("prepared write needs owned encoded bytes or a charged external value");
    }
    const size_t capacity = split || value.inlined() ? 0 : value.capacity();
    if (planned_.entries >= remaining_.entries || key.size() > remaining_.key_bytes - planned_.key_bytes ||
        capacity > remaining_.payload_bytes - planned_.payload_bytes ||
        key.size() > source_budget_->options().max_key_size ||
        (remaining_.max_key_size != 0 && key.size() > remaining_.max_key_size)) {
        return Status::invalid_argument("prepared write exceeds its admitted footprint");
    }
    const auto &options    = source_budget_->options();
    const auto  cell_flags = split ? flags : cell.flags();
    auto        prepared   = table_->prepare_write(key, std::move(value), cell_flags,
                                                   AllocationLease(credit_, capacity, AllocationKind::kPayload), bound_,
                                                   options.max_entries + options.max_gap_entries, credit_);
    out->table_            = table_;
    out->write_            = std::move(prepared);
    out->footprint_        = {.entries = 1, .key_bytes = key.size(), .payload_bytes = capacity};
    ++planned_.entries;
    planned_.key_bytes += key.size();
    planned_.payload_bytes += capacity;
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("admitted batch preparation allocation failed");
}

bool SourceAdmission::Writer::commit_write(Prepared write, uint64_t slot) noexcept
{
    assert(table_ && write.table_ == table_ && write.footprint_.entries == 1 && planned_.entries != 0);
    auto       observed = SourceTail::prepare(observation_.tail, write.write_);
    const bool changed  = table_->commit_write(std::move(write.write_), slot);
    observed.complete(changed, slot);
    --remaining_.entries;
    remaining_.key_bytes -= write.footprint_.key_bytes;
    remaining_.payload_bytes -= write.footprint_.payload_bytes;
    --planned_.entries;
    planned_.key_bytes -= write.footprint_.key_bytes;
    planned_.payload_bytes -= write.footprint_.payload_bytes;
    return changed;
}
} // namespace crowdb::tree
