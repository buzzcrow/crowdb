// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "extent_writeback.h"

namespace crowdb::tree
{
PageWriteOperation::ReserveExtent extent_write_reservation(std::shared_ptr<ExtentLedger>                  ledger,
                                                           std::shared_ptr<detail::AllocationReservation> metadata)
{
    return [ledger = std::move(ledger), metadata = std::move(metadata)](uint64_t                           length,
                                                                        PageWriteOperation::ExtentComplete complete) {
        auto rejected = complete;
        if (!ledger) {
            complete(Status::invalid_argument(), {});
            return;
        }
        auto status = ledger->reserve(length, metadata, [complete = std::move(complete)](ExtentLedgerResult result) {
            auto &extent = result.extent;
            complete(std::move(result.status), {.backend_id       = extent.backend_id,
                                                .address          = extent.address,
                                                .length           = extent.length,
                                                .content_id       = extent.identity,
                                                .generation       = extent.generation,
                                                .owner            = std::move(extent.owner),
                                                .physical_metrics = std::move(extent.physical_metrics),
                                                .chunk            = {}});
        });
        if (!status.ok()) {
            rejected(std::move(status), {});
        }
    };
}
} // namespace crowdb::tree
