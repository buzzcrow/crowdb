// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_writeback.h"
#include "extent_ledger.h"

namespace crowdb::tree
{
// Allocation is an asynchronous metadata step; a Tree CPU worker never waits
// for another ledger command or reserves an uncompressed worst-case disk span.
PageWriteOperation::ReserveExtent extent_write_reservation(std::shared_ptr<ExtentLedger>                  ledger,
                                                           std::shared_ptr<detail::AllocationReservation> metadata);
} // namespace crowdb::tree
