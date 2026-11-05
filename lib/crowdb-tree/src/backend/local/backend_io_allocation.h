// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/backend/async_page_store.h"
#ifdef CROWDB_HAVE_LIBURING
namespace crowdb::tree::detail
{
::crowdb::common::IOAllocation io_allocation(const std::shared_ptr<AllocationBudget> &budget);
}
#endif
