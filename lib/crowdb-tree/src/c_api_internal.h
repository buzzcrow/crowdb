// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/async_page_store.h"
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/page_store.h"

#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#endif

#include <memory>
#include <string>

struct PageStoreBundle
{
    std::unique_ptr<crowdb::tree::PageStore>      store;
    std::unique_ptr<crowdb::tree::AsyncPageStore> async_store;
    crowdb::tree::AsyncPageStore                 *async_store_view = nullptr;
#ifdef CROWDB_HAVE_LIBURING
    std::unique_ptr<crowdb::common::DiskIOUring> uring;
#endif
    std::string backend_label;
};

struct ct_page_store
{
    std::shared_ptr<PageStoreBundle> bundle;
};
