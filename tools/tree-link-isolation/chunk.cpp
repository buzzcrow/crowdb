// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/c_api.h"

int main()
{
    ct_root_catalog *catalog = nullptr;
    ct_page_store   *store   = nullptr;
    if (ct_memory_root_catalog_open(1, &catalog) != 0) {
        return 1;
    }
    const ct_chunk_page_store_options options{.tree_id = 1, .owner_epoch = 1};
    const ct_status                   status = ct_chunk_page_store_open(&options, catalog, &store);
    auto *volatile rpc_constructor           = &ct_rpc_chunk_transport_open;
    const bool linked                        = rpc_constructor != nullptr;
    ct_page_store_free(store);
    ct_root_catalog_free(catalog);
    return status == 0 && linked ? 0 : 1;
}
