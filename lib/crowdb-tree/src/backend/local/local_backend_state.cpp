// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/local_backend_state.h"

#include "crowdb-tree/backend/page_store.h"

namespace crowdb::tree
{
Status LocalBackendDomain::claim(PageStore *store, std::shared_ptr<const void> backend_owner,
                                 const std::shared_ptr<detail::AllocationReservation> &metadata,
                                 std::shared_ptr<LocalBackendDomain>                  *out)
try {
    if (store == nullptr || out == nullptr || *out) {
        return Status::invalid_argument("backend domain requires a store and empty output");
    }
    auto domain = detail::reserved_construct<LocalBackendDomain>(metadata, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) LocalBackendDomain(std::move(backend_owner));
    });
    if (!store->try_claim_runtime_domain()) {
        return Status::unavailable("local backend already has a runtime allocation authority");
    }
    domain->store_ = store;
    *out           = std::move(domain);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("backend domain allocation failed");
}

LocalBackendDomain::~LocalBackendDomain()
{
    if (store_ != nullptr) {
        store_->release_runtime_domain();
    }
}
} // namespace crowdb::tree
