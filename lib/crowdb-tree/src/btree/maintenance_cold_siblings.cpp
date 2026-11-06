// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "maintenance_operation_internal.h"
#include "page_load.h"

namespace crowdb::tree::detail
{
std::shared_ptr<const PublishedPage> MaintenanceOperation::cold_page(uint64_t pid) const
{
    for (const auto &page : cold->replacements) {
        if (page.page_id == pid) {
            return page.replacement;
        }
    }
    return cold->view->mapping.get(pid);
}

bool MaintenanceOperation::load_cold_inner(uint64_t pid)
{
    const auto page = cold_page(pid);
    if (!page || !page->inner_backing || !page->inner_backing->readable() ||
        cold->replacements.size() >= 3 * publication.max_path_pages) {
        finish(Status::corruption("maintenance cold inner exceeds its captured backing bound"));
        return false;
    }
    cold->loading_inner = true;
    cold->loading_pid   = pid;
    cold->waiting       = true;
    cold->ready.store(false, std::memory_order_release);
    auto self = shared_from_this();
    loader->load_inner(page->inner_backing, [self, pending = cold](Status status, auto inner) {
        pending->status       = std::move(status);
        pending->loaded_inner = std::move(inner);
        pending->ready.store(true, std::memory_order_release);
        self->schedule();
    });
    return false;
}

bool MaintenanceOperation::cold_siblings()
{
    while (cold->next_sibling < cold->siblings.size()) {
        const auto pid  = cold->siblings[cold->next_sibling++];
        const auto page = cold_page(pid);
        if (!page || page->level == 0) {
            finish(Status::corruption("inner maintenance sibling is not an inner page"));
            return false;
        }
        if (!page->inner) {
            return load_cold_inner(pid);
        }
    }
    return true;
}
} // namespace crowdb::tree::detail
