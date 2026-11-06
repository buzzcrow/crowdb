// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "page_cache_residency_internal.h"
#include "published_state.h"

namespace crowdb::tree
{
void PageCacheResidency::State::prepare_page(Update &update, uint64_t pid,
                                             const std::shared_ptr<const PublishedPage> &replacement,
                                             const std::shared_ptr<AllocationBudget>    &metadata)
{
    std::shared_ptr<Page> page;
    if (replacement) {
        page           = detail::reserved_shared<Page>(metadata, metadata);
        const auto add = [&](const BaseRef &base, PageCacheKind kind) {
            if (!base.backing || base.resident) {
                return;
            }
            const auto key = page_cache_key(kind, base);
            page->links.push_back({.key = key});
            if (!update.bases.contains(key)) {
                update.bases.emplace(key, detail::reserved_shared<Base>(metadata));
            }
        };
        if (replacement->leaf) {
            page->links.reserve(replacement->leaf->bases.size());
            for (const auto &base : replacement->leaf->bases) {
                add(base, PageCacheKind::Leaf);
            }
        }
        else if (!replacement->inner && replacement->inner_backing) {
            page->links.reserve(1);
            add(BaseRef{.backing = replacement->inner_backing}, PageCacheKind::Inner);
        }
    }
    update.pages.emplace(pid, std::move(page));
}

std::shared_ptr<PageCacheResidency::Update>
PageCacheResidency::prepare_initial(const MappingView &mapping, const std::shared_ptr<AllocationBudget> &metadata) const
{
    auto                         update = detail::reserved_shared<Update>(metadata, state_);
    PageCacheNodeResource::Scope scope(metadata);

    struct Context
    {
        std::shared_ptr<Update>                  update;
        const std::shared_ptr<AllocationBudget> &metadata;
    } context{.update = update, .metadata = metadata};

    mapping.visit(&context, [](void *argument, uint64_t pid, const auto &page) {
        auto &self = *static_cast<Context *>(argument);
        if (self.update->pages.size() == 32) {
            self.update->following = detail::reserved_shared<Update>(self.metadata, self.update->state);
            self.update            = self.update->following;
        }
        State::prepare_page(*self.update, pid, page, self.metadata);
    });
    return update;
}

std::shared_ptr<PageCacheResidency::Update>
PageCacheResidency::prepare_replacement(const MappingView                       &mapping,
                                        const std::shared_ptr<AllocationBudget> &metadata) const
{
    auto update   = prepare_initial(mapping, metadata);
    update->reset = true;
    return update;
}

size_t PageCacheResidency::initial_allocation_bound(const MappingView &mapping) noexcept
{
    detail::AllocationBound bound(detail::shared_allocation_bound<Update>());
    mapping.visit(&bound, [](void *argument, uint64_t, const auto &page) {
        auto      &total = *static_cast<detail::AllocationBound *>(argument);
        const auto refs  = page->leaf ? page->leaf->bases.size() : size_t{1};
        total.add(page_allocation_bound(refs));
    });
    return bound.value().value_or(SIZE_MAX);
}
} // namespace crowdb::tree
