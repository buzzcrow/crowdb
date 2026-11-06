// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_internal.h"

namespace crowdb::tree
{
namespace
{
bool evictable(const PublishedPage &page, PageCacheKind kind)
{
    if (page.metrics.dirty) {
        return false;
    }
    if (kind == PageCacheKind::Inner) {
        return page.level != 0 && page.inner && page.inner_backing && page.inner_backing->reloadable();
    }
    if (page.level != 0 || !page.leaf || !page.leaf->overlays.empty()) {
        return false;
    }
    bool resident = false;
    for (const auto &base : page.leaf->bases) {
        if (base.resident) {
            if (!base.backing || !base.backing->reloadable()) {
                return false;
            }
            resident = true;
        }
    }
    return resident;
}

struct CleanEviction : std::enable_shared_from_this<CleanEviction>
{
    TreeCore                                       *core      = nullptr;
    PageCacheKind                                   kind      = PageCacheKind::Leaf;
    size_t                                          limit     = 0;
    size_t                                          resident  = 0;
    size_t                                          evicted   = 0;
    uint64_t                                        cursor    = 0;
    uint64_t                                        candidate = MappingView::kPageLimit;
    bool                                            counting  = true;
    bool                                            waiting   = false;
    bool                                            installed = false;
    std::atomic<bool>                               ready{false};
    std::atomic<bool>                               done{false};
    std::shared_ptr<const PublishedTreeState>       view;
    std::shared_ptr<TreeOperationLifetime::Ticket>  ticket;
    std::shared_ptr<detail::AllocationReservation>  controls;
    std::shared_ptr<detail::AllocationReservation>  publication_credit;
    std::shared_ptr<detail::FlushRuntime::Progress> progress;
    std::shared_ptr<PublicationRequest>             request;
    std::shared_ptr<CleanEviction>                  trim_owner;
    PageCacheTrim                                   trim;

    ~CleanEviction()
    {
        if (publication_credit) {
            publication_credit->seal();
        }
        if (controls) {
            controls->seal();
        }
    }

    void wake() noexcept
    {
        progress->request(weak_from_this().lock());
    }

    static void run(void *context) noexcept
    {
        auto &self = *static_cast<CleanEviction *>(context);
        if (self.done.load(std::memory_order_acquire)) {
            return;
        }
        try {
            self.step();
        }
        catch (...) {
            self.trim_cache();
        }
    }

    void trim_cache() noexcept
    {
        request.reset();
        if (publication_credit) {
            publication_credit->seal();
            publication_credit.reset();
        }
        const auto remaining = resident - evicted;
        trim.kind            = kind;
        trim.max_pages       = limit > remaining ? limit - remaining : 0;
        trim.context         = this;
        trim.complete        = [](void *context, size_t count) noexcept {
            auto self = std::move(static_cast<CleanEviction *>(context)->trim_owner);
            self->evicted += count;
            self->done.store(true, std::memory_order_release);
            self->done.notify_one();
        };
        trim_owner = weak_from_this().lock();
        core->cache()->trim(trim);
    }

    void step()
    {
        if (waiting) {
            if (!ready.load(std::memory_order_acquire)) {
                return;
            }
            waiting = false;
            evicted += static_cast<size_t>(installed);
            request.reset();
            publication_credit->seal();
            publication_credit.reset();
        }
        if (counting) {
            const auto end = view->mapping.visit_from(cursor, 64, this, [](void *context, uint64_t, const auto &page) {
                auto &self = *static_cast<CleanEviction *>(context);
                self.resident += static_cast<size_t>(evictable(*page, self.kind));
            });
            if (end) {
                counting = false;
                cursor   = 0;
            }
            wake();
            return;
        }
        if (resident - evicted <= limit) {
            trim_cache();
            return;
        }
        candidate      = MappingView::kPageLimit;
        const auto end = view->mapping.visit_from(cursor, 1, this, [](void *context, uint64_t pid, const auto &page) {
            auto &self = *static_cast<CleanEviction *>(context);
            if (evictable(*page, self.kind)) {
                self.candidate = pid;
            }
        });
        if (candidate != MappingView::kPageLimit) {
            publish();
        }
        else if (end) {
            trim_cache();
        }
        else {
            wake();
        }
    }

    void publish()
    {
        const auto current = core->capture();
        const auto page    = current->mapping.get(candidate);
        if (current->generation != view->generation || !page || !evictable(*page, kind) ||
            page->revision == UINT64_MAX || (page->leaf && page->leaf->representation_id == UINT64_MAX)) {
            wake();
            return;
        }
        using namespace detail;
        const auto bound =
            AllocationBound(PublicationCoordinator::empty_commit_reserve_bytes())
                .add(MappingView::path_copy_bound())
                .add(current->mapping.copy_bounds().max_leaf_bytes)
                .add(shared_allocation_bound<PublishedPage>() + shared_allocation_bound<PublicationRequest>() +
                     shared_allocation_bound<InstallRecord>() + shared_allocation_bound<AllocationReservation>() +
                     sizeof(PageReplacement))
                .add(PageCacheResidency::page_allocation_bound(page->leaf ? page->leaf->bases.size() : 1))
                .value();
        auto credit = bound ? ResourceBudget::try_reserve(core->binding()->account(), BudgetClass::kReclaim, *bound)
                            : std::nullopt;
        if (!credit) {
            trim_cache();
            return;
        }
        publication_credit = AllocationReservation::create(std::move(*credit));
        auto replacement   = reserved_shared<PublishedPage>(publication_credit, *page);
        ++replacement->revision;
        replacement->metrics.evict_candidate = false;
        if (page->leaf) {
            auto leaf = reserved_shared<PageState>(publication_credit, *page->leaf, publication_credit);
            ++leaf->representation_id;
            for (auto &base : leaf->bases) {
                base.resident.reset();
            }
            replacement->leaf = std::move(leaf);
        }
        else {
            replacement->inner.reset();
        }
        request = reserved_shared<PublicationRequest>(publication_credit, publication_credit);
        request->transaction.record =
            reserved_shared<InstallRecord>(publication_credit, InstallToken{.generation = current->generation});
        request->transaction.pages.push_back({.page_id = candidate, .expected = page, .replacement = replacement});
        request->on_done = [self = shared_from_this()](PublicationResult result) {
            self->installed = result.status.ok();
            self->ready.store(true, std::memory_order_release);
            self->wake();
        };
        waiting = true;
        ready.store(false, std::memory_order_release);
        if (!core->publisher().try_submit(request)) {
            installed = false;
            ready.store(true, std::memory_order_release);
            wake();
        }
    }
};
} // namespace

size_t TreeCore::evict_clean_pages(PageCacheKind kind, size_t max_resident)
try {
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr || max_resident == SIZE_MAX) {
        return 0;
    }
    using namespace detail;
    const auto bound = AllocationBound(shared_allocation_bound<CleanEviction>())
                           .add(shared_allocation_bound<AllocationReservation>())
                           .add(TreeOperationLifetime::ticket_allocation_bound())
                           .add(FlushRuntime::progress_allocation_bound())
                           .value();
    auto       credit =
        bound ? ResourceBudget::try_reserve(binding()->account(), BudgetClass::kReclaim, *bound) : std::nullopt;
    if (!credit) {
        return 0;
    }
    auto metadata       = AllocationReservation::create(std::move(*credit));
    auto operation      = reserved_shared<CleanEviction>(metadata);
    operation->controls = metadata;
    operation->core     = this;
    operation->kind     = kind;
    operation->limit    = max_resident;
    operation->view     = capture();
    if (!operation_lifetime().acquire(metadata, &operation->ticket).ok() ||
        !binding()
             ->cpu()
             ->reserve_progress(binding()->lane(), &CleanEviction::run, operation.get(), metadata, &operation->progress)
             .ok()) {
        return 0;
    }
    operation->wake();
    while (!operation->done.load(std::memory_order_acquire)) {
        operation->done.wait(false, std::memory_order_acquire);
    }
    return operation->evicted;
}
catch (const std::bad_alloc &) {
    return 0;
}
} // namespace crowdb::tree
