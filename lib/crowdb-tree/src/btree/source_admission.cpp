// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "published_state.h"
#include "source_admission_internal.h"

#include <algorithm>
#include <utility>

namespace crowdb::tree
{
namespace
{
std::atomic<uint64_t> next_source_admission_identity{1};

class AdminGuard
{
  public:
    explicit AdminGuard(std::atomic_flag &gate) : gate_(gate), held_(!gate.test_and_set(std::memory_order_acquire))
    {
    }

    ~AdminGuard()
    {
        if (held_) {
            gate_.clear(std::memory_order_release);
        }
    }

    [[nodiscard]] bool held() const noexcept
    {
        return held_;
    }

  private:
    std::atomic_flag &gate_;
    bool              held_;
};
} // namespace

struct SourceAdmission::State
{
    explicit State(const std::shared_ptr<detail::AllocationReservation> &metadata) : epoch(metadata)
    {
    }

    struct Root : EpochManager::Deferred
    {
        Root(std::shared_ptr<const SourceCatalog> value, AllocationLease credit)
            : allocation(std::move(credit)),
              catalog(std::move(value))
        {
            destroy = [](EpochManager::Deferred *entry) noexcept { Deleter{}(static_cast<Root *>(entry)); };
        }

        struct Deleter
        {
            void operator()(Root *value) const noexcept
            {
                if (value != nullptr) {
                    auto credit = std::move(value->allocation);
                    delete value;
                }
            }
        };

        using Owner = std::unique_ptr<Root, Deleter>;

        static Owner create(std::shared_ptr<const SourceCatalog>                  value,
                            const std::shared_ptr<detail::AllocationReservation> &budget)
        {
            AllocationLease credit(budget, sizeof(Root), AllocationKind::kMetadata);
            Owner           result(new Root(std::move(value), std::move(credit)));
            return result;
        }

        [[nodiscard]] static size_t allocation_bound() noexcept
        {
            return sizeof(Root);
        }

        AllocationLease                      allocation;
        std::shared_ptr<const SourceCatalog> catalog;
    };

    void publish(Root::Owner next) noexcept
    {
        auto       *previous = root.load(std::memory_order_acquire);
        const auto &before   = *previous->catalog;
        const auto &after    = *next->catalog;
        if (after.active != before.active) {
            after.active->attach_source_stats(after.memory);
        }
        for (const auto &table : before.tables) {
            if (std::find(after.tables.begin(), after.tables.end(), table) == after.tables.end()) {
                table->detach_source_stats();
            }
        }
        previous = root.exchange(next.release(), std::memory_order_acq_rel);
        previous->catalog->retirement.mark();
        epoch.defer(previous);
    }

    bool matches(const std::shared_ptr<SourceCapture> &capture, uint64_t batch) const noexcept
    {
        return capture && capture == pending && capture->state_->owner_identity == identity &&
               capture->batch_id() == batch;
    }

    std::shared_ptr<SourceTailDomain> tail_domain;

    const uint64_t                 identity = next_source_admission_identity.fetch_add(1, std::memory_order_relaxed);
    mutable EpochManager           epoch;
    std::atomic<Root *>            root{nullptr};
    std::atomic<bool>              closed{false};
    std::atomic_flag               admin      = ATOMIC_FLAG_INIT;
    uint64_t                       next_table = 2;
    uint64_t                       next_batch = 1;
    std::shared_ptr<SourceCapture> pending;
    std::shared_ptr<Rotation>      rotation;
    Root::Owner                    pending_root;
    Root::Owner                    covered_root;
    SourceWriteOptions             write_options;
};

struct SourceAdmission::Rotation::Data
{
    std::shared_ptr<const SourceCatalog> expected;
    std::shared_ptr<SourceCatalog>       next;
    State::Root::Owner                   root;
    uint64_t                             owner_identity = 0;
    bool                                 committed      = false;
};

SourceAdmission::Rotation::Rotation(detail::ReservedUnique<Data> data) : data_(std::move(data))
{
}

SourceAdmission::Rotation::~Rotation() = default;

const std::shared_ptr<const SourceCatalog> &SourceAdmission::Rotation::expected() const noexcept
{
    return data_->expected;
}

const std::shared_ptr<SourceCatalog> &SourceAdmission::Rotation::next() const noexcept
{
    return data_->next;
}

std::optional<size_t> SourceAdmission::rotation_allocation_bound(size_t sources) noexcept
{
    using detail::shared_allocation_bound;
    return detail::AllocationBound(shared_allocation_bound<Rotation>() + sizeof(Rotation::Data) +
                                   State::Root::allocation_bound() + SourceCatalog::allocation_bound() +
                                   shared_allocation_bound<MemTable>() + shared_allocation_bound<SourceWriteBudget>() +
                                   EpochManager::infrastructure_allocation_bound() +
                                   Node::alloc_size(ConcurrentSkipList::kMaxHeight, 0))
        .add_product(sources + 1, sizeof(std::shared_ptr<MemTable>))
        .value();
}

Status SourceAdmission::prepare_rotation(const std::shared_ptr<detail::AllocationReservation> &metadata,
                                         const std::shared_ptr<const SourceCatalog>           &expected,
                                         std::shared_ptr<Rotation>                            *out)
try {
    if (!metadata || !expected || out == nullptr || *out ||
        (state_->write_options.account && metadata->account() != state_->write_options.account)) {
        return Status::invalid_argument("source rotation requires its owned metadata and expected catalog");
    }
    AdminGuard admin(state_->admin);
    if (!admin.held() || state_->closed.load(std::memory_order_acquire) || state_->pending || state_->rotation ||
        catalog() != expected) {
        return Status::unavailable("source capture or administration prevents rotation");
    }
    auto data            = detail::reserved_unique<Rotation::Data>(metadata);
    data->owner_identity = state_->identity;
    data->expected       = expected;
    data->next           = detail::reserved_shared<SourceCatalog>(metadata, metadata);
    auto &next           = *data->next;
    next.memory          = expected->memory;
    next.tables.reserve(expected->tables.size() + 1);
    next.tables.assign(expected->tables.begin(), expected->tables.end());
    next.floor      = expected->floor;
    next.generation = expected->generation;
    next.serial     = expected->serial + 1;
    next.active     = detail::reserved_shared<MemTable>(metadata, state_->next_table, nullptr,
                                                        expected->active_budget ? metadata : nullptr);
    if (expected->active_budget) {
        next.active_budget = detail::reserved_shared<SourceWriteBudget>(metadata, state_->write_options);
    }
    next.active->set_durable_floor(next.floor);
    next.tables.push_back(next.active);
    data->root    = State::Root::create(data->next, metadata);
    auto rotation = detail::reserved_construct<Rotation>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) Rotation(std::move(data)); });
    state_->rotation = rotation;
    *out             = std::move(rotation);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("source rotation allocation failed");
}

Status SourceAdmission::activate_rotation(const std::shared_ptr<Rotation> &rotation)
{
    AdminGuard admin(state_->admin);
    if (!admin.held()) {
        return Status::unavailable("source administration busy");
    }
    if (!rotation || rotation->data_->owner_identity != state_->identity) {
        return Status::invalid_argument("source rotation belongs to another owner");
    }
    auto &data = *rotation->data_;
    if (data.committed) {
        return Status::Ok();
    }
    if (state_->rotation != rotation || catalog() != data.expected) {
        return Status::invalid_argument("source rotation lost its expected catalog");
    }
    if (data.expected->active_budget && data.expected->active_budget->paused()) {
        const auto bound = data.expected->active_budget->paused_bound();
        data.expected->active->set_sealed_admission_bound(bound.entries, bound.key_bytes);
    }
    data.expected->active->close();
    state_->publish(std::move(data.root));
    ++state_->next_table;
    data.committed = true;
    state_->rotation.reset();
    return Status::Ok();
}

Status SourceAdmission::abort_rotation(const std::shared_ptr<Rotation> &rotation)
{
    AdminGuard admin(state_->admin);
    if (!admin.held()) {
        return Status::unavailable("source administration busy");
    }
    if (state_->rotation != rotation) {
        return Status::invalid_argument("source rotation no longer pending");
    }
    state_->rotation.reset();
    return Status::Ok();
}

struct SourceAdmission::Coverage::Data
{
    std::shared_ptr<const SourceCatalog> expected;
    std::shared_ptr<SourceCatalog>       next;
    State::Root::Owner                   root;
    uint64_t                             owner_identity = 0;
    bool                                 committed      = false;
};

SourceAdmission::Coverage::Coverage(detail::ReservedUnique<Data> data) : data_(std::move(data))
{
}

SourceAdmission::Coverage::~Coverage() = default;

uint64_t SourceAdmission::identity() const noexcept
{
    return state_->identity;
}

std::optional<size_t> SourceAdmission::coverage_allocation_bound(size_t sources) noexcept
{
    return detail::AllocationBound(detail::shared_allocation_bound<Coverage>() + sizeof(Coverage::Data) +
                                   State::Root::allocation_bound() + SourceCatalog::allocation_bound())
        .add_product(sources, sizeof(std::shared_ptr<MemTable>))
        .value();
}

Status SourceAdmission::prepare_coverage(uint64_t                                              frontier,
                                         const std::shared_ptr<detail::AllocationReservation> &metadata,
                                         std::shared_ptr<Coverage>                            *out)
try {
    if (!metadata || out == nullptr || *out ||
        (state_->write_options.account && metadata->account() != state_->write_options.account)) {
        return Status::invalid_argument("split coverage requires owned source credit and empty output");
    }
    AdminGuard admin(state_->admin);
    if (!admin.held() || state_->closed.load(std::memory_order_acquire) || state_->pending || state_->rotation) {
        return Status::unavailable("source capture or administration prevents split coverage");
    }
    const auto previous = catalog();
    if (frontier < previous->floor) {
        return Status::invalid_argument("split coverage cannot move the source floor backwards");
    }
    auto data                 = detail::reserved_unique<Coverage::Data>(metadata);
    data->expected            = previous;
    data->owner_identity      = state_->identity;
    data->next                = detail::reserved_shared<SourceCatalog>(metadata, metadata);
    data->next->memory        = previous->memory;
    data->next->tables        = previous->tables;
    data->next->active        = previous->active;
    data->next->active_budget = previous->active_budget;
    data->next->floor         = frontier;
    data->next->generation    = previous->generation;
    data->next->serial        = previous->serial + 1;
    data->root                = State::Root::create(data->next, metadata);
    *out                      = detail::reserved_construct<Coverage>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) Coverage(std::move(data)); });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("split coverage preparation failed");
}

Status SourceAdmission::install_coverage(const std::shared_ptr<Coverage> &prepared, const PublishedTreeState &published)
{
    AdminGuard admin(state_->admin);
    if (!admin.held()) {
        return Status::unavailable("source administration is busy");
    }
    if (!prepared || prepared->data_->owner_identity != state_->identity) {
        return Status::invalid_argument("split coverage belongs to another source owner");
    }
    auto &data = *prepared->data_;
    if (published.generation != data.next->generation || published.coverage != data.next->floor || !published.sources ||
        published.sources->inheritance || published.sources->floor != data.next->floor ||
        published.sources->tables.size() != data.next->tables.size() ||
        !std::equal(published.sources->tables.begin(), published.sources->tables.end(), data.next->tables.begin())) {
        return Status::unavailable("split coverage has not atomically published "
                                   "its local source membership");
    }
    if (data.committed) {
        return Status::Ok();
    }
    if (state_->pending || state_->rotation || catalog() != data.expected) {
        return Status::unavailable("source catalog changed before split coverage acknowledgement");
    }
    data.next->active->set_durable_floor(data.next->floor);
    state_->publish(std::move(data.root));
    data.committed = true;
    data.expected.reset();
    return Status::Ok();
}

std::optional<size_t> SourceAdmission::allocation_bound(size_t max_threads) noexcept
{
    using detail::shared_allocation_bound;
    return detail::AllocationBound(
               sizeof(SourceAdmission) + sizeof(State) + State::Root::allocation_bound() +
               SourceCatalog::allocation_bound() + shared_allocation_bound<MemTable>() +
               shared_allocation_bound<SourceWriteBudget>() + shared_allocation_bound<SourceMemoryLedger>() +
               shared_allocation_bound<detail::AllocationReservation>() +
               (2 * EpochManager::infrastructure_allocation_bound()) +
               Node::alloc_size(ConcurrentSkipList::kMaxHeight, 0) + sizeof(std::shared_ptr<MemTable>))
        .add(SourceTailDomain::allocation_bound(max_threads))
        .add_product(max_threads, 2 * EpochManager::participant_allocation_bytes())
        .value();
}

SourceAdmission::SourceAdmission(uint64_t floor, uint64_t generation, SourceWriteOptions options,
                                 std::shared_ptr<detail::AllocationReservation> lifecycle)
{
    if (!lifecycle && options.account) {
        const auto bytes = allocation_bound();
        if (!bytes) {
            throw std::bad_alloc();
        }
        auto credit =
            detail::ResourceBudget::try_reserve(options.account, detail::BudgetClass::kAdmittedWriter, *bytes);
        if (!credit) {
            throw std::bad_alloc();
        }
        lifecycle = detail::AllocationReservation::create(std::move(*credit));
    }
    state_                = detail::reserved_unique<State>(lifecycle, lifecycle);
    state_->write_options = std::move(options);
    state_->tail_domain   = detail::reserved_shared<SourceTailDomain>(lifecycle, lifecycle);
    auto initial          = detail::reserved_shared<SourceCatalog>(lifecycle, lifecycle);
    initial->memory       = detail::reserved_shared<SourceMemoryLedger>(lifecycle);
    initial->floor        = floor;
    initial->generation   = generation;
    initial->active       = detail::reserved_shared<MemTable>(lifecycle, 1, nullptr, lifecycle);
    if (state_->write_options.account) {
        initial->active_budget = detail::reserved_shared<SourceWriteBudget>(lifecycle, state_->write_options);
    }
    initial->active->set_durable_floor(floor);
    initial->tables.push_back(initial->active);
    auto initial_root = State::Root::create(initial, lifecycle);
    initial->active->attach_source_stats(initial->memory);
    state_->root.store(initial_root.release(), std::memory_order_release);
}

SourceAdmission::~SourceAdmission()
{
    close();
    state_->tail_domain->close();
    auto *root = state_->root.load(std::memory_order_relaxed);
    root->catalog->retirement.mark();
    State::Root::Deleter{}(root);
}

std::optional<size_t> SourceAdmission::capture_allocation_bound(size_t sources) noexcept
{
    using detail::shared_allocation_bound;
    // Includes successor infrastructure, two catalogs and pending/covered
    // roots. Table vector growth retains old/new buffers before closure.
    return detail::AllocationBound(
               (2 * SourceCatalog::allocation_bound()) + shared_allocation_bound<MemTable>() +
               shared_allocation_bound<SourceWriteBudget>() + shared_allocation_bound<SourceCapture::State>() +
               shared_allocation_bound<SourceCapture>() + (2 * State::Root::allocation_bound()) +
               EpochManager::infrastructure_allocation_bound() + Node::alloc_size(ConcurrentSkipList::kMaxHeight, 0) +
               (6 * sizeof(std::shared_ptr<MemTable>)))
        .add_product(sources, sizeof(SourceCapture::State::Node) + (6 * sizeof(std::shared_ptr<MemTable>)))
        .value();
}

std::shared_ptr<const SourceCatalog> SourceAdmission::catalog() const
{
    auto guard = state_->epoch.enter();
    return state_->root.load(std::memory_order_acquire)->catalog;
}

Status SourceAdmission::try_admit(uint64_t bound, Writer *out)
try {
    if (out == nullptr || out->table()) {
        return Status::invalid_argument("writer output must be empty");
    }
    if (state_->closed.load(std::memory_order_acquire)) {
        return Status::unavailable("source admission closed");
    }
    auto observation = state_->tail_domain->observe();
    auto view        = catalog();
    if (view->active_budget) {
        return Status::invalid_argument("budgeted source requires a writer footprint");
    }
    if (!view->active->try_enter()) {
        return Status::unavailable("source publication in progress");
    }
    *out              = Writer(view->active, bound);
    out->observation_ = std::move(observation);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("source lifecycle participant credit exhausted");
}

Status SourceAdmission::try_admit(uint64_t bound, WriteFootprint footprint, Writer *out, detail::BudgetClass category)
{
    return try_admit(bound, footprint, out, category, nullptr);
}

Status SourceAdmission::try_admit(uint64_t bound, WriteFootprint footprint, Writer *out, detail::BudgetClass category,
                                  bool *paused)
try {
    if (paused != nullptr) {
        *paused = false;
    }
    if (out == nullptr || out->table()) {
        return Status::invalid_argument("writer output must be empty");
    }
    if (state_->closed.load(std::memory_order_acquire)) {
        return Status::unavailable("source admission closed");
    }
    auto observation = state_->tail_domain->observe();
    auto view        = catalog();
    if (!view->active_budget) {
        return Status::invalid_argument("source has no writer budget account");
    }
    if (!view->active->try_enter()) {
        if (paused != nullptr) {
            *paused = true;
        }
        return Status::unavailable("source publication in progress");
    }
    std::shared_ptr<detail::AllocationReservation> credit;
    auto status = view->active_budget->admit(footprint, category, &credit, paused);
    if (!status.ok()) {
        view->active->leave();
        return status;
    }

    *out                = Writer(view->active, bound);
    out->observation_   = std::move(observation);
    out->source_budget_ = view->active_budget;
    out->credit_        = std::move(credit);
    out->remaining_     = footprint;
    out->category_      = category;
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("source lifecycle participant credit exhausted");
}

Status SourceAdmission::try_admit_reserved(uint64_t bound, SourceWriteReservation *reservation, Writer *out,
                                           std::optional<detail::BudgetClass> source_category)
try {
    if (out == nullptr || out->table() || reservation == nullptr || !reservation->valid()) {
        return Status::invalid_argument("writer requires an unused reservation and empty output");
    }
    if (state_->closed.load(std::memory_order_acquire)) {
        return Status::unavailable("source admission closed");
    }
    auto observation = state_->tail_domain->observe();
    auto view        = catalog();
    if (!view->active_budget) {
        return Status::invalid_argument("source has no writer budget account");
    }
    if (!view->active->try_enter()) {
        return Status::unavailable("source publication in progress");
    }
    const auto                                     footprint = reservation->footprint();
    const auto                                     category  = source_category.value_or(reservation->category());
    std::shared_ptr<detail::AllocationReservation> credit;
    auto                                           status = view->active_budget->bind(reservation, &credit, category);
    if (!status.ok()) {
        view->active->leave();
        return status;
    }

    *out                = Writer(view->active, bound);
    out->observation_   = std::move(observation);
    out->source_budget_ = view->active_budget;
    out->credit_        = std::move(credit);
    out->remaining_     = footprint;
    out->category_      = category;
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("source lifecycle participant credit exhausted");
}

Status SourceAdmission::try_capture(const std::atomic<uint64_t>                   &contiguous,
                                    std::shared_ptr<detail::AllocationReservation> admission_owner,
                                    std::shared_ptr<SourceCapture>                *out,
                                    std::shared_ptr<const SourceCatalog>           expected_catalog)
try {
    if (out == nullptr || !admission_owner) {
        return Status::invalid_argument("capture requires admitted metadata credit");
    }
    if (state_->write_options.account && admission_owner->account() != state_->write_options.account) {
        return Status::invalid_argument("capture credit belongs to another source account");
    }
    AdminGuard admin(state_->admin);
    if (!admin.held() || state_->closed.load(std::memory_order_acquire)) {
        return Status::unavailable("source administration busy or closed");
    }
    if (state_->rotation) {
        return Status::unavailable("source rotation publication is pending");
    }
    if (state_->pending) {
        if (expected_catalog && (state_->pending->expected_catalog() != expected_catalog ||
                                 state_->pending->admission_owner() != admission_owner)) {
            return Status::unavailable("source capture belongs to another prepared reservation");
        }
        *out = state_->pending;
        return Status::Ok();
    }
    const auto previous = catalog();
    if (expected_catalog && previous != expected_catalog) {
        return Status::unavailable("source catalog changed after reserve preparation");
    }
    if (contiguous.load(std::memory_order_acquire) <= previous->floor) {
        return Status::unavailable("no completed prefix beyond source floor");
    }
    if (admission_owner->available() == 0) {
        return Status::resource_exhausted("capture metadata credit exhausted");
    }
    auto next        = detail::reserved_shared<SourceCatalog>(admission_owner, admission_owner);
    next->memory     = previous->memory;
    next->tables     = previous->tables;
    next->floor      = previous->floor;
    next->generation = previous->generation;
    next->serial     = previous->serial + 1;
    next->active     = detail::reserved_shared<MemTable>(admission_owner, state_->next_table, nullptr,
                                                         previous->active_budget ? admission_owner : nullptr);
    if (previous->active_budget) {
        next->active_budget = detail::reserved_shared<SourceWriteBudget>(admission_owner, state_->write_options);
    }
    next->active->set_durable_floor(previous->floor);
    next->tables.push_back(next->active);
    auto captured   = detail::reserved_shared<SourceCapture::State>(admission_owner, previous, next, admission_owner);
    auto covered    = detail::reserved_shared<SourceCatalog>(admission_owner, admission_owner);
    covered->memory = next->memory;
    covered->tables.reserve(next->tables.size());
    covered->generation       = next->generation;
    covered->serial           = next->serial + 1;
    covered->active           = next->active;
    covered->active_budget    = next->active_budget;
    captured->covered_catalog = covered;
    captured->batch           = state_->next_batch;
    captured->owner_identity  = state_->identity;
    auto handle               = detail::reserved_construct<SourceCapture>(
        admission_owner, [&](auto *storage) { ::new (static_cast<void *>(storage)) SourceCapture(captured); });
    auto root         = State::Root::create(next, admission_owner);
    auto covered_root = State::Root::create(covered, admission_owner);
    if (contiguous.load(std::memory_order_acquire) <= previous->floor) {
        return Status::unavailable("completed prefix changed before source closure");
    }
    previous->active->close();
    captured->frontier   = contiguous.load(std::memory_order_acquire);
    state_->pending      = handle;
    state_->pending_root = std::move(root);
    state_->covered_root = std::move(covered_root);
    ++state_->next_table;
    ++state_->next_batch;
    *out = std::move(handle);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("source capture allocation failed");
}

Status SourceAdmission::activate_successor(const std::shared_ptr<SourceCapture> &capture, uint64_t batch_id)
{
    AdminGuard admin(state_->admin);
    if (!admin.held()) {
        return Status::unavailable("source administration busy");
    }
    if (!state_->matches(capture, batch_id)) {
        return Status::invalid_argument("stale source capture");
    }
    if (capture->successor_active()) {
        return Status::Ok();
    }
    state_->publish(std::move(state_->pending_root));
    capture->state_->activated.store(true, std::memory_order_release);
    return Status::Ok();
}

Status SourceAdmission::mark_repair(const std::shared_ptr<SourceCapture> &capture, uint64_t batch_id,
                                    uint64_t actual_coverage)
{
    AdminGuard admin(state_->admin);
    if (!admin.held()) {
        return Status::unavailable("source administration busy");
    }
    if (!state_->matches(capture, batch_id) || actual_coverage != capture->floor()) {
        return Status::invalid_argument("repair must retain this capture at its original floor");
    }
    capture->state_->phase.store(SourceCapturePhase::kRepairRequired, std::memory_order_release);
    return Status::Ok();
}

Status SourceAdmission::finish_covered(const std::shared_ptr<SourceCapture> &capture, uint64_t batch_id,
                                       uint64_t actual_coverage)
try {
    AdminGuard admin(state_->admin);
    if (!admin.held()) {
        return Status::unavailable("source administration busy");
    }
    if (!state_->matches(capture, batch_id) || actual_coverage != capture->frontier() || !capture->drained() ||
        !capture->successor_active()) {
        return Status::invalid_argument("coverage acknowledgement must match the drained active capture");
    }
    const auto  previous = catalog();
    const auto &next     = capture->state_->covered_catalog;
    next->floor          = actual_coverage;
    for (const auto &table : previous->tables) {
        if (table == next->active || table->slot_range().max > actual_coverage) {
            next->tables.push_back(table);
        }
    }
    next->active->set_durable_floor(actual_coverage);
    state_->publish(std::move(state_->covered_root));
    capture->state_->phase.store(SourceCapturePhase::kCovered, std::memory_order_release);
    state_->pending.reset();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("source coverage acknowledgement allocation failed");
}

void SourceAdmission::close() noexcept
{
    state_->closed.store(true, std::memory_order_release);
    state_->tail_domain->close();
}

size_t SourceAdmission::reclaim()
{
    return state_->epoch.try_reclaim();
}

} // namespace crowdb::tree

namespace crowdb::tree
{
const std::shared_ptr<SourceTailDomain> &SourceAdmission::tail_domain() const noexcept
{
    return state_->tail_domain;
}
} // namespace crowdb::tree
