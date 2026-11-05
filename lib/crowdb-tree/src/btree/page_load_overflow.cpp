// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/overflow_backing.h"
#include "btree/page_load_internal.h"
#include "btree/page_load_lifetime.h"
#include "btree/recovered_frame_identity.h"
#include "crowdb-tree/maptable/compressor.h"

#include <limits>
#include <set>

namespace crowdb::tree
{
namespace
{
struct LoadedValue
{
    LoadedValue(const std::shared_ptr<detail::AllocationReservation> &metadata, size_t bytes)
        : payload(metadata, bytes > buffer::kInlineCap ? bytes : 0, AllocationKind::kPayload),
          value(buffer::alloc(bytes))
    {
    }

    AllocationLease payload;
    buffer          value;
};

struct OverflowLoad : PageLoadProgress, std::enable_shared_from_this<OverflowLoad>
{
    OverflowLoad(const PageLoadBudget &budget, uint64_t started_at_ns)
        : PageLoadProgress(budget, started_at_ns),
          metadata(budget.bytes),
          blob(detail::ReservationAllocator<uint8_t>(metadata)),
          visited(std::less<>{}, detail::ReservationAllocator<uint64_t>(metadata))
    {
    }

    std::shared_ptr<detail::AllocationReservation>                          metadata;
    std::shared_ptr<AsyncPageStore>                                         store;
    std::shared_ptr<detail::FlushRuntime>                                   runtime;
    std::shared_ptr<detail::FlushRuntime::Lane>                             lane;
    PageLoadLimits                                                          limits;
    OverflowBackingResolver                                                 resolve;
    PageLoader::ValueComplete                                               complete;
    std::shared_ptr<OverflowLoad>                                           queued_owner;
    std::shared_ptr<const BackingRef>                                       backing;
    std::shared_ptr<buffer>                                                 value;
    AlignedLoadBytes                                                        blob;
    std::set<uint64_t, std::less<>, detail::ReservationAllocator<uint64_t>> visited;
    uint64_t                                                                next    = kInvalidPageId;
    size_t                                                                  length  = 0;
    size_t                                                                  written = 0;
    std::atomic<bool>                                                       terminal{false};

    void finish(Status status) noexcept
    {
        if (terminal.exchange(true)) {
            return;
        }
        release_if_idle();
        (void)metrics.finish(status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
        auto callback = std::move(complete);
        auto result   = std::move(value);
        if (!status.ok()) {
            result.reset();
        }
        complete_page_load(callback, std::move(status), std::move(result));
    }

    void read_next()
    {
        if (visited.contains(next)) {
            finish(Status::corruption("overflow backing chain contains a cycle"));
            return;
        }
        if (visited.size() >= limits.max_overflow_pages) {
            finish(Status::resource_exhausted("overflow chain exceeds page budget"));
            return;
        }
        visited.insert(next);
        backing.reset();
        auto status = resolve(next, &backing);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        if (!backing || !backing->readable() || backing->encoding != 0) {
            finish(Status::corruption("overflow backing has no independent supported extent"));
            return;
        }
        if (backing->length > limits.max_blob_bytes || !chunk_load_allowed(*backing, limits)) {
            finish(Status::resource_exhausted("overflow blob exceeds load budget"));
            return;
        }
        if (backing->storage == BackingStorage::kResidentFrame) {
            queue_page_load(shared_from_this(), &OverflowLoad::decode);
        }
        else {
            if (!store) {
                finish(Status::invalid_argument("durable overflow read requires a backend"));
                return;
            }
            blob.resize(backing->length);
            submit_page_read(shared_from_this(), backing, blob.data(), blob.size());
        }
    }

    void decode_frame()
    {
        LoadBytes  frame{LoadBytes::allocator_type(metadata)};
        auto       status      = decode_backing_frame({blob.data(), blob.size()}, *backing, limits, metadata, &frame);
        const auto frame_bytes = static_cast<uint32_t>(frame.size());
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        if (!frame_validate(frame.data(), frame_bytes) || frame_page_type(frame.data()) != page_type::kOverflowFrame ||
            !recovered_frame_identity_matches(*backing, page_type::kOverflowFrame,
                                              frame_u64(frame.data(), fh::kSelfpage_id), next)) {
            finish(Status::corruption("loaded backing is not a valid overflow frame"));
            return;
        }
        const OverflowFrameView decoded(frame.data(), frame_bytes);
        const size_t            chunk = decoded.chunk_len();
        if (chunk == 0 || chunk > overflow_chunk_cap(frame_bytes) || chunk > length - written) {
            finish(Status::corruption("overflow chunk disagrees with declared value length"));
            return;
        }
        std::memcpy(value->data() + written, decoded.payload().data(), chunk);
        record_work(metadata, TreeWorkCounter::MaterializeCopyBytes, chunk, cause);
        written += chunk;
        next = decoded.next_page_id();
        if (written == length) {
            finish(next == kInvalidPageId ? Status::Ok() : Status::corruption("overflow chain exceeds value length"));
            return;
        }
        if (next == kInvalidPageId) {
            finish(Status::corruption("overflow chain ends before declared value length"));
            return;
        }
        read_next();
    }

    static void start(void *context) noexcept
    {
        auto self = std::move(static_cast<OverflowLoad *>(context)->queued_owner);
        try {
            auto value  = detail::reserved_shared<LoadedValue>(self->metadata, self->metadata, self->length);
            self->value = std::shared_ptr<buffer>(value, &value->value);
            self->read_next();
        }
        catch (const std::bad_alloc &) {
            self->finish(Status::resource_exhausted("overflow read allocation failed"));
        }
        catch (...) {
            self->finish(Status::io_error("overflow resolution or submission threw"));
        }
    }

    static void decode(void *context) noexcept
    {
        auto self = std::move(static_cast<OverflowLoad *>(context)->queued_owner);
        try {
            self->decode_frame();
        }
        catch (const std::bad_alloc &) {
            self->finish(Status::resource_exhausted("overflow decode allocation failed"));
        }
        catch (...) {
            self->finish(Status::io_error("overflow resolution or submission threw"));
        }
    }
};
} // namespace

std::optional<size_t> overflow_read_allocation_bound(const PageLoadLimits &limits, size_t workers,
                                                     size_t retained_values, size_t value_bytes) noexcept
{
    using detail::AllocationBound;
    using detail::shared_allocation_bound;
    if (workers == 0 || limits.max_overflow_pages == 0) {
        return {};
    }
    const auto operations = AllocationBound(workers).add(1).value();
    const auto values     = AllocationBound(retained_values).add(workers).add(1).value();
    // The node allowance covers red/black tree links, color and alignment on
    // supported standard libraries, independently of the uint64 payload.
    const auto metadata = AllocationBound(shared_allocation_bound<OverflowLoad>())
                              .add(shared_allocation_bound<PageReadTicket>())
                              .add_product(limits.max_overflow_pages, shared_allocation_bound<uint64_t>())
                              .add(limits.max_blob_bytes)
                              .add(chunk_load_window_bound(limits))
                              .add(AlignedLoadBytes::kAlignment - 1)
                              .value();
    if (!operations || !metadata || !values) {
        return {};
    }
    return AllocationBound(value_bytes)
        .add_product(*operations, *metadata)
        .add_product(*values, shared_allocation_bound<LoadedValue>())
        .add_product(workers, limits.max_frame_bytes)
        .value();
}

void AsyncStorePageLoader::load_overflow(uint64_t head, uint64_t length, ValueComplete complete)
{
    load_overflow({}, head, length, std::move(complete));
}

void AsyncStorePageLoader::load_overflow(std::shared_ptr<const BackingRef> primary, uint64_t head, uint64_t length,
                                         ValueComplete complete)
{
    const auto started_at = tree_metrics_now_ns();
    const auto reject     = [&](Status status) {
        record_page_load_terminal(state_->budget, started_at, status);
        complete(std::move(status), {});
    };
    OverflowBackingResolver resolver = state_->overflow;
    if (primary && primary->overflow) {
        resolver = [primary](uint64_t pid, std::shared_ptr<const BackingRef> *out) {
            const auto found = primary->overflow->pages.find(pid);
            if (found == primary->overflow->pages.end() || !found->second.backing ||
                found->second.backing->backend_id != primary->backend_id ||
                found->second.backing->generation != primary->generation) {
                return Status::corruption("overflow is outside the captured primary backing");
            }
            *out = found->second.backing;
            return Status::Ok();
        };
    }
    if (!resolver) {
        reject(Status::not_supported("captured overflow backing resolver is unavailable"));
        return;
    }
    if (!state_->runtime || !state_->lane || head == kInvalidPageId || length == 0) {
        reject(Status::invalid_argument("overflow read requires a runtime and a nonempty valid chain"));
        return;
    }
    if (length > state_->limits.max_value_bytes || length > std::numeric_limits<size_t>::max()) {
        reject(Status::resource_exhausted("overflow value exceeds read budget"));
        return;
    }
    if (!acquire_page_load(state_->inflight, state_->limits.max_inflight)) {
        reject(Status::resource_exhausted("overflow read concurrency budget exhausted"));
        return;
    }
    std::shared_ptr<OverflowLoad> operation;
    try {
        operation          = detail::reserved_shared<OverflowLoad>(state_->budget.bytes, state_->budget, started_at);
        operation->store   = state_->store;
        operation->runtime = state_->runtime;
        operation->lane    = state_->lane;
        operation->loader_owner = state_;
        operation->inflight     = &state_->inflight;
        operation->limits       = state_->limits;
        operation->complete     = std::move(complete);
        operation->resolve      = std::move(resolver);
        operation->next         = head;
        operation->length       = static_cast<size_t>(length);
        queue_page_load(operation, &OverflowLoad::start);
    }
    catch (const std::bad_alloc &) {
        if (operation && operation->complete) {
            operation->finish(Status::resource_exhausted("overflow operation allocation failed"));
        }
        else {
            state_->inflight.fetch_sub(1, std::memory_order_release);
            reject(Status::resource_exhausted("overflow operation allocation failed"));
        }
    }
}
} // namespace crowdb::tree
