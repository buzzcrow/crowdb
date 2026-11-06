// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"

#include "btree/tree_work_counters.h"

#include <algorithm>
#include <cstring>

namespace crowdb::tree
{
bool PageKeyRange::contains(Slice key) const
{
    return (!lower.has_value() || key >= Slice(*lower)) && (!upper.has_value() || key < Slice(*upper));
}

bool PageKeyRange::empty() const
{
    return lower.has_value() && upper.has_value() && Slice(*lower) >= Slice(*upper);
}

bool PageKeyRange::overlaps(const PageKeyRange &other) const
{
    return !empty() && !other.empty() &&
           (!upper.has_value() || !other.lower.has_value() || Slice(*upper) > Slice(*other.lower)) &&
           (!lower.has_value() || !other.upper.has_value() || Slice(*lower) < Slice(*other.upper));
}

std::optional<Slice> PageKeyRange::lower_slice() const
{
    return lower.has_value() ? std::optional<Slice>(Slice(*lower)) : std::nullopt;
}

std::optional<Slice> PageKeyRange::upper_slice() const
{
    return upper.has_value() ? std::optional<Slice>(Slice(*upper)) : std::nullopt;
}

Slice EntryRef::key() const
{
    return key_arena != nullptr ? Slice(key_arena->data() + key_offset, key_length) : Slice();
}

Slice EntryRef::value() const
{
    if (owner == nullptr || (owner->flags & (kFlagTombstone | kFlagOverflow)) != 0) {
        return {};
    }
    return owner->cell.ownership() == buffer::mode::kExternal ? owner->cell.slice()
                                                              : CellView(owner->cell.slice()).value();
}

uint64_t EntryRef::slot() const
{
    return owner != nullptr ? owner->slot : 0;
}

uint8_t EntryRef::flags() const
{
    return owner != nullptr ? owner->flags : 0;
}

Status KeyArenaBuilder::append(Slice key, std::shared_ptr<const CellVersion> owner, EntryRef *out)
{
    if (out == nullptr || owner == nullptr) {
        return Status::invalid_argument("entry needs a cell owner and output");
    }
    if (entries_ >= limits_.max_entries || key.size() > limits_.max_key_bytes - bytes_ ||
        limits_.key_block_bytes == 0) {
        return Status::resource_exhausted("overlay key/entry budget exhausted");
    }
    if (block_ == nullptr || key.size() > block_->size() - used_) {
        if (key.size() > limits_.max_key_bytes - allocated_) {
            return Status::resource_exhausted("overlay arena capacity budget exhausted");
        }
        const size_t capacity =
            std::max(key.size(), std::min(limits_.key_block_bytes, limits_.max_key_bytes - allocated_));
        block_ = detail::reserved_shared<EntryKeyArena>(metadata_, capacity, metadata_);
        allocated_ += capacity;
        used_ = 0;
    }
    if (!key.empty()) {
        std::memcpy(block_->data() + used_, key.data(), key.size());
        record_work(metadata_, TreeWorkCounter::KeyCopyBytes, key.size());
    }
    *out = {.key_arena = block_, .key_offset = used_, .key_length = key.size(), .owner = std::move(owner)};
    used_ += key.size();
    bytes_ += key.size();
    ++entries_;
    return Status::Ok();
}

Status OverlayBatch::build(uint64_t id, std::vector<EntryRef> entries, const RunLimits &limits,
                           std::shared_ptr<const OverlayBatch> *out, std::shared_ptr<AllocationBudget> metadata)
{
    EntryStorage owned(std::make_move_iterator(entries.begin()), std::make_move_iterator(entries.end()),
                       EntryAllocator(std::move(metadata)));
    return build_owned(id, std::move(owned), limits, out);
}

Status OverlayBatch::build_owned(uint64_t id, EntryStorage entries, const RunLimits &limits,
                                 std::shared_ptr<const OverlayBatch> *out)
{
    if (out == nullptr) {
        return Status::invalid_argument("overlay needs an output");
    }
    if (entries.size() > limits.max_entries) {
        return Status::resource_exhausted("overlay entry budget exhausted");
    }
    auto metadata = entries.get_allocator().owner();
    auto run      = detail::reserved_shared<OverlayBatch>(metadata, metadata);
    for (size_t i = 0; i < entries.size(); ++i) {
        const auto &entry = entries[i];
        if (entry.owner == nullptr || entry.key_arena == nullptr || entry.key_offset > entry.key_arena->size() ||
            entry.key_length > entry.key_arena->size() - entry.key_offset) {
            return Status::invalid_argument("overlay entry has no valid independent owner");
        }
        if (i != 0 && entries[i - 1].key() >= entry.key()) {
            return Status::invalid_argument("overlay entries must be sorted and unique");
        }
        if (entry.key_length > limits.max_key_bytes - run->key_bytes_) {
            return Status::resource_exhausted("overlay key budget exhausted");
        }
        run->key_bytes_ += entry.key_length;
        run->min_slot_            = std::min(run->min_slot_, entry.slot());
        run->max_slot_            = std::max(run->max_slot_, entry.slot());
        run->has_overflow_values_ = run->has_overflow_values_ || (entry.flags() & kFlagOverflow) != 0;
    }
    run->id_      = id;
    run->entries_ = std::move(entries);
    *out          = std::move(run);
    return Status::Ok();
}

size_t OverlayBatch::lower_bound(Slice key) const
{
    return static_cast<size_t>(
        std::lower_bound(entries_.begin(), entries_.end(), key,
                         [](const EntryRef &entry, Slice wanted) { return entry.key() < wanted; }) -
        entries_.begin());
}

const EntryRef *OverlayBatch::find(Slice key) const
{
    const size_t index = lower_bound(key);
    return index < entries_.size() && entries_[index].key() == key ? &entries_[index] : nullptr;
}

Status OverlayBuilder::append(Slice key, std::shared_ptr<const CellVersion> owner)
{
    if (!entries_.empty() && entries_.back().key() >= key) {
        return Status::invalid_argument("overlay entries must be sorted and unique");
    }
    EntryRef entry;
    auto     status = arena_.append(key, std::move(owner), &entry);
    return status.ok() ? append(std::move(entry)) : status;
}

Status OverlayBuilder::append(EntryRef entry)
{
    if (entry.owner == nullptr || entry.key_arena == nullptr || entry.key_offset > entry.key_arena->size() ||
        entry.key_length > entry.key_arena->size() - entry.key_offset ||
        (!entries_.empty() && entries_.back().key() >= entry.key())) {
        return Status::invalid_argument("overlay entry must own a valid key and be strictly ordered");
    }
    if (entries_.size() >= limits_.max_entries || entry.key_length > limits_.max_key_bytes - key_bytes_) {
        return Status::resource_exhausted("overlay key/entry budget exhausted");
    }
    key_bytes_ += entry.key_length;
    entries_.push_back(std::move(entry));
    return Status::Ok();
}

Status OverlayBuilder::finish(uint64_t id, std::shared_ptr<const OverlayBatch> *out)
{
    auto status = OverlayBatch::build_owned(id, std::move(entries_), limits_, out);
    if (status.ok()) {
        key_bytes_ = 0;
        arena_     = KeyArenaBuilder(limits_, metadata_);
    }
    return status;
}

bool BaseRef::valid() const
{
    return resident != nullptr || (backing != nullptr && backing->readable());
}

LogicalCell LogicalCell::from_entry(const EntryRef &entry)
{
    return {.owner   = entry.owner,
            .slot    = entry.slot(),
            .flags   = entry.flags(),
            .value   = entry.value(),
            .encoded = entry.owner->cell.ownership() == buffer::mode::kExternal ? Slice() : entry.owner->cell.slice()};
}

LogicalCell LogicalCell::from_base(const std::shared_ptr<const LeafBase> &base, size_t index,
                                   std::shared_ptr<const BackingRef> backing)
{
    const CellView view(base->cell(index));
    return {.owner   = base,
            .slot    = view.slot(),
            .flags   = view.flags(),
            .value   = view.value(),
            .encoded = view.raw(),
            .backing = std::move(backing)};
}

LogicalLookup PageState::lookup(Slice key, uint64_t frontier) const
{
    LogicalLookup result;
    if (!range.contains(key)) {
        return result;
    }
    size_t winner_source = 0;
    auto   consider      = [&](LogicalCell candidate, size_t source) {
        if (candidate.slot <= frontier && (result.cell.owner == nullptr || candidate.slot > result.cell.slot)) {
            result.cell   = std::move(candidate);
            winner_source = source;
        }
    };
    for (size_t i = 0; i < overlays.size(); ++i) {
        const auto &run = overlays[i];
        if (run == nullptr) {
            result.state  = LogicalLookupState::kInvalid;
            result.status = Status::corruption("logical overlay has no owner");
            return result;
        }
        if (const auto *entry = run->find(key); entry != nullptr) {
            consider(LogicalCell::from_entry(*entry), i);
        }
    }
    for (size_t i = 0; i < bases.size(); ++i) {
        const auto &base = bases[i];
        if (!base.range.contains(key)) {
            continue;
        }
        if (!base.valid()) {
            result.state      = LogicalLookupState::kInvalid;
            result.status     = Status::corruption("logical base has neither resident nor reloadable owner");
            result.base_index = i;
            return result;
        }
        if (base.resident != nullptr) {
            const int index = base.resident->find(key);
            if (index >= 0) {
                consider(LogicalCell::from_base(base.resident, static_cast<size_t>(index), base.backing),
                         overlays.size() + i);
            }
        }
    }
    for (size_t i = 0; i < bases.size(); ++i) {
        const auto &base = bases[i];
        if (base.range.contains(key) && !base.resident &&
            (result.cell.owner == nullptr || !base.max_slot.has_value() || result.cell.slot < *base.max_slot ||
             (result.cell.slot == *base.max_slot && winner_source > overlays.size() + i))) {
            result.state      = LogicalLookupState::kNeedsBase;
            result.base_index = i;
            return result;
        }
    }
    if (result.cell.owner != nullptr) {
        result.state = result.cell.tombstone() ? LogicalLookupState::kTombstone : LogicalLookupState::kValue;
    }
    return result;
}

MemTableRangeMerge::MemTableRangeMerge(std::initializer_list<std::shared_ptr<MemTable>> sources, PageKeyRange range,
                                       uint64_t frontier, uint64_t floor, RunLimits limits, size_t max_sources,
                                       std::shared_ptr<AllocationBudget> metadata)
    : MemTableRangeMerge(std::span<const std::shared_ptr<MemTable>>(sources), std::move(range), frontier, floor, limits,
                         max_sources, std::move(metadata))
{
}

MemTableRangeMerge::MemTableRangeMerge(std::span<const std::shared_ptr<MemTable>> sources, PageKeyRange range,
                                       uint64_t frontier, uint64_t floor, RunLimits limits, size_t max_sources,
                                       std::shared_ptr<AllocationBudget> metadata)
    : owners_(detail::ReservationAllocator<std::shared_ptr<MemTable>>(metadata)),
      guards_(detail::ReservationAllocator<EpochManager::Guard>(metadata)),
      range_(std::move(range)),
      limits_(limits),
      metadata_(std::move(metadata)),
      arena_(limits, metadata_),
      cursors_(detail::ReservationAllocator<ConcurrentSkipList::Cursor>(metadata_)),
      sources_(detail::ReservationAllocator<MergeSource>(metadata_)),
      merge_(metadata_)
{
    if (sources.size() > max_sources || sources.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
        status_ = Status::resource_exhausted("MemTable merge source budget exhausted");
        return;
    }
    owners_.assign(sources.begin(), sources.end());
    guards_.reserve(owners_.size());
    cursors_.reserve(owners_.size());
    sources_.reserve(owners_.size());
    for (const auto &owner : owners_) {
        if (owner == nullptr) {
            status_ = Status::invalid_argument("MemTable merge source has no owner");
            return;
        }
        guards_.push_back(owner->read_guard(metadata_));
        cursors_.push_back(owner->range_cursor(range_.lower_slice(), range_.upper_slice(), frontier, floor));
        sources_.push_back({.kind = MergeSource::kL0, .l0 = &cursors_.back(), .l1 = nullptr});
    }
    if (!sources_.empty()) {
        merge_.init(sources_.data(), static_cast<int>(sources_.size()));
    }
}

bool MemTableRangeMerge::valid() const
{
    return status_.ok() && !sources_.empty() && merge_.winner_valid();
}

Slice MemTableRangeMerge::key() const
{
    return valid() ? sources_[static_cast<size_t>(merge_.winner())].key() : Slice();
}

Status MemTableRangeMerge::next(EntryRef *out)
{
    if (!status_.ok()) {
        return status_;
    }
    if (!valid()) {
        return Status::not_found("MemTable range exhausted");
    }
    auto &cursor = cursors_[static_cast<size_t>(merge_.winner())];
    // Acquire the independent owner before any source cursor advances.
    auto status = arena_.append(cursor.key(), cursor.cell_owner(), out);
    if (!status.ok()) {
        return status;
    }
    const Slice key = out->key();
    do {
        ++candidates_;
        merge_.advance_winner();
    } while (merge_.winner_valid() && sources_[static_cast<size_t>(merge_.winner())].key() == key);
    return Status::Ok();
}

void MemTableRangeMerge::rotate_arena()
{
    arena_ = KeyArenaBuilder(limits_, metadata_);
}
} // namespace crowdb::tree
