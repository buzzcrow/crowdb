// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// B+tree skip-list implementation.

#include "crowdb-tree/memtable/skip_list.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <new>
#include <random>

namespace crowdb::tree
{

namespace
{
// Branching probability for skip-list height (p=0.25, RocksDB/LevelDB default).
constexpr uint32_t kBranching = 4; // 1/4 probability of height increase
} // namespace

// --- Node allocation ---

Node *ConcurrentSkipList::alloc_node(uint32_t height, Slice key)
{
    size_t sz = Node::alloc_size(height, key.size());
    void  *p  = ::operator new(sz);
    // Construct the node metadata with
    // placement new — the atomics need proper initialization, not just raw
    // memory. The tower (next_ptr) is constructed separately below since it
    // lives beyond sizeof(Node).
    Node *n     = new (p) Node{};
    n->height_  = height;
    n->key_len_ = static_cast<uint32_t>(key.size());
    // Construct the tower (next_[0..height-1]) with placement new.
    for (uint32_t i = 0; i < height; ++i) {
        new (n->next_ptr(i)) std::atomic<Node *>(nullptr);
    }
    // Copy the key inline.
    if (!key.empty()) {
        std::memcpy(const_cast<char *>(n->key_data()), key.data(), key.size());
    }
    return n;
}

void ConcurrentSkipList::free_node(void *p)
{
    Node *n = static_cast<Node *>(p);
    // Destroy the atomic objects in the tower, then the Node base, then free.
    for (uint32_t i = 0; i < n->height_; ++i) {
        n->next_ptr(i)->~atomic();
    }
    n->~Node();
    ::operator delete(static_cast<void *>(n));
}

// --- ConcurrentSkipList ---

ConcurrentSkipList::ConcurrentSkipList(EpochManager *epoch)
    : owned_epoch_(epoch == nullptr ? std::make_unique<EpochManager>() : nullptr),
      epoch_(epoch == nullptr ? owned_epoch_.get() : epoch)
{
    head_ = alloc_node(kMaxHeight, Slice());
}

ConcurrentSkipList::~ConcurrentSkipList()
{
    Node *node = head_->next(0);
    while (node != nullptr) {
        Node *next    = node->next(0);
        node->destroy = [](EpochManager::Deferred *entry) noexcept {
            auto *n = static_cast<Node *>(entry);
            delete n->versions_.load(std::memory_order_relaxed);
            free_node(n);
        };
        epoch_->defer(node);
        node = next;
    }
    free_node(head_);
}

uint32_t ConcurrentSkipList::random_height()
{
    static thread_local std::minstd_rand rng{std::random_device{}()};
    uint32_t                             h = 1;
    while (h < kMaxHeight && (rng() % kBranching) == 0) {
        ++h;
    }
    return h;
}

Node *ConcurrentSkipList::find_ge(Slice key, Node **prev, Node **successors) const
{
    Node *x         = head_;
    Node *candidate = nullptr;
    for (int h = static_cast<int>(kMaxHeight) - 1; h >= 0; --h) {
        Node *next = x->next(h);
        while (next != nullptr && next->key_slice().compare(key) < 0) {
            x    = next;
            next = x->next(h);
        }
        candidate = next;
        if (prev != nullptr) {
            prev[h] = x;
        }
        if (successors != nullptr) {
            successors[h] = next;
        }
    }
    return candidate;
}

Node *ConcurrentSkipList::find_or_insert(Slice key, VersionSet *versions, bool *inserted)
{
    std::array<Node *, kMaxHeight> prev{};
    std::array<Node *, kMaxHeight> next{};
    Node                          *candidate = nullptr;
#ifdef CROWDB_TREE_TEST_UTIL
    if (test_hook_ != nullptr) {
        test_hook_(test_context_, PausePoint::kBeforeSearch, key);
    }
#endif
    for (;;) {
        find_ge(key, prev.data(), next.data());
        if (next[0] != nullptr && next[0]->key_slice().compare(key) == 0) {
            if (candidate != nullptr) {
                free_node(candidate);
            }
            *inserted = false;
            return next[0];
        }
        if (candidate == nullptr) {
            candidate = alloc_node(random_height(), key);
            candidate->allocation.set(epoch_->memtable_allocation(), Node::alloc_size(candidate->height_, key.size()));
            candidate->versions_.store(versions, std::memory_order_relaxed);
        }
        candidate->set_next(0, next[0]);
        if (prev[0]->next_ptr(0)->compare_exchange_strong(next[0], candidate, std::memory_order_acq_rel)) {
            count_.fetch_add(1, std::memory_order_relaxed);
            bytes_.fetch_add(key.size() + versions->bytes, std::memory_order_relaxed);
#ifdef CROWDB_TREE_TEST_UTIL
            if (test_hook_ != nullptr) {
                test_hook_(test_context_, PausePoint::kAfterLevelZero, key);
            }
#endif
            link_upper(candidate);
            *inserted = true;
            return candidate;
        }
    }
}

void ConcurrentSkipList::link_upper(Node *node)
{
    std::array<Node *, kMaxHeight> prev{};
    std::array<Node *, kMaxHeight> next{};
    for (uint32_t level = 1; level < node->height_; ++level) {
        do {
            find_ge(node->key_slice(), prev.data(), next.data());
            node->set_next(level, next[level]);
        } while (!prev[level]->next_ptr(level)->compare_exchange_strong(next[level], node, std::memory_order_acq_rel));
    }
    uint32_t height = max_height_.load(std::memory_order_relaxed);
    while (height < node->height_ && !max_height_.compare_exchange_weak(height, node->height_)) {
    }
}

const CellVersion *ConcurrentSkipList::find(Slice key) const
{
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h); // acquire
        while (next != nullptr && next->key_slice().compare(key) < 0) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    Node *cand = x->next(0); // acquire
    if (cand != nullptr && cand->key_slice().compare(key) == 0) {
        return cand->versions_.load(std::memory_order_acquire)->current.get();
    }
    return nullptr;
}

ConcurrentSkipList::Cursor ConcurrentSkipList::cursor(Slice start_after) const
{
    if (start_after.empty()) {
        // First live node.
        Node *n = head_->next(0);
        return Cursor(n);
    }
    // Find first node with key > start_after.
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h);
        while (next != nullptr && next->key_slice().compare(start_after) <= 0) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    Node *n = x->next(0);
    return Cursor(n);
}

ConcurrentSkipList::Cursor ConcurrentSkipList::cursor_from(Slice start_key, bool inclusive) const
{
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h);
        while (next != nullptr &&
               (inclusive ? next->key_slice().compare(start_key) < 0 : next->key_slice().compare(start_key) <= 0)) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    Node *n = x->next(0);
    return Cursor(n);
}

ConcurrentSkipList::Cursor ConcurrentSkipList::cursor_reverse(Slice start_key, bool has_start_bound,
                                                              bool inclusive) const
{
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h);
        while (next != nullptr && (!has_start_bound || (inclusive ? next->key_slice().compare(start_key) <= 0
                                                                  : next->key_slice().compare(start_key) < 0))) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    return Cursor(x == head_ ? nullptr : x);
}

void ConcurrentSkipList::Cursor::select()
{
    candidate_ = nullptr;
    while (cur_ != nullptr) {
        auto *versions = cur_->versions_.load(std::memory_order_acquire);
        candidate_     = versions->at(frontier_);
        if (candidate_ != nullptr && (floor_ == 0 || candidate_->slot > floor_)) {
            return;
        }
        cur_ = cur_->next(0);
    }
}

void ConcurrentSkipList::Cursor::advance()
{
    if (cur_ != nullptr) {
        cur_ = cur_->next(0);
    }
    select();
}

ConcurrentSkipList::Cursor ConcurrentSkipList::prefix_cursor(uint64_t frontier, uint64_t floor) const
{
    return Cursor(head_->next(0), frontier, floor);
}

} // namespace crowdb::tree
