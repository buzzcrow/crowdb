// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/btree/leaf_cursor.h"
#include "crowdb-tree/memtable/skip_list.h"

namespace crowdb::tree
{
struct MergeSource
{
    enum Kind : uint8_t { kL0, kL1 };

    Kind                        kind = kL0;
    ConcurrentSkipList::Cursor *l0   = nullptr;
    LeafChainCursor            *l1   = nullptr;

    [[nodiscard]] bool valid() const
    {
        if (kind == kL0) {
            return l0 != nullptr && l0->valid();
        }
        return l1 != nullptr && l1->valid();
    }

    [[nodiscard]] Slice key() const
    {
        if (kind == kL0) {
            return l0->key();
        }
        return l1->key();
    }

    [[nodiscard]] uint64_t slot() const
    {
        if (kind == kL0) {
            const CellVersion *cv = l0->cell_version();
            return cv != nullptr ? cv->slot : 0;
        }
        return CellView{l1->cell()}.slot();
    }

    void advance() const
    {
        if (kind == kL0) {
            l0->advance();
        }
        else {
            l1->next();
        }
    }

    void prefetch_next() const
    {
        if (kind == kL0) {
            l0->prefetch_next();
        }
    }
};

// R58: loser tree for k-way merge (k > 2). O(log k) compares per merge step
// instead of the 2-pass O(2k) scan. The match function: lower key wins; on
// key tie, higher slot wins; on key+slot tie, lower source index wins
// (deterministic, matching the original iteration order). Exhausted sources
// always lose, so the tree never needs rebuilding when a cursor exhausts —
// it stays in the tree and naturally sinks to the bottom.
class LoserTree
{
  public:
    void init(MergeSource *sources, int k)
    {
        sources_ = sources;
        k_       = k;
        losers_.assign(static_cast<size_t>(k), -1);
        for (int i = 0; i < k; ++i) {
            insert(i);
        }
    }

    [[nodiscard]] int winner() const
    {
        return losers_[0];
    }

    // Advance the winner's cursor and sift its new key up the tree.
    void advance_winner()
    {
        int w = losers_[0];
        sources_[w].advance();
        replay(w);
    }

    // Advance the current winner without emitting (collision drain: the
    // winner's key matches the just-emitted key, so it's a duplicate).
    void drain_winner()
    {
        int w = losers_[0];
        sources_[w].advance();
        replay(w);
    }

    // Replay a source whose key changed externally (L1 refilled a new leaf).
    void replay_source(int src)
    {
        replay(src);
    }

    [[nodiscard]] bool winner_valid() const
    {
        int w = losers_[0];
        return w >= 0 && sources_[w].valid();
    }

  private:
    MergeSource     *sources_ = nullptr;
    int              k_       = 0;
    std::vector<int> losers_; // [0] = winner, [1..k-1] = losers

    // a wins over b: lower key; tie → higher slot; tie → lower index.
    [[nodiscard]] bool less(int a, int b) const
    {
        bool va = sources_[a].valid();
        bool vb = sources_[b].valid();
        if (!va && !vb) {
            return a < b;
        }
        if (!va) {
            return false;
        }
        if (!vb) {
            return true;
        }
        int cmp = sources_[a].key().compare(sources_[b].key());
        if (cmp != 0) {
            return cmp < 0;
        }
        uint64_t sa = sources_[a].slot();
        uint64_t sb = sources_[b].slot();
        if (sa != sb) {
            return sa > sb;
        }
        return a < b;
    }

    void insert(int src)
    {
        int parent = (src + k_) / 2;
        while (parent >= 1) {
            if (losers_[parent] == -1) {
                losers_[parent] = src;
                return;
            }
            if (less(losers_[parent], src)) {
                std::swap(src, losers_[parent]);
            }
            parent /= 2;
        }
        losers_[0] = src;
    }

    void replay(int src)
    {
        int parent = (src + k_) / 2;
        while (parent >= 1) {
            if (losers_[parent] == -1) {
                losers_[parent] = src;
                return;
            }
            if (less(losers_[parent], src)) {
                std::swap(src, losers_[parent]);
            }
            parent /= 2;
        }
        losers_[0] = src;
    }
};

} // namespace crowdb::tree
