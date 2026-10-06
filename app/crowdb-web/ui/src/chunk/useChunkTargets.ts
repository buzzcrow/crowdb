// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import type { SelectedEntity } from '../contexts/SelectionContext';
import type { EnrichedStoreView, Node } from '../types';

interface Target { storeId: number; groupId: number }

export function useChunkTargets(ownership: SelectedEntity | null, nodes: Node[], stores: EnrichedStoreView[]): Target[] {
  const targets = (() => {
    if (!ownership) return [];
    if (ownership.type === 'Group') {
      const storeId = Number(ownership.parentIds?.store_id);
      const groupId = Number(ownership.id);
      return Number.isFinite(storeId) && Number.isFinite(groupId) ? [{ storeId, groupId }] : [];
    }
    if (ownership.type === 'Store') {
      const store = stores.find(candidate => String(candidate.store_id) === String(ownership.id));
      return store?.groups.map(group => ({ storeId: Number(store.store_id), groupId: Number(group.group_id) }))
        .filter(target => Number.isFinite(target.storeId) && Number.isFinite(target.groupId)) ?? [];
    }
    const nodeIds = ownership.type === 'Node'
      ? [Number(ownership.id)]
      : ownership.type === 'Rack'
        ? nodes.filter(node => String(node.rack_id) === String(ownership.id)).map(node => Number(node.id))
        : ownership.type === 'Datacenter' ? nodes.map(node => Number(node.id)) : [];
    if (!nodeIds.length && ownership.type !== 'Datacenter') return [];
    return stores.flatMap(store => store.groups
      .filter(group => ownership.type === 'Datacenter' || group.replicas.some(replica => nodeIds.includes(Number(replica.node_id))))
      .map(group => ({ storeId: Number(store.store_id), groupId: Number(group.group_id) })));
  })();
  // Polling replaces topology objects even when the query targets are unchanged.
  // Keep request dependencies stable so a refresh cannot abort a page in flight.
  const key = JSON.stringify(targets);
  return useMemo(() => JSON.parse(key) as Target[], [key]);
}
