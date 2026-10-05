// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { ServerSummary } from '../../api';
import type { SelectedEntity } from '../../contexts/SelectionContext';
import type { EnrichedStoreView, Node } from '../../types';

export type Layer = 'service' | 'storage';
export interface Snapshot { layer: Layer; generation: string; slot_count: number; owners: string[]; source: string }
export interface Owner { id: string; label: string; nodes: string[]; scope: 'inside' | 'outside' | 'unknown' }
export function ownershipLayers(selection: SelectedEntity): Layer[] {
  if (selection.type === 'Server') {
    if (selection.serviceType === 'paxos-kv') return ['storage'];
    if (selection.serviceType === 'chunkdb') return ['service'];
  }
  if (selection.type === 'Store' || selection.type === 'Group') return ['storage'];
  return ['service', 'storage'];
}
export const ownerColor = (id: string) => {
  let hash = 0;
  for (const char of id) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return `hsl(${(hash * 137.508) % 360} 34% 38%)`;
};
export function projectOwners(snapshot: Snapshot, selection: SelectedEntity, nodes: Node[], servers: ServerSummary[], stores: EnrichedStoreView[]): Owner[] {
  const parent = selection.type === 'Datacenter' || selection.type === 'Rack' || selection.type === 'Node';
  const scopedNodes = new Set(nodes.filter(node => selection.type === 'Datacenter' ||
    (selection.type === 'Rack' ? String(node.rack_id) === selection.id : String(node.id) === selection.id)).map(node => String(node.id)));
  return [...new Set(snapshot.owners)].map(id => {
    let members: string[];
    let label: string;
    let scope: Owner['scope'];
    if (snapshot.layer === 'service') {
      const instances = servers.filter(server => server.service_type === 'chunkdb' && server.id?.replace(/^chunkdb-/, '') === id);
      members = [...new Set(instances.flatMap(server => server.node_id == null ? [] : [String(server.node_id)]))];
      label = `CDB-${id}`;
      const selectedNode = selection.type === 'Server' ? String(selection.parentIds?.node_id ?? '') : '';
      scope = parent ? selection.type === 'Datacenter' ? 'inside' : !members.length ? 'unknown' : members.some(node => scopedNodes.has(node)) ? 'inside' : 'outside'
        : selection.type === 'Server' && selection.serviceType === 'chunkdb' && selection.id.replace(/^chunkdb-/, '') === id ? 'inside'
          : selectedNode && members.includes(selectedNode) ? 'inside' : 'outside';
    } else {
      const [storeId, groupId] = id.split('/');
      const group = stores.find(store => String(store.store_id) === storeId)?.groups.find(group => String(group.group_id) === groupId);
      members = [...new Set(group?.replicas.map(replica => String(replica.node_id)) ?? [])];
      label = `S-${storeId} / G-${groupId}`;
      const selectedNode = selection.type === 'Server' ? String(selection.parentIds?.node_id ?? '') : '';
      scope = parent ? selection.type === 'Datacenter' ? 'inside' : !members.length ? 'unknown' : members.some(node => scopedNodes.has(node)) ? 'inside' : 'outside'
        : selection.type === 'Group' && String(selection.parentIds?.store_id) === storeId && selection.id === groupId ? 'inside'
          : selection.type === 'Store' && selection.id === storeId ? 'inside'
            : selectedNode && members.includes(selectedNode) ? 'inside' : 'outside';
    }
    return { id, label, nodes: members, scope };
  });
}
export function parseSnapshot(value: Snapshot): Snapshot {
  if (!value || !['service', 'storage'].includes(value.layer) || !/^\d+$/.test(value.generation) ||
    value.slot_count !== 1024 || !Array.isArray(value.owners) || value.owners.length !== 1024 ||
    value.owners.some(id => typeof id !== 'string' || !(value.layer === 'service' ? /^[1-9]\d*$/ : /^\d+\/[1-9]\d*$/).test(id))) {
    throw new Error('Invalid ownership observation');
  }
  return value;
}
