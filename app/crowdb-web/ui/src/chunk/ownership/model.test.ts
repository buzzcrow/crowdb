// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { describe, expect, it } from 'vitest';
import { ownershipLayers, parseSnapshot, projectOwners, type Snapshot } from './model';
import { Domain, type EnrichedStoreView, type Node } from '../../types';

const observation: Snapshot = { layer: 'storage', generation: '18446744073709551615', slot_count: 1024, owners: Array.from({ length: 1024 }, (_, slot) => `0/${slot % 2 + 1}`), source: 'test' };
const nodes = [{ id: 1, rack_id: 1 }, { id: 2, rack_id: 1 }, { id: 3, rack_id: 2 }] as Node[];
const stores = [{ store_id: '0', groups: [
  { group_id: '1', replicas: [{ node_id: 1 }, { node_id: 2 }] },
  { group_id: '2', replicas: [{ node_id: 3 }] },
] }] as EnrichedStoreView[];
describe('Chunk ownership projection', () => {
  it('selects the ownership map that each server provides', () => {
    expect(ownershipLayers({ domain: Domain.Chunk, type: 'Server', id: 'p', serviceType: 'paxos-kv' })).toEqual(['storage']);
    expect(ownershipLayers({ domain: Domain.Chunk, type: 'Server', id: 'c', serviceType: 'chunkdb' })).toEqual(['service']);
    expect(ownershipLayers({ domain: Domain.Chunk, type: 'Node', id: '1' })).toEqual(['service', 'storage']);
  });
  it('deduplicates replica groups and keeps disjoint assignments exact', () => {
    const owners = projectOwners(parseSnapshot(observation), { domain: Domain.Chunk, type: 'Rack', id: '1' }, nodes, [], stores);
    expect(owners).toEqual([
      { id: '0/1', label: 'S-0 / G-1', nodes: ['1', '2'], scope: 'inside' },
      { id: '0/2', label: 'S-0 / G-2', nodes: ['3'], scope: 'outside' },
    ]);
    expect(observation.owners.filter(id => id === owners[0].id)).toHaveLength(512);
    expect(observation.generation).toBe('18446744073709551615');
  });
  it('never treats missing membership as outside scope', () => {
    const owners = projectOwners(observation, { domain: Domain.Chunk, type: 'Node', id: '1' }, nodes, [], []);
    expect(owners.every(owner => owner.scope === 'unknown')).toBe(true);
  });
  it('keeps service instance identity independent from node identity', () => {
    const snapshot = { ...observation, layer: 'service' as const, owners: Array(1024).fill('18446744073709551615') };
    const owners = projectOwners(snapshot, { domain: Domain.Chunk, type: 'Node', id: '1' }, nodes,
      [{ id: 'chunkdb-18446744073709551615', node_id: 1, service_type: 'chunkdb', health: 'healthy' }], []);
    expect(owners[0].scope).toBe('inside');
    expect(owners[0].id).toBe('18446744073709551615');
  });
  it('projects a new owner observation without carrying an old assignment', () => {
    const scope = { domain: Domain.Chunk, type: 'Node' as const, id: '1' };
    const changed = { ...observation, generation: '2', owners: Array(1024).fill('0/2') };
    expect(projectOwners(changed, scope, nodes, [], stores)).toEqual([
      { id: '0/2', label: 'S-0 / G-2', nodes: ['3'], scope: 'outside' },
    ]);
  });
  it('rejects incomplete and malformed snapshots', () => {
    expect(() => parseSnapshot({ ...observation, owners: ['0/1'] })).toThrow();
    expect(() => parseSnapshot({ ...observation, owners: Array(1024).fill('0/0') })).toThrow();
  });
});
