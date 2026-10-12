// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { describe, expect, it } from 'vitest';
import { NodeHealth, ProcState } from '../types';
import { diskdbHealthForNodes, diskdbInstancesForNodes } from './observations';

const node = { id: 1, diskdb_server: { endpoint: 'tcp://10.0.0.2:7102', health: NodeHealth.Down, state: ProcState.Stopped } };
const server = { service_type: 'diskdb', node_id: 1, health: 'up', endpoint: '10.0.0.2:7102' };

describe('DiskDB runtime observation', () => {
  it('keeps live service health when the node snapshot reports a stopped service', () => {
    expect(diskdbHealthForNodes([server], [node]).get(1)).toBe('up');
    expect(diskdbHealthForNodes([{ ...server, health: 'down' }], [
      { ...node, diskdb_server: { ...node.diskdb_server, health: NodeHealth.Up } },
    ]).get(1)).toBe('down');
    expect(diskdbHealthForNodes([], [node]).get(1)).toBe('down');
  });

  it('keeps equal ports on different node IPs associated with their own instances', () => {
    const instances = [
      { instance_id: 'first', rpc_endpoint: '10.0.0.2:7102' },
      { instance_id: 'second', rpc_endpoint: 'tcp://10.0.0.3:7102' },
    ];
    const actual = diskdbInstancesForNodes([
      server, { ...server, node_id: 2, endpoint: '10.0.0.3:7102' },
    ], [node], instances);
    expect([...actual]).toEqual([[1, 'first'], [2, 'second']]);
  });

  it('uses the observed endpoint when persisted node configuration is stale', () => {
    const actual = diskdbInstancesForNodes([server], [
      { ...node, diskdb_server: { ...node.diskdb_server, endpoint: '10.0.0.3:7102' } },
    ], [{ instance_id: 'actual', rpc_endpoint: 'tcp://10.0.0.2:7102' }]);
    expect(actual.get(1)).toBe('actual');
  });

  it('does not guess an owner for conflicting endpoint registrations or invalid addresses', () => {
    const actual = diskdbInstancesForNodes([server], [node], [
      { instance_id: 'first', rpc_endpoint: '10.0.0.2:7102' },
      { instance_id: 'second', rpc_endpoint: 'tcp://10.0.0.2:7102' },
      { instance_id: 'invalid', rpc_endpoint: 'unusable' },
    ]);
    expect(actual.size).toBe(0);
  });
});
