// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent, within } from '@testing-library/react';
import { DomainProvider } from '../contexts/DomainContext';
import { SelectionProvider } from '../contexts/SelectionContext';
import { Sidebar } from './Sidebar';
import {
  Domain,
  NodeHealth,
  ProcState,
  Rack,
  CrowdbKVServerView,
  EnrichedStoreView,
} from '../types';
import type { NodeDiskGroups } from '../data/useCapacityTree';

const rack: Rack = {
  id: 1,
  name: 'r1',
  nodes: [
    { id: 10, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' }, has_server: true, kv_server: { mgmt_url: 'http://127.0.0.1:19910', rpc_url: 'http://127.0.0.1:19920', state: ProcState.Running, health: NodeHealth.Up, last_seen_ms: 0 } },
    { id: 11, rack_id: 1, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } },
  ],
};

const servers: CrowdbKVServerView[] = [
  {
    id: 'n10-kv',
    node_id: 10,
    rack_id: 1,
    host: '127.0.0.1',
    process: { mgmt_url: 'http://127.0.0.1:19910', rpc_url: 'http://127.0.0.1:19920', state: ProcState.Running, health: NodeHealth.Up, last_seen_ms: 0 },
    rest_port: 19910,
    rpc_port: 19920,
  },
];

const stores: EnrichedStoreView[] = [
  {
    store_id: '7',
    nodes: [10, 11],
    groups: [
      {
        store_id: '7',
        group_id: '70',
        replicas: [
          { replica_id: '700', node_id: 10, store_id: '7', group_id: '70', role: 'leader' as any, state: 'running' as any, engine_healthy: true },
          { replica_id: '701', node_id: 11, store_id: '7', group_id: '70', role: 'follower' as any, state: 'running' as any, engine_healthy: true },
        ],
        state: 'healthy' as any,
      },
    ],
  },
];

const diskGroups: Record<number, NodeDiskGroups> = {
  10: {
    diskGroups: [{ id: 100, rack_id: 1, node_id: 10, name: 'Physical Group' }],
    disksByDg: {
      100: [{ disk_id: '0123456789abcdef-0123456789abcdef', disk_group_id: 100, rack_id: 1, node_id: 10, disk_type: 'Hdd', capacity_bytes: 0, zone_size_bytes: 0, unit_size_bytes: 0 }],
    },
  },
};

function renderSidebar(domain: Domain, props: Record<string, unknown> = {}) {
  return render(
    <DomainProvider initialDomain={domain}>
      <SelectionProvider>
        <Sidebar
          racks={[rack]}
          servers={servers}
          stores={stores}
          nodeHealthById={{ '10': NodeHealth.Up, '11': NodeHealth.Unknown }}
          nodeDiskGroups={diskGroups}
          diskdbInstances={[{ instance_id: '10', rpc_endpoint: '127.0.0.1:29922', last_heartbeat_ms: 1, owned_dg_ids: [100], group_usages: [] }]}
          diskdbNodeIds={new Set([10])}
          diskdbHealthById={new Map([[10, 'up']])}
          diskdbInstanceIdByNodeId={new Map([[10, '10']])}
          {...props}
        />
      </SelectionProvider>
    </DomainProvider>,
  );
}

describe('Sidebar · Cluster tree projection', () => {
  it('renders rack → node → PKV server under the node', () => {
    const { getByText, queryByText } = renderSidebar(Domain.Cluster);
    expect(getByText(/R-1/)).toBeTruthy();
    expect(getByText('N-10', { exact: true })).toBeTruthy();
    // PKV server appears as a child of node 10, not as a top-level item.
    expect(getByText('PKV-10', { exact: true })).toBeTruthy();
    // Node 11 has no PKV server — no PKV-11 item.
    expect(queryByText('PKV-11')).toBeNull();
  });

  it('keeps a stopped registered PKV service visible for restart actions', () => {
    const stoppedRack = {
      ...rack,
      nodes: rack.nodes.map(node => node.id === 10 ? { ...node, kv_server: undefined } : node),
    };
    const { getByText } = renderSidebar(Domain.Cluster, {
      racks: [stoppedRack],
      servers: [],
      allServers: [{ id: 'kv-10', node_id: 10, service_type: 'paxos-kv', health: 'down' }],
    });
    expect(getByText('PKV-10', { exact: true })).toBeTruthy();
  });

  it('keeps physical storage separate from the owning service and loads on expansion', () => {
    const onLoadNodeDisks = vi.fn().mockResolvedValue(undefined);
    const { getByTestId, getByText, queryByText } = renderSidebar(Domain.Cluster, { onLoadNodeDisks });
    const diskdb = getByTestId('tree-node-DDB-10');
    const storage = getByTestId('tree-node-STORAGE-10');
    expect(queryByText(/Physical Group.*DG-100/)).toBeNull();
    expect(onLoadNodeDisks).not.toHaveBeenCalled();
    fireEvent.click(within(storage).getByRole('button', { name: 'Expand' }));
    expect(onLoadNodeDisks).toHaveBeenCalledWith(10);
    fireEvent.click(within(getByTestId('tree-node-CH-DG-10-100')).getByRole('button', { name: 'Expand' }));
    expect(storage.contains(getByText(/Physical Group.*DG-100/))).toBe(true);
    expect(storage.contains(getByText('0123456789ab…'))).toBe(true);
    expect(diskdb.contains(getByText('0123456789ab…'))).toBe(false);
  });

  it('exposes candidate scope with honest unknown and stale observed counts', () => {
    const onNodeClick = vi.fn();
    const { getByTestId, getByText } = renderSidebar(Domain.Cluster, {
      onNodeClick, candidateSummaries: { 10: { count: 2, observed_at_ms: 1234, stale: true } },
    });
    fireEvent.click(within(getByTestId('tree-node-STORAGE-10')).getByRole('button', { name: 'Expand' }));
    fireEvent.click(getByText('Candidates (2)'));
    expect(onNodeClick.mock.calls[0][0]).toMatchObject({ type: 'Node', section: 'candidates', rawId: 10 });
    fireEvent.click(within(getByTestId('tree-node-STORAGE-11')).getByRole('button', { name: 'Expand' }));
    expect(getByText('Candidates (?)')).toBeTruthy();
  });

  it('shows unassigned physical disk groups under Storage', () => {
    const { getByText, getByTestId } = renderSidebar(Domain.Cluster, {
      diskdbInstances: [], diskdbNodeIds: new Set(),
    });
    fireEvent.click(within(getByTestId('tree-node-STORAGE-10')).getByRole('button', { name: 'Expand' }));
    expect(getByText(/Physical Group.*DG-100/)).toBeTruthy();
  });
});

describe('Sidebar · KV logical projection', () => {
  it('renders datacenter → store → group → replica without PKV-server parents', () => {
    const { getByText, queryByText } = renderSidebar(Domain.KV);
    // Logical tree: datacenter → store → group → replica.
    expect(getByText('S-7', { exact: true })).toBeTruthy();
    expect(getByText('G-70', { exact: true })).toBeTruthy();
    expect(getByText('LR-700', { exact: true })).toBeTruthy();
    expect(getByText('LR-701', { exact: true })).toBeTruthy();
    // No physical PKV-server or node items in the KV tree.
    expect(queryByText('PKV-10')).toBeNull();
    expect(queryByText('N-10')).toBeNull();
  });

  it('offers documentation beside the title and an Add Store action', () => {
    const { getByRole } = renderSidebar(Domain.KV, { onAdd: vi.fn() });
    expect(getByRole('link', { name: 'Help' })).toHaveAttribute('href', 'https://crowdb.dev/docs/manual/console/ui/tab/kv/');
    expect(getByRole('button', { name: 'Add Store' })).toBeTruthy();
  });
});

describe('Sidebar · shared Capacity hierarchy', () => {
  it('retains services and physical storage for legacy Capacity entry', () => {
    const { getByText, getByTestId } = renderSidebar(Domain.Capacity);
    fireEvent.click(within(getByTestId('tree-node-STORAGE-10')).getByRole('button', { name: 'Expand' }));
    fireEvent.click(within(getByTestId('tree-node-CH-DG-10-100')).getByRole('button', { name: 'Expand' }));
    expect(getByText('N-10', { exact: true })).toBeTruthy();
    expect(getByText(/Physical Group.*DG-100/)).toBeTruthy();
    expect(getByText('0123456789ab…')).toBeTruthy();
    expect(getByText('DDB-10')).toBeTruthy();
    expect(getByText('PKV-10')).toBeTruthy();
  });
});
