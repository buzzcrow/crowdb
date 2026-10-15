// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { fireEvent, render, screen, within } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { Domain, NodeHealth, type Node } from '../types';
import { DomainProvider } from '../contexts/DomainContext';
import { SelectionProvider, useSelection } from '../contexts/SelectionContext';
import { ClusterOverview, type ClusterOverviewProps } from './ClusterOverview';

const nodes: Node[] = [1, 2].map(id => ({ id, rack_id: id, host: `host-${id}`, ssh: { type: 'KeyDefault', user: 'test' } }));
const props: ClusterOverviewProps = {
  racks: nodes.map(node => ({ id: node.rack_id, nodes: [node] })), nodes, services: [],
  nodeHealthById: { 1: NodeHealth.Up, 2: NodeHealth.Down },
  hardware: { datacenter_capacity_bytes: 1024, racks: [], nodes: [], disk_groups: [
    { disk_group_id: 7, node_id: 1, rack_id: 1, status: 1, disk_count: 0, capacity_bytes: 1024, disks: [] },
  ] }, usage: null, candidates: { 2: { count: 2, observed_at_ms: 10, stale: true, partial: true } },
  readonly: false, onAddRack: vi.fn(), onAddNode: vi.fn(),
};
function Workspace({ capacity = false }: { capacity?: boolean }) {
  const { scopeEntity, selectedEntity, selectEntity } = useSelection();
  return <><output aria-label="scope">{scopeEntity?.id ?? 'root'}</output><output aria-label="selection">{selectedEntity?.id ?? 'none'}</output>
    <button onClick={() => selectEntity({ domain: Domain.Cluster, type: 'Rack', id: '2' })}>Scope Rack 2</button>
    <ClusterOverview {...props} capacity={capacity} /></>;
}
function mount(capacity = false) {
  return render(<DomainProvider><SelectionProvider><Workspace capacity={capacity} /></SelectionProvider></DomainProvider>);
}
it('inspects a card without changing scope, opens explicitly and scopes Add Node to its rack', () => {
  mount();
  const card = screen.getByRole('article', { name: 'Node 2 summary' });
  fireEvent.click(card);
  expect(screen.getByLabelText('scope')).toHaveTextContent('root');
  expect(screen.getByLabelText('selection')).toHaveTextContent('2');
  expect(card).toHaveTextContent('2+ · Stale');
  const rack = screen.getByRole('region', { name: 'Rack 2 nodes' });
  fireEvent.click(within(rack).getByRole('button', { name: 'Add Node' }));
  expect(props.onAddNode).toHaveBeenCalledWith(2);
  fireEvent.click(within(card).getByRole('button', { name: 'N-2' }));
  expect(screen.getByLabelText('scope')).toHaveTextContent('2');
});
it('filters scoped table rows and sorts registered capacity without inventing usage', () => {
  mount(true);
  const table = screen.getByRole('table', { name: 'Node capacity' });
  const nodeOrder = () => within(table).getAllByRole('row').slice(1).map(row => within(row).getByRole('button', { name: /^N-/ }).textContent);
  // Node 2 has no registered groups, so its capacity is known zero; both usages remain unknown.
  fireEvent.click(within(table).getByRole('button', { name: 'Capacity' }));
  expect(nodeOrder()).toEqual(['N-2', 'N-1']);
  fireEvent.click(within(table).getByRole('button', { name: 'Capacity' }));
  expect(nodeOrder()).toEqual(['N-1', 'N-2']);
  expect(within(table).getAllByText('Unknown')).toHaveLength(4);
  fireEvent.change(screen.getByLabelText('Node health'), { target: { value: 'down' } });
  expect(nodeOrder()).toEqual(['N-2']);
  fireEvent.change(screen.getByLabelText('Node health'), { target: { value: 'all' } });
  fireEvent.change(screen.getByLabelText('Find node'), { target: { value: 'host-1' } });
  expect(nodeOrder()).toEqual(['N-1']);
  fireEvent.click(screen.getByRole('button', { name: 'Scope Rack 2' }));
  expect(within(table).queryByRole('button', { name: /^N-/ })).toBeNull();
});
