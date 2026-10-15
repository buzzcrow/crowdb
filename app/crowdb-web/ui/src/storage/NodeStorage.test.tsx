// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { diskLifecycle, offlineNodeDisk, removeNodeDisk } from '../api';
import { NodeStorage } from './NodeStorage';
vi.mock('../api', () => ({ diskLifecycle: vi.fn(), offlineNodeDisk: vi.fn(), removeNodeDisk: vi.fn(), rejoinNodeDisk: vi.fn(), createNodeDiskFile: vi.fn() }));
vi.mock('../contexts/SelectionContext', () => ({ useSelection: () => ({ inspectEntity: vi.fn(), selectEntity: vi.fn(), selectedEntity: null }) }));
vi.mock('./useNodeStorage', () => ({ useNodeStorage: () => ({ operations: {}, errors: {}, loading: false, next: null, refresh: vi.fn(), accept: vi.fn() }) }));
vi.mock('./useRemovedDisks', () => ({ useRemovedDisks: () => ({ items: [], next: null, observed: true, refresh: vi.fn() }) }));

it('moves focus after dialog cleanup even while its original row remains mounted', async () => {
  const original = { version: 1, disk_id: 'one', offline: false, removed: false, disk_revision: '100',
    remove_conditions: ['Mark Offline'], operation_id: null, removed_from_revision: null };
  vi.mocked(diskLifecycle).mockResolvedValue(original);
  vi.mocked(offlineNodeDisk).mockResolvedValue({ ...original, offline: true, disk_revision: '101', remove_conditions: [] });
  vi.mocked(removeNodeDisk).mockResolvedValue({ disk_id: 'one', removed: true, operation_id: 'remove/one/101', offline_revision: '101' });
  render(<NodeStorage nodeId={1} rackId={1} active readonly={false} ready hardware={null} usage={null}
    inventory={{ diskGroups: [{ id: 7, node_id: 1, rack_id: 1 }], disksByDg: { 7: [{ disk_id: 'one', disk_group_id: 7,
      node_id: 1, rack_id: 1, disk_type: 'Hdd', capacity_bytes: 65536, zone_size_bytes: 65536, unit_size_bytes: 4096 }] } }}
    loadGroup={vi.fn().mockResolvedValue(undefined)} onAddGroup={vi.fn()} onCandidates={vi.fn()} />);
  const trigger = screen.getByRole('button', { name: 'Manage disk' });
  trigger.focus(); fireEvent.click(trigger);
  await waitFor(() => expect(screen.getByRole('button', { name: 'Mark Offline' })).toBeEnabled());
  fireEvent.click(screen.getByRole('button', { name: 'Mark Offline' }));
  await screen.findByText(/Owner and DiskIO exclusion confirmed/);
  fireEvent.click(screen.getByRole('checkbox'));
  fireEvent.click(screen.getByRole('button', { name: 'Remove disk' }));
  await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
  expect(trigger).toBeInTheDocument();
  expect(screen.getByRole('region', { name: 'Candidate disks' })).toHaveFocus();
});
