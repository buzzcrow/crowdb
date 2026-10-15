// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { diskLifecycle, offlineNodeDisk, removeNodeDisk } from '../api';
import { DiskLifecycleDialog } from './DiskLifecycleDialog';
import type { DiskLifecycle } from './types';
vi.mock('../api', () => ({ diskLifecycle: vi.fn(), offlineNodeDisk: vi.fn(), removeNodeDisk: vi.fn() }));
const read = vi.mocked(diskLifecycle); const offline = vi.mocked(offlineNodeDisk); const remove = vi.mocked(removeNodeDisk);
const initial: DiskLifecycle = { version: 1, disk_id: 'disk', offline: false, removed: false,
  disk_revision: '18446744073709551000', remove_conditions: ['Mark the disk Offline before removal'], operation_id: null, removed_from_revision: null };
const excluded: DiskLifecycle = { ...initial, offline: true, disk_revision: '18446744073709551001', remove_conditions: ['DiskIO 7 pending'], operation_id: 'offline/disk/18446744073709551001' };
function setup() {
  const onClose = vi.fn(); const onCommitted = vi.fn();
  render(<DiskLifecycleDialog nodeId={1} groupId={7} diskId="disk" onClose={onClose} onCommitted={onCommitted} />);
  return { onClose, onCommitted };
}
beforeEach(() => {
  read.mockReset(); offline.mockReset(); remove.mockReset(); read.mockResolvedValue(initial);
});

it('requires exact-revision exclusion and explicit data-unavailability acknowledgement', async () => {
  offline.mockResolvedValue(excluded);
  const { onCommitted, onClose } = setup();
  fireEvent.click(await screen.findByRole('button', { name: 'Mark Offline' }));
  await waitFor(() => expect(offline).toHaveBeenCalledWith(1, 7, 'disk', initial.disk_revision));
  await screen.findByText('DiskIO 7 pending');
  fireEvent.click(screen.getByRole('checkbox'));
  expect(screen.getByRole('button', { name: 'Remove disk' })).toBeDisabled();
  read.mockResolvedValue({ ...excluded, remove_conditions: [] });
  fireEvent.click(screen.getByRole('button', { name: 'Refresh authority' }));
  await waitFor(() => expect(screen.getByRole('button', { name: 'Remove disk' })).toBeEnabled());
  remove.mockResolvedValue({ operation_id: 'remove/disk/revision', disk_id: 'disk', removed: true, offline_revision: excluded.disk_revision! });
  fireEvent.click(screen.getByRole('button', { name: 'Remove disk' }));
  await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1));
  expect(remove).toHaveBeenCalledWith(1, 7, 'disk', excluded.disk_revision);
  expect(onCommitted).toHaveBeenCalledTimes(2);
  expect(onCommitted.mock.calls[1][0]).toMatchObject({ removed: true, removed_from_revision: excluded.disk_revision });
});

it('reconciles an unknown Offline response before enabling another mutation', async () => {
  offline.mockRejectedValue(new Error('connection lost'));
  const { onCommitted } = setup();
  fireEvent.click(await screen.findByRole('button', { name: 'Mark Offline' }));
  await screen.findByRole('alert');
  expect(screen.getByRole('button', { name: 'Retry same operation' })).toBeDisabled();
  read.mockResolvedValue(excluded);
  fireEvent.click(screen.getByRole('button', { name: 'Refresh authority' }));
  await screen.findByText('DiskIO 7 pending');
  expect(screen.getByRole('button', { name: 'Remove disk' })).toBeDisabled();
  expect(offline).toHaveBeenCalledTimes(1);
  expect(onCommitted).toHaveBeenCalledTimes(1);
});

it('reconciles a committed removal receipt after connection loss without repeating Remove', async () => {
  read.mockResolvedValue({ ...excluded, remove_conditions: [] });
  remove.mockRejectedValue(new Error('response lost'));
  const { onClose, onCommitted } = setup();
  await screen.findByText(/Owner and DiskIO exclusion confirmed/);
  fireEvent.click(screen.getByRole('checkbox'));
  fireEvent.click(screen.getByRole('button', { name: 'Remove disk' }));
  await screen.findByRole('alert');
  expect(screen.getByRole('button', { name: 'Retry same operation' })).toBeDisabled();
  read.mockResolvedValue({ ...excluded, removed: true, disk_revision: null, removed_from_revision: excluded.disk_revision });
  fireEvent.click(screen.getByRole('button', { name: 'Refresh authority' }));
  await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1));
  expect(remove).toHaveBeenCalledTimes(1);
  expect(onCommitted.mock.calls[0][0].removed).toBe(true);
});
