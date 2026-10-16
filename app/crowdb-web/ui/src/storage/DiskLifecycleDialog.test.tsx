// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { diskLifecycle, offlineNodeDisk, removeNodeDisk, rejoinNodeDisk, onlineNodeDisk } from '../api';
import { DiskLifecycleDialog } from './DiskLifecycleDialog';
import type { DiskLifecycle } from './types';
vi.mock('../api', () => ({ diskLifecycle: vi.fn(), offlineNodeDisk: vi.fn(), removeNodeDisk: vi.fn(), rejoinNodeDisk: vi.fn(), onlineNodeDisk: vi.fn() }));
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
  read.mockReset(); offline.mockReset(); remove.mockReset(); vi.mocked(rejoinNodeDisk).mockReset(); vi.mocked(onlineNodeDisk).mockReset(); read.mockResolvedValue(initial);
});

it('keeps an exact-revision Online action clickable during a refresh and ignores its old response', async () => {
  read.mockResolvedValue({ ...excluded, remove_conditions: [] });
  const operation = { operation_id: `online/disk/${excluded.disk_revision}`, offline_revision: excluded.disk_revision!, state: 'pending' as const, error: null };
  vi.mocked(onlineNodeDisk).mockResolvedValue(operation);
  const { onClose } = setup();
  await screen.findByText(/Owner and DiskIO exclusion confirmed/);
  fireEvent.click(screen.getByRole('button', { name: 'Bring Online…' }));
  let finish!: (value: DiskLifecycle) => void;
  read.mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
  fireEvent.click(screen.getByRole('button', { name: 'Refresh authority' }));
  expect(screen.getByRole('button', { name: 'Refresh authority' })).toBeDisabled();
  expect(screen.getByRole('button', { name: 'Validate and bring Online' })).toBeEnabled();
  fireEvent.click(screen.getByRole('button', { name: 'Validate and bring Online' }));
  await screen.findByText(/Validation accepted/);
  await act(async () => { finish({ ...excluded, remove_conditions: [] }); });
  expect(screen.getByRole('button', { name: 'Validation in progress' })).toBeDisabled();
  expect(onlineNodeDisk).toHaveBeenCalledTimes(1);
  expect(onlineNodeDisk).toHaveBeenCalledWith(1, 7, 'disk', excluded.disk_revision);
  expect(onClose).not.toHaveBeenCalled();
});

it('shows accepted validation until Group 0 confirms Online and preserves exact revision strings', async () => {
  read.mockResolvedValue({ ...excluded, remove_conditions: [] });
  const operation = { operation_id: `online/disk/${excluded.disk_revision}`, offline_revision: excluded.disk_revision!, state: 'pending' as const, error: null };
  vi.mocked(onlineNodeDisk).mockResolvedValue(operation);
  const { onClose, onCommitted } = setup();
  await screen.findByText(/Owner and DiskIO exclusion confirmed/);
  fireEvent.click(screen.getByRole('button', { name: 'Bring Online…' }));
  fireEvent.click(screen.getByRole('button', { name: 'Validate and bring Online' }));
  await screen.findByText(/Validation accepted/);
  expect(onlineNodeDisk).toHaveBeenCalledWith(1, 7, 'disk', excluded.disk_revision);
  expect(onClose).not.toHaveBeenCalled(); expect(onCommitted).not.toHaveBeenCalled();
  expect(screen.getByRole('button', { name: 'Validation in progress' })).toBeDisabled();
  read.mockResolvedValue({ ...initial, disk_revision: '18446744073709551002', online: { ...operation, state: 'complete' } });
  fireEvent.click(screen.getByRole('button', { name: 'Refresh authority' }));
  await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1));
  expect(onCommitted.mock.calls[0][1]).toBe('online');
});

it('reconciles lost Online acceptance without issuing a second request', async () => {
  read.mockResolvedValue({ ...excluded, remove_conditions: [] });
  vi.mocked(onlineNodeDisk).mockRejectedValue(new Error('response lost'));
  const { onClose } = setup();
  await screen.findByText(/Owner and DiskIO exclusion confirmed/);
  fireEvent.click(screen.getByRole('button', { name: 'Bring Online…' }));
  fireEvent.click(screen.getByRole('button', { name: 'Validate and bring Online' }));
  await screen.findByRole('alert');
  expect(screen.getByRole('button', { name: 'Retry same operation' })).toBeDisabled();
  read.mockResolvedValue({ ...excluded, online: { operation_id: 'online/disk/revision', offline_revision: excluded.disk_revision!, state: 'pending', error: null } });
  fireEvent.click(screen.getByRole('button', { name: 'Refresh authority' }));
  await screen.findByText(/Validation accepted/);
  expect(onlineNodeDisk).toHaveBeenCalledTimes(1); expect(onClose).not.toHaveBeenCalled();
});

it('reopens a failed validation as Offline with its reason and an explicit retry', async () => {
  read.mockResolvedValue({ ...excluded, online: { operation_id: 'online/disk/revision', offline_revision: excluded.disk_revision!, state: 'failed', error: 'existing snapshot checksum mismatch' } });
  const { onClose } = setup();
  await screen.findByText(/Online validation failed: existing snapshot checksum mismatch/);
  expect(screen.getByRole('button', { name: 'Validate and bring Online' })).toBeEnabled();
  expect(onClose).not.toHaveBeenCalled(); expect(onlineNodeDisk).not.toHaveBeenCalled();
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

it('rejoins only the inspected removed operation and preserves Offline membership', async () => {
  const removed = { ...excluded, removed: true, disk_revision: null, operation_id: 'remove/disk/99', removed_from_revision: '99' };
  read.mockResolvedValue(removed);
  const restored = { ...excluded, rejoin_operation_id: 'rejoin/disk/99', rejoin_source_removal_operation_id: removed.operation_id };
  vi.mocked(rejoinNodeDisk).mockResolvedValue({ operation_id: 'rejoin/disk/99', source_removal_operation_id: removed.operation_id, state: restored });
  const { onClose, onCommitted } = setup();
  await waitFor(() => expect(screen.getByRole('button', { name: 'Rejoin Offline' })).toBeEnabled());
  fireEvent.click(screen.getByRole('button', { name: 'Rejoin Offline' }));
  await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1));
  expect(rejoinNodeDisk).toHaveBeenCalledWith(1, 7, 'disk', removed.operation_id);
  expect(onCommitted.mock.calls[0]).toEqual([restored, 'rejoin']);
  expect(offline).not.toHaveBeenCalled(); expect(remove).not.toHaveBeenCalled();
});

it('reconciles a lost Rejoin response against the original removal operation', async () => {
  const removed = { ...excluded, removed: true, disk_revision: null, operation_id: 'remove/disk/99', removed_from_revision: '99' };
  read.mockResolvedValue(removed); vi.mocked(rejoinNodeDisk).mockRejectedValue(new Error('response lost'));
  const { onClose } = setup();
  await waitFor(() => expect(screen.getByRole('button', { name: 'Rejoin Offline' })).toBeEnabled());
  fireEvent.click(screen.getByRole('button', { name: 'Rejoin Offline' }));
  await screen.findByRole('alert');
  expect(screen.getByRole('button', { name: 'Retry same operation' })).toBeDisabled();
  read.mockResolvedValue({ ...excluded, rejoin_source_removal_operation_id: removed.operation_id });
  fireEvent.click(screen.getByRole('button', { name: 'Refresh authority' }));
  await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1));
  expect(rejoinNodeDisk).toHaveBeenCalledTimes(1);
});
