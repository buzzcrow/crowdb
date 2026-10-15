// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { diskBacking, nodeDiskFile } from '../api';
import { DiskBackingProperties } from './DiskBackingProperties';
import type { DiskBackingInfo } from './types';

vi.mock('../api', () => ({ diskBacking: vi.fn(), nodeDiskFile: vi.fn() }));
const backing = vi.mocked(diskBacking);
const sample: DiskBackingInfo = {
  version: 1, operation_id: 'one', backend: 'file', backing_identity: 'file:1:9007199254740993',
  media_type: null, sparse: false, removed: false, container_path: '/opt/crowdb/data/disks/managed-one.img',
  capacity_units: '9007199254740993', unit_size_bytes: 4096, zone_size_units: '1024', zone_count: 4,
  owner: null, binding: '1/2', observed_at_ms: 1234,
};
beforeEach(() => {
  backing.mockReset(); vi.mocked(nodeDiskFile).mockReset();
  backing.mockResolvedValue(sample);
  vi.mocked(nodeDiskFile).mockResolvedValue({ version: 1, request: { operation_id: 'one', node_id: 2, cluster_id: 'c', capacity_bytes: 4096, sparse: false, reserve_bytes: 0 },
    phase: 'registered', path: sample.container_path, backing_identity: sample.backing_identity, preparation_complete: true, error: null });
});

it('keeps exact backing identity and geometry distinct from operation and health', async () => {
  render(<DiskBackingProperties nodeId={2} groupId={7} diskId="disk" />);
  await screen.findByText(sample.backing_identity);
  expect(screen.getByText('9007199254740993')).toBeVisible();
  expect(screen.getByText('Unknown', { exact: true })).toBeVisible();
  expect(screen.getByText('Unassigned')).toBeVisible();
  await screen.findByText('Operation: Registered · DiskIO verified');
  expect(screen.getByText(/does not assert current I\/O health/)).toBeVisible();
});

it('retains last-known properties on failure and allows explicit recovery', async () => {
  render(<DiskBackingProperties nodeId={2} groupId={7} diskId="disk" />);
  await screen.findByText(sample.backing_identity);
  backing.mockRejectedValueOnce(new Error('authority unavailable'));
  fireEvent.click(screen.getByRole('button', { name: 'Refresh backing properties' }));
  await screen.findByRole('alert');
  expect(screen.getByText(sample.backing_identity)).toBeVisible();
  fireEvent.click(screen.getByRole('button', { name: 'Refresh backing properties' }));
  await waitFor(() => expect(screen.queryByRole('alert')).toBeNull());
});

it('shows unknown legacy backing without inventing a file or allocation policy', async () => {
  backing.mockResolvedValue(null);
  render(<DiskBackingProperties nodeId={2} groupId={7} diskId="disk" />);
  await screen.findByText(/unknown for this legacy disk/);
  expect(nodeDiskFile).not.toHaveBeenCalled();
});

it('refreshes the same selected disk when it is inspected after removal', async () => {
  const { rerender } = render(<DiskBackingProperties nodeId={2} groupId={7} diskId="disk" observationKey={{}} />);
  await screen.findByText('Registered', { exact: true });
  backing.mockResolvedValue({ ...sample, removed: true });
  rerender(<DiskBackingProperties nodeId={2} groupId={7} diskId="disk" observationKey={{}} />);
  await screen.findByText('Removed', { exact: true });
  expect(backing).toHaveBeenCalledTimes(2);
});

it('shows physical resource and retained tail separately from usable geometry', async () => {
  backing.mockResolvedValue({ ...sample, version: 2, container_path: '/opt/crowdb/data/disks/supplied',
    physical_backing: { host_id: 'owned-host', resource_identity: 'file:1:2', capacity_bytes: '68157952', usable_bytes: '67108864', tail_bytes: '1049088' } });
  render(<DiskBackingProperties nodeId={2} groupId={7} diskId="disk" />);
  await screen.findByText('owned-host');
  expect(screen.getByText('file:1:2')).toBeVisible();
  for (const value of ['68157952', '67108864', '1049088']) expect(screen.getByText(value)).toBeVisible();
  expect(nodeDiskFile).not.toHaveBeenCalled();
});
