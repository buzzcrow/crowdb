// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { render, screen } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { RemovedDiskRow } from './RemovedDiskRow';
import type { DiskInventory, RemovedDisk } from './types';
const disk: RemovedDisk = { disk_id: 'one', disk_group_id: 7, operation_id: 'source', container_path: '/data/disks/one',
  backend: 'file', backing_identity: 'file:1:2', media_type: null, sparse: false, capacity_units: '64', unit_size_bytes: 4096 };
const inventory: DiskInventory = { observed_at_ms: 1, filesystems: [], disks: [{ path: disk.container_path, backend: 'file',
  backing_identity: disk.backing_identity, capacity_bytes: 64 * 4096, allocated_bytes: 64 * 4096,
  supplied: false, error: null, backing_filesystem: null }] };
it('requires the original observed type identity and capacity before Rejoin', () => {
  const props = { disk, ready: true, readonly: false, onInspect: vi.fn(), onRejoin: vi.fn() };
  const { rerender } = render(<RemovedDiskRow {...props} />);
  expect(screen.getByRole('button', { name: 'Rejoin disk' })).toBeDisabled();
  for (const replacement of [{ ...inventory.disks[0], backing_identity: 'file:other' },
    { ...inventory.disks[0], capacity_bytes: 4096 }, { ...inventory.disks[0], backend: 'block-device' as const }]) {
    rerender(<RemovedDiskRow {...props} inventory={{ ...inventory, disks: [replacement] }} />);
    expect(screen.getByRole('button', { name: 'Rejoin disk' })).toBeDisabled();
  }
  rerender(<RemovedDiskRow {...props} inventory={inventory} />);
  expect(screen.getByRole('button', { name: 'Rejoin disk' })).toBeEnabled();
  rerender(<RemovedDiskRow {...props} inventory={inventory} readonly />);
  expect(screen.getByRole('button', { name: 'Rejoin disk' })).toBeDisabled();
});
