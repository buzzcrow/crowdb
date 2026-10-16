// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, renderHook, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { listNodeDiskFiles, nodeDiskFile, nodeDiskInventory } from '../api';
import { useNodeStorage } from './useNodeStorage';
import type { DiskInventory } from './types';

vi.mock('../api', () => ({ listNodeDiskFiles: vi.fn(), nodeDiskFile: vi.fn(), nodeDiskInventory: vi.fn() }));
const inventory = vi.mocked(nodeDiskInventory);
const files = vi.mocked(listNodeDiskFiles);
const observe = vi.mocked(nodeDiskFile);
const sample: DiskInventory = { observed_at_ms: 1234, filesystems: [], disks: [{
  path: '/opt/crowdb/data/disks/one.img', backend: 'file', capacity_bytes: 4096, allocated_bytes: 4096,
  backing_identity: 'file:1:2', supplied: true, error: null, backing_filesystem: null,
}] };
beforeEach(() => {
  inventory.mockReset(); files.mockReset(); observe.mockReset();
  inventory.mockResolvedValue(sample); files.mockResolvedValue({ items: [], next: null });
});

it('retains each successful observation across a failed refresh and exposes recovery', async () => {
  const { result } = renderHook(() => useNodeStorage(1, true));
  await waitFor(() => expect(result.current.inventory).toEqual(sample));
  inventory.mockRejectedValueOnce(new Error('node unavailable'));
  await act(async () => { await result.current.refresh(); });
  expect(result.current.inventory).toEqual(sample);
  expect(result.current.errors.inventory).toContain('node unavailable');
  await act(async () => { await result.current.refresh(); });
  expect(result.current.errors.inventory).toBe('');
  expect(result.current.inventory).toEqual(sample);
});

it('does not inherit a previous node observation or a late response', async () => {
  let complete!: (value: DiskInventory) => void;
  inventory.mockImplementationOnce(() => new Promise(resolve => { complete = resolve; }));
  const second = { ...sample, observed_at_ms: 2345, disks: [] };
  inventory.mockResolvedValueOnce(second);
  const { result, rerender } = renderHook(({ node }) => useNodeStorage(node, true), { initialProps: { node: 1 } });
  rerender({ node: 2 });
  expect(result.current.nodeId).toBe(2);
  expect(result.current.inventory).toBeUndefined();
  await waitFor(() => expect(result.current.inventory).toEqual(second));
  await act(async () => { complete(sample); });
  expect(result.current.nodeId).toBe(2);
  expect(result.current.inventory).toEqual(second);
});

it('coalesces explicit refreshes instead of issuing duplicate inventory reads', async () => {
  const { result } = renderHook(() => useNodeStorage(1, true));
  await waitFor(() => expect(result.current.loading).toBe(false));
  const count = inventory.mock.calls.length;
  let first!: Promise<void>; let second!: Promise<void>;
  await act(async () => {
    first = result.current.refresh(); second = result.current.refresh();
    await first;
  });
  expect(first).toBe(second);
  expect(inventory.mock.calls.length).toBe(count + 1);
});
