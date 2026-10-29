// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { listNodes, listRacks, listNodeDiskGroups, listDisksInGroup } from '../api';
import type { Rack } from '../types';
import { useClusterTree } from './useClusterTree';

vi.mock('../api', () => ({ listRacks: vi.fn(), listNodes: vi.fn(),
  listNodeDiskGroups: vi.fn(), listDisksInGroup: vi.fn() }));
afterEach(() => { cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); vi.resetAllMocks(); });

it('refreshes newly created disks in an opened node without reopening or reloading', async () => {
  vi.useFakeTimers();
  vi.mocked(listRacks).mockResolvedValue([]);
  vi.mocked(listNodes).mockResolvedValue([]);
  vi.mocked(listNodeDiskGroups).mockResolvedValue([{ id: 7, node_id: 1, rack_id: 1 }]);
  vi.mocked(listDisksInGroup).mockResolvedValue([]);
  const { result } = renderHook(() => useClusterTree({ pollIntervalActive: 1000 }));
  await act(async () => { await result.current.loadNodeDisks(1); await result.current.loadGroupDisks(1, 7); });
  vi.mocked(listDisksInGroup).mockResolvedValue([{ disk_id: 'disk-1', node_id: 1, rack_id: 1, disk_group_id: 7, disk_type: 'ssd', capacity_bytes: 1024, zone_size_bytes: 512, unit_size_bytes: 128 }]);
  await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
  expect(result.current.nodeDiskGroups[1].disksByDg[7]).toEqual([expect.objectContaining({ disk_id: 'disk-1' })]);
  expect(listNodeDiskGroups).toHaveBeenLastCalledWith(1, expect.anything());
});

it('observes current nodes immediately on returning to the browser', async () => {
  vi.useFakeTimers();
  const visibility = vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
  vi.mocked(listRacks).mockResolvedValue([]);
  vi.mocked(listNodes).mockResolvedValue([]);
  const { result } = renderHook(() => useClusterTree({ pollIntervalActive: 1000 }));
  await act(async () => {});
  visibility.mockReturnValue('hidden');
  act(() => document.dispatchEvent(new Event('visibilitychange')));
  await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
  vi.mocked(listNodes).mockResolvedValue([{ id: 1, rack_id: 1, host: 'node1', ssh: { type: 'KeyDefault', user: '' } }]);
  visibility.mockReturnValue('visible');
  await act(async () => { document.dispatchEvent(new Event('visibilitychange')); });
  expect(result.current.nodes).toEqual([expect.objectContaining({ id: 1 })]);
});

it('lets a slow post-mutation refresh finish without polling cancelling its response', async () => {
  vi.useFakeTimers();
  vi.mocked(listRacks).mockResolvedValue([]);
  vi.mocked(listNodes).mockResolvedValue([]);
  const { result } = renderHook(() => useClusterTree({ pollIntervalActive: 1000 }));
  await act(async () => {});
  let finish!: (racks: Rack[]) => void;
  vi.mocked(listRacks).mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
  let refreshed!: Promise<void>;
  act(() => { refreshed = result.current.refresh(); });
  const signal = vi.mocked(listRacks).mock.calls[1][1]!.signal!;
  await act(async () => { await vi.advanceTimersByTimeAsync(2100); });
  expect(signal.aborted).toBe(false);
  expect(listRacks).toHaveBeenCalledTimes(2);
  const racks: Rack[] = [{ id: 61, name: 'Rack Sixty-One', nodes: [] }];
  await act(async () => { finish(racks); await refreshed; });
  expect(result.current.racks).toEqual(racks);
  expect(result.current.loading).toBe(false);
});

it('starts a fresh read after a mutation and fences an older observation', async () => {
  vi.useFakeTimers();
  let finish!: (racks: Rack[]) => void;
  vi.mocked(listRacks).mockReturnValueOnce(new Promise(resolve => { finish = resolve; }))
    .mockResolvedValue([{ id: 61, name: 'new', nodes: [] }]);
  vi.mocked(listNodes).mockResolvedValue([]);
  const { result } = renderHook(() => useClusterTree());
  const previous = vi.mocked(listRacks).mock.calls[0][1]!.signal!;
  await act(async () => { await result.current.refresh(); });
  expect(previous.aborted).toBe(true);
  await act(async () => { finish([]); });
  expect(result.current.racks).toEqual([{ id: 61, name: 'new', nodes: [] }]);
});
