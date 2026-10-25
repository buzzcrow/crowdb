// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { listNodes, listRacks } from '../api';
import type { Rack } from '../types';
import { useClusterTree } from './useClusterTree';

vi.mock('../api', () => ({ listRacks: vi.fn(), listNodes: vi.fn(),
  listNodeDiskGroups: vi.fn(), listDisksInGroup: vi.fn() }));
afterEach(() => { cleanup(); vi.useRealTimers(); vi.resetAllMocks(); });

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
