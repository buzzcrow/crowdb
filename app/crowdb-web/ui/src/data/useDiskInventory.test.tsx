// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import * as api from '../api';
import { useDiskInventory } from './useDiskInventory';
vi.mock('../api', () => ({ listNodeDiskGroups: vi.fn(), listDisksInGroup: vi.fn() }));
afterEach(() => { vi.resetAllMocks(); });

it('loads only requested branches and deduplicates requests with four workers', async () => {
  const completions: Array<() => void> = [];
  let active = 0; let maximum = 0;
  vi.mocked(api.listNodeDiskGroups).mockImplementation(() => {
    active++; maximum = Math.max(maximum, active);
    return new Promise(resolve => completions.push(() => { active--; resolve([]); }));
  });
  vi.mocked(api.listDisksInGroup).mockResolvedValue([]);
  const { result, unmount } = renderHook(() => useDiskInventory(true));
  expect(api.listNodeDiskGroups).not.toHaveBeenCalled();
  expect(api.listDisksInGroup).not.toHaveBeenCalled();
  let requests!: Promise<void>[];
  act(() => { requests = Array.from({ length: 12 }, (_, id) => result.current.loadNode(id)); });
  expect(api.listNodeDiskGroups).toHaveBeenCalledTimes(4);
  expect(result.current.loadNode(0)).toBe(requests[0]);
  for (let index = 0; index < 12; index++) await act(async () => completions[index]());
  await act(async () => { await Promise.all(requests); });
  expect(maximum).toBe(4);
  expect(api.listDisksInGroup).not.toHaveBeenCalled();
  await act(async () => { await result.current.loadGroup(0, 7); });
  expect(api.listDisksInGroup).toHaveBeenCalledTimes(1);
  expect(result.current.inventory[0].disksByDg[7]).toEqual([]);
  unmount();
});

it('does not replace known inventory with empty success on a failed refresh', async () => {
  vi.mocked(api.listNodeDiskGroups).mockResolvedValueOnce([{ id: 7, node_id: 1, rack_id: 1 }]).mockRejectedValueOnce(new Error('group0 offline'));
  const { result, unmount } = renderHook(() => useDiskInventory(true));
  await act(async () => { await result.current.loadNode(1); });
  await act(async () => { await result.current.refresh(); });
  expect(result.current.inventory[1].diskGroups).toHaveLength(1);
  expect(result.current.error?.message).toContain('group0 offline');
  unmount();
});

it('aborts unfinished branch requests on exit and ignores late data', async () => {
  let finish!: (value: []) => void;
  vi.mocked(api.listNodeDiskGroups).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  const { result, rerender, unmount } = renderHook(({ enabled }) => useDiskInventory(enabled), { initialProps: { enabled: true } });
  act(() => { void result.current.loadNode(1); });
  const signal = vi.mocked(api.listNodeDiskGroups).mock.calls[0][1]!.signal!;
  rerender({ enabled: false });
  expect(signal.aborted).toBe(true);
  await act(async () => finish([]));
  await waitFor(() => expect(result.current.inventory).toEqual({}));
  unmount();
});
