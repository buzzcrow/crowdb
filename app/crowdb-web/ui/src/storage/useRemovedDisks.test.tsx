// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, renderHook, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { removedNodeDisks } from '../api';
import { useRemovedDisks } from './useRemovedDisks';
import type { RemovedDisk } from './types';
vi.mock('../api', () => ({ removedNodeDisks: vi.fn() }));
const read = vi.mocked(removedNodeDisks);
const disk: RemovedDisk = { disk_id: 'one', disk_group_id: 7, operation_id: 'original', container_path: '/data/original',
  backend: 'file', backing_identity: 'file:1:2', media_type: null, sparse: false, capacity_units: '4096', unit_size_bytes: 4096 };
beforeEach(() => { read.mockReset(); read.mockResolvedValue({ items: [disk], next: 'cursor' }); });

it('pages retained candidates and preserves them when refresh is unavailable', async () => {
  const { result } = renderHook(() => useRemovedDisks(1, true));
  await waitFor(() => expect(result.current.observed).toBe(true));
  read.mockResolvedValueOnce({ items: [{ ...disk, disk_id: 'two' }], next: null });
  await act(async () => { await result.current.refresh(result.current.next!); });
  expect(result.current.items.map(disk => disk.disk_id)).toEqual(['one', 'two']);
  expect(result.current.next).toBeNull();
  read.mockRejectedValueOnce(new Error('Group 0 unavailable'));
  await act(async () => { await result.current.refresh(); });
  expect(result.current.items).toHaveLength(2);
  expect(result.current.error).toContain('Group 0 unavailable');
});

it('isolates candidate pages when the selected node changes', async () => {
  const { result, rerender } = renderHook(({ node }) => useRemovedDisks(node, true), { initialProps: { node: 1 } });
  await waitFor(() => expect(result.current.items).toHaveLength(1));
  read.mockResolvedValue({ items: [], next: null });
  rerender({ node: 2 });
  expect(result.current.items).toHaveLength(0);
  await waitFor(() => expect(result.current.observed).toBe(true));
  expect(result.current.node).toBe(2);
  expect(result.current.items).toHaveLength(0);
});

it('does not reuse a candidate read that started before a committed removal', async () => {
  let finish!: (value: { items: RemovedDisk[]; next: null }) => void;
  read.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  read.mockResolvedValueOnce({ items: [disk], next: null });
  const { result } = renderHook(() => useRemovedDisks(1, true));
  let committed!: Promise<void>;
  act(() => { committed = result.current.refresh(undefined, true); });
  await act(async () => { finish({ items: [], next: null }); await committed; });
  expect(read).toHaveBeenCalledTimes(2);
  expect(result.current.items).toEqual([disk]);
});
