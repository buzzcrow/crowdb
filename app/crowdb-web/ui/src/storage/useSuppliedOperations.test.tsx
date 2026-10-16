// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, renderHook } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { listNodeCandidateOperations, nodeCandidateOperation } from '../api';
import { useSuppliedOperations } from './useSuppliedOperations';
import type { SuppliedOperation } from './types';
vi.mock('../api', () => ({ listNodeCandidateOperations: vi.fn(), nodeCandidateOperation: vi.fn() }));
afterEach(() => { vi.resetAllMocks(); vi.useRealTimers(); });
const row = (id: number): SuppliedOperation => ({ operation_id: String(id), node_id: 1, disk_group_id: 7, disk_id: `disk-${id}`,
  medium: { path: `/disks/${id}`, backend: 'file', backing_identity: `file:1:${id}`, resource_identity: `file:1:${id}`,
    capacity_bytes: 67108864, initialize: true, media_type: null }, phase: 'accepted', verified_bytes: 0, error: null });

it('resumes bounded durable pages and observes siblings even if one status is unavailable', async () => {
  vi.useFakeTimers();
  const rows = Array.from({ length: 6 }, (_, id) => row(id));
  vi.mocked(listNodeCandidateOperations).mockResolvedValue({ items: rows, next: null });
  vi.mocked(nodeCandidateOperation).mockImplementation(async (_, id) => {
    if (id === '0') throw new Error('one owner unavailable');
    return { ...rows[Number(id)], phase: 'registered' };
  });
  const { result, unmount } = renderHook(() => useSuppliedOperations(1, true));
  await act(async () => {});
  expect(result.current.operations['0'].phase).toBe('accepted');
  expect(result.current.operations['5'].phase).toBe('registered');
  expect(result.current.error).toContain('one owner unavailable');
  expect(nodeCandidateOperation).toHaveBeenCalledTimes(6);
  unmount();
  await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
  expect(nodeCandidateOperation).toHaveBeenCalledTimes(6);
});
it('aborts a departed node and rejects a changed operation identity without replacing its row', async () => {
  vi.useFakeTimers();
  const original = row(1);
  let resolve!: (value: SuppliedOperation | null) => void;
  vi.mocked(listNodeCandidateOperations).mockResolvedValue({ items: [original], next: null });
  vi.mocked(nodeCandidateOperation).mockResolvedValue({ ...original, medium: { ...original.medium, backing_identity: 'file:99:99' } });
  const { result, rerender, unmount } = renderHook(({ node }) => useSuppliedOperations(node, true), { initialProps: { node: 1 } });
  await act(async () => {});
  expect(result.current.operations['1'].medium.backing_identity).toBe('file:1:1');
  expect(result.current.error).toContain('identity changed');
  vi.mocked(nodeCandidateOperation).mockReturnValue(new Promise(done => { resolve = done; }));
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  const calls = vi.mocked(nodeCandidateOperation).mock.calls;
  const signal = calls[calls.length - 1][2]!.signal!;
  vi.mocked(listNodeCandidateOperations).mockResolvedValue({ items: [], next: null });
  rerender({ node: 2 });
  expect(signal.aborted).toBe(true);
  await act(async () => { resolve({ ...original, phase: 'registered' }); });
  expect(result.current.operations).toEqual({});
  unmount();
});
