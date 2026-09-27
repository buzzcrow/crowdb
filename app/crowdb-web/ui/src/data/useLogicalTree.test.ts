// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { getGroup, listStores } from '../api';
import { GroupHealth, ReplicaRole, ReplicaState } from '../types';
import { useLogicalTree } from './useLogicalTree';

vi.mock('../api', () => ({ listStores: vi.fn(), getGroup: vi.fn(), listGroups: vi.fn() }));
afterEach(() => { vi.resetAllMocks(); });

it('removes previously confirmed topology during an outage and restores confirmed reads', async () => {
  vi.mocked(listStores).mockResolvedValue([{
    store_id: '7', nodes: [1], groups: [{ group_id: '70', replica_count: 1 }],
  }]);
  vi.mocked(getGroup).mockResolvedValue({
    store_id: '7', group_id: '70', state: GroupHealth.Healthy,
    replicas: [{ store_id: '7', group_id: '70', replica_id: '700', node_id: 1,
      role: ReplicaRole.Leader, state: ReplicaState.Running, engine_healthy: true }],
  });
  const { result, unmount } = renderHook(() => useLogicalTree());
  await waitFor(() => expect(result.current.replicas).toHaveLength(1));
  vi.mocked(listStores).mockRejectedValueOnce(new Error('Group 0 unavailable'));
  await act(() => result.current.refresh());
  expect(result.current.error?.message).toBe('Group 0 unavailable');
  expect(result.current.stores).toEqual([]);
  expect(result.current.groups).toEqual([]);
  expect(result.current.replicas).toEqual([]);
  await act(() => result.current.refresh());
  expect(result.current.error).toBeNull();
  expect(result.current.replicas).toHaveLength(1);
  unmount();
});
