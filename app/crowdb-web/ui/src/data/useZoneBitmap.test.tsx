// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { getDiskdbUsage } from '../api';
import { useZoneBitmap } from './useZoneBitmap';
import type { CapacityUsageResponse } from '../types';
vi.mock('../api', () => ({ getDiskdbUsage: vi.fn() }));
afterEach(() => { vi.resetAllMocks(); });
it('keeps same-Zone data on failure, masks it on navigation, and ignores late results', async () => {
  const response = (zone: number) => ({ disk_groups: [{ disk_group_id: 1, disks: [{ disk_id: 'disk-a', zone_usages: [{ zone_index: zone, usage_bitmap: '0f' }] }] }] }) as CapacityUsageResponse;
  vi.mocked(getDiskdbUsage).mockResolvedValue(response(0));
  const { result, rerender } = renderHook(({ zone }) => useZoneBitmap(1, 'disk-a', zone), { initialProps: { zone: 0 } });
  await waitFor(() => expect(result.current.zone?.usage_bitmap).toBe('0f'));
  vi.mocked(getDiskdbUsage).mockRejectedValue(new Error('offline'));
  await act(async () => result.current.refresh());
  expect(result.current.zone?.zone_index).toBe(0);
  expect(result.current.error?.message).toBe('offline');
  let finish!: (value: CapacityUsageResponse) => void;
  vi.mocked(getDiskdbUsage).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  rerender({ zone: 1 }); expect(result.current.zone).toBeNull();
  vi.mocked(getDiskdbUsage).mockResolvedValue(response(2));
  rerender({ zone: 2 });
  await waitFor(() => expect(result.current.zone?.zone_index).toBe(2));
  await act(async () => finish(response(1)));
  expect(result.current.zone?.zone_index).toBe(2);
});
