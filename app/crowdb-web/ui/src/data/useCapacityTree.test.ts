// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, renderHook } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import * as api from '../api';
import { useCapacityTree } from './useCapacityTree';

vi.mock('../api', () => ({ listDiskdbInstances: vi.fn(), getDiskdbUsage: vi.fn(), getHardwareCapacity: vi.fn(), getDiskdbScanStatus: vi.fn(), listNodeDiskGroups: vi.fn(), listDisksInGroup: vi.fn() }));
afterEach(() => { vi.resetAllMocks(); vi.useRealTimers(); vi.restoreAllMocks(); });
function setup() {
  vi.useFakeTimers();
  vi.mocked(api.listDiskdbInstances).mockResolvedValue([]);
  vi.mocked(api.getDiskdbUsage).mockResolvedValue({ disk_groups: [] });
  vi.mocked(api.getHardwareCapacity).mockResolvedValue({ datacenter_capacity_bytes: 0, racks: [], nodes: [], disk_groups: [] });
  vi.mocked(api.getDiskdbScanStatus).mockResolvedValue({ has_run: false, scan_in_progress: false } as Awaited<ReturnType<typeof api.getDiskdbScanStatus>>);
}

it('aborts on domain exit, ignores late responses and leaves no polling chain', async () => {
  setup();
  let finish!: (value: { disk_groups: [] }) => void;
  vi.mocked(api.getDiskdbUsage).mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
  const { result, rerender, unmount } = renderHook(({ enabled }) => useCapacityTree({ enabled }), { initialProps: { enabled: true } });
  const signal = vi.mocked(api.getDiskdbUsage).mock.calls[0][3]!.signal!;
  rerender({ enabled: false });
  expect(signal.aborted).toBe(true);
  await act(async () => { finish({ disk_groups: [] }); await vi.advanceTimersByTimeAsync(60000); });
  expect(api.getDiskdbUsage).toHaveBeenCalledTimes(1);
  expect(result.current.usage).toBeNull();
  expect(result.current.loading).toBe(false);
  rerender({ enabled: true });
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(api.getDiskdbUsage).toHaveBeenCalledTimes(2);
  expect(result.current.usage).toEqual({ disk_groups: [] });
  unmount();
});

it('suspends hidden-tab polling and reports partial observation failures', async () => {
  setup();
  const visibility = vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
  const { result, unmount } = renderHook(() => useCapacityTree());
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  visibility.mockReturnValue('hidden');
  act(() => document.dispatchEvent(new Event('visibilitychange')));
  await act(async () => { await vi.advanceTimersByTimeAsync(60000); });
  expect(api.getDiskdbUsage).toHaveBeenCalledTimes(1);
  vi.mocked(api.getDiskdbUsage).mockRejectedValueOnce(new Error('DiskDB offline'));
  visibility.mockReturnValue('visible');
  await act(async () => { document.dispatchEvent(new Event('visibilitychange')); await vi.advanceTimersByTimeAsync(0); });
  expect(result.current.usage).toBeNull();
  expect(result.current.hardwareCapacity).not.toBeNull();
  expect(result.current.error?.message).toContain('usage');
  unmount();
});
