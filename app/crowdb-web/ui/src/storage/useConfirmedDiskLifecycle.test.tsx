// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, renderHook } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { diskLifecycle } from '../api';
import type { DiskLifecycle } from './types';
import { useConfirmedDiskLifecycle } from './useConfirmedDiskLifecycle';

vi.mock('../api', () => ({ diskLifecycle: vi.fn() }));
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });
const offline: DiskLifecycle = { version: 1, disk_id: 'one', disk_revision: '9007199254740993', offline: true,
  removed: false, removed_from_revision: null, operation_id: 'offline', remove_conditions: [] };

it('a read begun before Remove cannot restore the previous row at the same Offline revision', async () => {
  vi.useFakeTimers();
  let resolve!: (value: DiskLifecycle) => void;
  vi.mocked(diskLifecycle).mockReturnValue(new Promise(yes => { resolve = yes; }));
  const changed = vi.fn();
  const { result, unmount } = renderHook(() => useConfirmedDiskLifecycle(1, true, changed));
  act(() => result.current.confirm(7, offline));
  await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
  act(() => result.current.confirm(7, { ...offline, removed: true, disk_revision: null, removed_from_revision: offline.disk_revision }));
  await act(async () => { resolve(offline); });
  expect(result.current.disks.one.state.removed).toBe(true);
  expect(changed).not.toHaveBeenCalled();
  unmount();
});

it('preserves full-width revisions and aborts observations when the node is hidden', async () => {
  vi.useFakeTimers();
  vi.mocked(diskLifecycle).mockResolvedValue({ ...offline, disk_revision: '9007199254740992', offline: false });
  const { result, rerender } = renderHook(({ active }) => useConfirmedDiskLifecycle(1, active, vi.fn()), { initialProps: { active: true } });
  act(() => result.current.confirm(7, offline));
  await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
  expect(result.current.disks.one.state.disk_revision).toBe(offline.disk_revision);
  const signal = vi.mocked(diskLifecycle).mock.calls[0][3]?.signal;
  rerender({ active: false });
  expect(signal?.aborted).toBe(true);
});
