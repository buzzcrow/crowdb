// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { listServers, type ServerSummary } from '../api';
import { useServiceObservations } from './useServiceObservations';

vi.mock('../api', () => ({ listServers: vi.fn() }));
afterEach(() => { cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); vi.resetAllMocks(); });
it('suspends hidden observations and fences an old response on returning to the foreground', async () => {
  vi.useFakeTimers();
  const visibility = vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
  let finish!: (servers: ServerSummary[]) => void;
  vi.mocked(listServers).mockReturnValueOnce(new Promise(resolve => { finish = resolve; }))
    .mockResolvedValue([{ id: 'access-1', service_type: 'access-server', health: 'up' }]);
  const error = vi.fn(); const { result, unmount } = renderHook(() => useServiceObservations(true, error));
  const original = vi.mocked(listServers).mock.calls[0][0]!.signal!;
  visibility.mockReturnValue('hidden'); act(() => document.dispatchEvent(new Event('visibilitychange')));
  await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
  expect(listServers).toHaveBeenCalledTimes(1); expect(original.aborted).toBe(true);
  visibility.mockReturnValue('visible');
  await act(async () => { document.dispatchEvent(new Event('visibilitychange')); });
  await act(async () => { finish([{ id: 'access-1', service_type: 'access-server', health: 'down' }]); });
  expect(result.current.servers).toEqual([{ id: 'access-1', service_type: 'access-server', health: 'up' }]);
  expect(error).not.toHaveBeenCalled();
  unmount(); await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
  expect(listServers).toHaveBeenCalledTimes(2);
});
it('does not cancel a slow manual refresh when the polling interval elapses', async () => {
  vi.useFakeTimers(); vi.mocked(listServers).mockResolvedValue([]);
  const error = vi.fn(); const { result } = renderHook(() => useServiceObservations(true, error));
  await act(async () => {});
  let finish!: (servers: ServerSummary[]) => void;
  vi.mocked(listServers).mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
  let refreshed!: Promise<void>; act(() => { refreshed = result.current.refresh(); });
  const signal = vi.mocked(listServers).mock.calls[1][0]!.signal!;
  await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
  expect(listServers).toHaveBeenCalledTimes(2); expect(signal.aborted).toBe(false);
  await act(async () => { finish([{ id: 'new-service', service_type: 'access-server', health: 'up' }]); await refreshed; });
  expect(result.current.servers).toEqual([{ id: 'new-service', service_type: 'access-server', health: 'up' }]);
});
