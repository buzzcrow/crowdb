// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { listServers } from '../api';
import { serviceRequest } from './client';
import { useNodeServicePlans } from './useNodeServicePlans';
import type { EnrichedStoreView } from '../types';
vi.mock('../api', () => ({ listServers: vi.fn(), deployServer: vi.fn(), deployDiskdb: vi.fn() }));
vi.mock('./client', () => ({ serviceNames: {}, serviceRequest: vi.fn() }));
const stores = [{ store_id: '0', groups: [] }] as unknown as EnrichedStoreView[];
beforeEach(() => {
  vi.useFakeTimers();
  vi.mocked(listServers).mockResolvedValue([
    { node_id: 1, service_type: 'kv' }, { node_id: 1, service_type: 'diskdb' },
  ] as Awaited<ReturnType<typeof listServers>>);
  vi.mocked(serviceRequest).mockImplementation(async path => path === '/deployment-defaults'
    ? { chunkdb: { instance_id: '1', http_port: 12010, rpc_port: 12110 }, 'access-server': { instance_id: '1', http_port: 9092, s3_port: 9091 } } : {});
});
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });
describe('node service plans', () => {
  it('resumes dependencies without another submit and keeps unrelated prerequisites waiting', async () => {
    const refresh = vi.fn().mockResolvedValue(undefined);
    const { result, rerender } = renderHook(({ value }) => useNodeServicePlans(value, {}, refresh, true), { initialProps: { value: [] as EnrichedStoreView[] } });
    await act(async () => { result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('waiting');
    expect(serviceRequest).not.toHaveBeenCalled();
    rerender({ value: stores });
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(result.current.plans[1].chunkdb.state).toBe('deployed');
    expect(result.current.plans[1]['access-server'].state).toBe('deployed');
    expect(result.current.plans[1].diskio.state).toBe('waiting');
    expect(result.current.plans[1]['chunk-kv'].state).toBe('waiting');
    expect(serviceRequest).toHaveBeenCalledWith('/nodes/1/services/deploy', 'POST', expect.objectContaining({ kind: 'chunkdb', test_single_node: false }));
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(vi.mocked(serviceRequest).mock.calls.filter(([, method]) => method === 'POST')).toHaveLength(2);
  });
  it('does not retry failures automatically and stops queued deployments before reset', async () => {
    vi.mocked(serviceRequest).mockRejectedValue(new Error('Unavailable'));
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('failed');
    const attempts = vi.mocked(serviceRequest).mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(serviceRequest).toHaveBeenCalledTimes(attempts);
    await act(async () => { await result.current.stop(); });
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(result.current.plans).toEqual({});
    expect(serviceRequest).toHaveBeenCalledTimes(attempts);
  });
});
