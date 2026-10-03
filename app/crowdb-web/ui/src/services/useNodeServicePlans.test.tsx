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
const stores = [{ store_id: '0', groups: [{ group_id: '0' }, { group_id: '1' }] }] as unknown as EnrichedStoreView[];
beforeEach(() => {
  vi.useFakeTimers();
  vi.mocked(listServers).mockResolvedValue([
    { node_id: 1, service_type: 'kv', pid: 10 }, { node_id: 1, service_type: 'diskdb', pid: 11 },
  ] as Awaited<ReturnType<typeof listServers>>);
  vi.mocked(serviceRequest).mockImplementation(async path => path === '/deployment-defaults'
    ? { chunkdb: { instance_id: '1', http_port: 12010, rpc_port: 12110 }, 'access-server': { instance_id: '1', http_port: 9092, s3_port: 9091 } } : path === '/service-plans' ? {} : { revision: 1 });
});
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });
describe('node service plans', () => {
  it('keeps CDB queued until an ordinary data group exists', async () => {
    const systemOnly = [{ store_id: '0', groups: [{ group_id: '0' }] }] as unknown as EnrichedStoreView[];
    const { result } = renderHook(() => useNodeServicePlans(systemOnly, {}, async () => {}, true));
    await act(async () => { result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('waiting');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path === '/deployment-defaults')).toBe(false);
  });
  it('resumes dependencies without another submit and keeps unrelated prerequisites waiting', async () => {
    const refresh = vi.fn().mockResolvedValue(undefined);
    const { result, rerender } = renderHook(({ value }) => useNodeServicePlans(value, {}, refresh, true), { initialProps: { value: [] as EnrichedStoreView[] } });
    await act(async () => { result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('waiting');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path === '/deployment-defaults')).toBe(false);
    rerender({ value: stores });
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(result.current.plans[1].chunkdb.state).toBe('deployed');
    expect(result.current.plans[1]['access-server'].state).toBe('waiting');
    expect(result.current.plans[1].diskio.state).toBe('waiting');
    expect(result.current.plans[1]['chunk-kv'].state).toBe('waiting');
    expect(serviceRequest).toHaveBeenCalledWith('/nodes/1/services/deploy', 'POST', expect.objectContaining({ kind: 'chunkdb', test_single_node: false }));
    vi.mocked(listServers).mockResolvedValue([{ node_id: 2, service_type: 'chunk-kv', pid: 100 }] as Awaited<ReturnType<typeof listServers>>);
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(result.current.plans[1]['access-server'].state).toBe('deployed');
    expect(vi.mocked(serviceRequest).mock.calls.filter(([, method]) => method === 'POST')).toHaveLength(2);
  });
  it('does not retry failures automatically and stops queued deployments before reset', async () => {
    vi.mocked(serviceRequest).mockImplementation(async path => { if (path === '/service-plans') return {}; if (path.endsWith('/service-plan')) return { revision: 1 }; throw new Error('Unavailable'); });
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
  it('recovers waiting progress and fences interrupted deployment until explicit reconciliation', async () => {
    const steps = Object.fromEntries(['kv', 'diskdb', 'chunkdb', 'diskio', 'chunk-kv', 'access-server'].map(kind => [kind,
      { state: kind === 'chunkdb' ? 'deploying' : kind === 'kv' || kind === 'diskdb' ? 'deployed' : 'waiting' }]));
    vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans' ? { 1: { revision: 7, steps } } : { revision: 8 });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => {});
    expect(result.current.plans[1].chunkdb.state).toBe('failed');
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path === '/deployment-defaults')).toBe(false);
    vi.mocked(listServers).mockResolvedValue([
      { node_id: 1, service_type: 'kv', pid: 10 }, { node_id: 1, service_type: 'diskdb', pid: 11 },
      { node_id: 1, service_type: 'chunkdb', pid: 12 },
    ] as Awaited<ReturnType<typeof listServers>>);
    await act(async () => { await result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('deployed');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path.endsWith('/services/deploy'))).toBe(false);
    expect(serviceRequest).toHaveBeenCalledWith('/nodes/1/service-plan', 'PUT', expect.objectContaining({ revision: 7 }));
  });
  it('does not deploy when a competing browser owns the saved plan revision', async () => {
    vi.mocked(serviceRequest).mockImplementation(async path => { if (path === '/service-plans') return {}; throw new Error('Deployment plan changed; reload before resuming'); });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('failed');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path.endsWith('/services/deploy'))).toBe(false);
  });

});
