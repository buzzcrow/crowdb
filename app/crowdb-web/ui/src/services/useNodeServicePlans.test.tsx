// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { deployServer, deployDiskdb, listServers } from '../api';
import { serviceRequest } from './client';
import { newPlan, serviceOrder, useNodeServicePlans } from './useNodeServicePlans';
import type { EnrichedStoreView } from '../types';
vi.mock('../api', () => ({ listServers: vi.fn(), deployServer: vi.fn(), deployDiskdb: vi.fn() }));
vi.mock('./client', () => ({ serviceNames: {}, serviceRequest: vi.fn() }));
const stores = [{ store_id: '0', groups: [{ group_id: '0' }, { group_id: '1' }] }] as unknown as EnrichedStoreView[];
beforeEach(() => {
  vi.useFakeTimers();
  vi.mocked(listServers).mockResolvedValue([
    { node_id: 1, service_type: 'paxos-kv', pid: 10 }, { node_id: 1, service_type: 'diskdb', pid: 11 },
  ] as Awaited<ReturnType<typeof listServers>>);
  vi.mocked(serviceRequest).mockImplementation(async path => path === '/deployment-defaults'
    ? { chunkdb: { instance_id: '1', http_port: 12010, rpc_port: 12110 }, diskio: { instance_id: '1', rpc_port: 13010 }, 'access-server': { instance_id: '1', http_port: 9092, s3_port: 9091 } } : path === '/chunk-storage-readiness' ? { ready: true } : path === '/service-plans' ? {} : { revision: 1, ready: true });
});
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });
describe('node service plans', () => {
  it('loads saved revisions before resuming waiting steps after reload', async () => {
    let release!: (value: unknown) => void;
    const saved = new Promise(resolve => { release = resolve; });
    const request = vi.mocked(serviceRequest).getMockImplementation()!;
    vi.mocked(serviceRequest).mockImplementation(async (path, method, body) =>
      path === '/service-plans' ? saved : request(path, method, body));
    const { result } = renderHook(() => useNodeServicePlans([], {}, async () => {}, true));
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(serviceRequest).toHaveBeenCalledTimes(1);
    const steps = newPlan();
    steps['paxos-kv'] = { state: 'deployed' };
    await act(async () => { release({ 1: { revision: 7, steps } }); });
    expect(result.current.plans[1].diskio.state).toBe('waiting');
    expect(serviceRequest).toHaveBeenCalledWith('/nodes/1/service-plan', 'PUT', expect.objectContaining({ revision: 7 }));
  });
  it('keeps a deployment queued when ownership changes after its readiness probe', async () => {
    vi.mocked(listServers).mockResolvedValue([{ node_id: 1, service_type: 'diskio', pid: 10 }] as Awaited<ReturnType<typeof listServers>>);
    let ownershipPublished = false;
    const request = vi.mocked(serviceRequest).getMockImplementation()!;
    vi.mocked(serviceRequest).mockImplementation(async (path, method, body) => {
      if (path.endsWith('/services/deploy') && !ownershipPublished) throw new Error('HTTP 409: Waiting: every configured disk group needs live DiskIO ownership');
      return request(path, method, body);
    });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await result.current.start(1, ['chunkdb']); });
    expect(result.current.plans[1].chunkdb).toEqual({ state: 'waiting', detail: 'Waiting: every configured disk group needs live DiskIO ownership' });
    ownershipPublished = true;
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(result.current.plans[1].chunkdb.state).toBe('deployed');
  });
  it('finishes every selected ChunkDB deployment before journal bootstrap can block the serial plan', async () => {
    const first = newPlan();
    const second = newPlan();
    for (const kind of serviceOrder) first[kind] = second[kind] = { state: 'disabled' };
    first.chunkdb = { state: 'deployed' };
    first['chunk-kv'] = { state: 'waiting' };
    second.chunkdb = { state: 'pending' };
    const existing = [
      { node_id: 1, service_type: 'chunkdb', pid: 11 },
      { node_id: 1, service_type: 'diskio', pid: 12 },
      { node_id: 2, service_type: 'diskio', pid: 22 },
    ] as Awaited<ReturnType<typeof listServers>>;
    vi.mocked(listServers).mockImplementation(async () => existing);
    let release!: () => void;
    const deployment = new Promise<void>(resolve => { release = () => {
      existing.push({ node_id: 2, service_type: 'chunkdb', pid: 21 } as typeof existing[number]);
      resolve();
    }; });
    const request = vi.mocked(serviceRequest).getMockImplementation()!;
    vi.mocked(serviceRequest).mockImplementation(async (path, method, body) => {
      if (path === '/service-plans') return { 1: { revision: 1, steps: first }, 2: { revision: 1, steps: second } };
      if (path === '/nodes/2/services/deploy') await deployment;
      return request(path, method, body);
    });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(result.current.plans[1]['chunk-kv']).toEqual({ state: 'waiting', detail: 'Waiting: deploy selected ChunkDB services before starting Chunk-KV' });
    expect(serviceRequest).toHaveBeenCalledWith('/nodes/2/services/deploy', 'POST', expect.objectContaining({ kind: 'chunkdb' }));
    expect(vi.mocked(serviceRequest).mock.calls.some(([, method, body]) => method === 'POST' && (body as { kind?: string })?.kind === 'chunk-kv')).toBe(false);
    await act(async () => { release(); await vi.advanceTimersByTimeAsync(2000); });
    expect(serviceRequest).toHaveBeenCalledWith('/nodes/1/services/deploy', 'POST', expect.objectContaining({ kind: 'chunk-kv' }));
  });
  it('rechecks authority after initialization completes without another topology change', async () => {
    let ready = false;
    vi.mocked(listServers).mockResolvedValue([{ node_id: 1, service_type: 'paxos-kv', pid: 10 }] as Awaited<ReturnType<typeof listServers>>);
    vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans' ? {}
      : path === '/group0-readiness' ? { ready }
      : path === '/deployment-defaults' ? { diskdb: { rpc_port: 13010 } } : { revision: 1 });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await result.current.start(1, ['diskdb']); });
    expect(result.current.plans[1].diskdb.state).toBe('waiting');
    ready = true;
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(deployDiskdb).toHaveBeenCalledWith(1, { rpc_port: 13010 });
  });
  it('does not deploy from an unconfirmed Group 0 summary during initialization', async () => {
    vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans' ? {}
      : path === '/group0-readiness' ? { ready: false } : { revision: 1 });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await result.current.start(1); });
    expect(result.current.plans[1].diskio.state).toBe('waiting');
    expect(result.current.plans[1]['chunk-kv'].state).toBe('waiting');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path.endsWith('/services/deploy'))).toBe(false);
  });
  it('keeps Chunk-KV waiting for ownership even when two DiskIO processes are live', async () => {
    vi.mocked(listServers).mockResolvedValue([
      { node_id: 1, service_type: 'paxos-kv', pid: 10 },
      { node_id: 1, service_type: 'diskdb', pid: 11 },
      { node_id: 1, service_type: 'chunkdb', pid: 13 },
      { node_id: 1, service_type: 'diskio', pid: 12 },
      { node_id: 2, service_type: 'diskio', pid: 20 },
    ] as Awaited<ReturnType<typeof listServers>>);
    vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans' ? {}
      : path === '/chunk-storage-readiness' ? { ready: false, reason: 'Waiting: add disks and publish ownership' }
      : { revision: 1, ready: true });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await result.current.start(1); });
    expect(result.current.plans[1]['chunk-kv']).toEqual({ state: 'waiting', detail: 'Waiting: add disks and publish ownership' });
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path.endsWith('/services/deploy'))).toBe(false);
  });

  it('waits for stopped registered DiskIO routes even with two running mirror nodes', async () => {
    vi.mocked(listServers).mockResolvedValue([
      ...['paxos-kv', 'diskdb', 'chunkdb', 'diskio'].map(service_type => ({ node_id: 1, service_type, pid: 10 })),
      { node_id: 2, service_type: 'diskio', pid: 20 },
      { node_id: 3, service_type: 'diskio', pid: null },
    ] as Awaited<ReturnType<typeof listServers>>);
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await result.current.start(1); });
    expect(result.current.plans[1]['chunk-kv']).toEqual({ state: 'waiting', detail: 'Waiting: restart registered DiskIO services before connecting chunk storage' });
    expect(result.current.plans[1]['access-server'].state).toBe('waiting');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path.endsWith('/services/deploy'))).toBe(false);
  });
  it('keeps CDB queued until an ordinary data group exists', async () => {
    const systemOnly = [{ store_id: '0', groups: [{ group_id: '0' }] }] as unknown as EnrichedStoreView[];
    const { result } = renderHook(() => useNodeServicePlans(systemOnly, {}, async () => {}, true));
    await act(async () => { result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('waiting');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path, method]) => path.endsWith('/services/deploy') && method === 'POST')).toBe(true);
  });
  it('queues DiskDB with the other services until Group 0 is ready', async () => {
    vi.mocked(listServers).mockResolvedValue([]);
    const { result } = renderHook(() => useNodeServicePlans([], {}, async () => {}, true));
    await act(async () => { result.current.start(1); });
    expect(result.current.plans[1].diskdb).toEqual({ state: 'waiting', detail: 'Waiting: initialize Group 0 in Paxos KV' });
    expect(result.current.plans[1].chunkdb.state).toBe('waiting');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path, method]) => path.endsWith('/services/deploy') && method === 'POST')).toBe(false);
  });
  it('resumes dependencies without another submit and keeps unrelated prerequisites waiting', async () => {
    const refresh = vi.fn().mockResolvedValue(undefined);
    const request = vi.mocked(serviceRequest).getMockImplementation()!;
    vi.mocked(serviceRequest).mockImplementation(async (path, method, body) => {
      const value = await request(path, method, body);
      if (path.endsWith('/services/deploy') && method === 'POST') {
        const existing = await listServers();
        vi.mocked(listServers).mockResolvedValue([...existing, { node_id: 1, service_type: (body as { kind: string }).kind, pid: 100 }] as Awaited<ReturnType<typeof listServers>>);
      }
      return value;
    });
    const { result, rerender } = renderHook(({ value }) => useNodeServicePlans(value, {}, refresh, true), { initialProps: { value: [] as EnrichedStoreView[] } });
    await act(async () => { result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('waiting');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path === '/deployment-defaults')).toBe(false);
    rerender({ value: stores });
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(result.current.plans[1].chunkdb.state).toBe('deployed');
    expect(result.current.plans[1]['access-server'].state).toBe('waiting');
    expect(result.current.plans[1].diskio.state).toBe('deployed');
    expect(result.current.plans[1]['chunk-kv'].state).toBe('waiting');
    expect(serviceRequest).toHaveBeenCalledWith('/nodes/1/services/deploy', 'POST', expect.objectContaining({ kind: 'chunkdb', test_single_node: false }));
    vi.mocked(listServers).mockResolvedValue([{ node_id: 2, service_type: 'chunk-kv', pid: 100 }] as Awaited<ReturnType<typeof listServers>>);
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(result.current.plans[1]['access-server'].state).toBe('deployed');
    expect(vi.mocked(serviceRequest).mock.calls.filter(([, method]) => method === 'POST')).toHaveLength(3);
  });
  it('does not retry failures automatically and stops queued deployments before reset', async () => {
    vi.mocked(serviceRequest).mockImplementation(async path => { if (path === '/service-plans') return {}; if (path === '/chunk-storage-readiness' || path === '/group0-readiness') return { ready: true }; if (path.endsWith('/service-plan')) return { revision: 1, ready: true }; throw new Error('Unavailable'); });
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
    const steps = Object.fromEntries(['paxos-kv', 'diskdb', 'chunkdb', 'diskio', 'chunk-kv', 'access-server'].map(kind => [kind,
      { state: kind === 'chunkdb' ? 'deploying' : kind === 'paxos-kv' || kind === 'diskdb' ? 'deployed' : 'waiting' }]));
    vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans' ? { 1: { revision: 7, steps } } : path === '/chunk-storage-readiness' ? { ready: true } : { revision: 8, ready: true });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => {});
    expect(result.current.plans[1].chunkdb.state).toBe('failed');
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path === '/deployment-defaults')).toBe(true);
    vi.mocked(listServers).mockResolvedValue([
      { node_id: 1, service_type: 'paxos-kv', pid: 10 }, { node_id: 1, service_type: 'diskdb', pid: 11 },
      { node_id: 1, service_type: 'chunkdb', pid: 12 },
    ] as Awaited<ReturnType<typeof listServers>>);
    await act(async () => { await result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('deployed');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path, method, body]) => path.endsWith('/services/deploy') && method === 'POST' && (body as { kind?: string })?.kind === 'chunkdb')).toBe(false);
    expect(serviceRequest).toHaveBeenCalledWith('/nodes/1/service-plan', 'PUT', expect.objectContaining({ revision: 7 }));
  });
  it('does not deploy when a competing browser owns the saved plan revision', async () => {
    vi.mocked(serviceRequest).mockImplementation(async path => { if (path === '/service-plans') return {}; throw new Error('Deployment plan changed; reload before resuming'); });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await expect(result.current.start(1)).rejects.toThrow('Deployment plan changed'); });
    expect(result.current.plans[1].chunkdb.state).toBe('failed');
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path.endsWith('/services/deploy'))).toBe(false);
  });

  it('keeps unselected services disabled when the first durable save fails', async () => {
    vi.mocked(serviceRequest).mockImplementation(async path => { if (path === '/service-plans') return {}; throw new Error('Disk full'); });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await expect(result.current.start(1, ['paxos-kv'])).rejects.toThrow('Disk full'); });
    expect(result.current.plans[1]['paxos-kv'].state).toBe('failed');
    for (const kind of serviceOrder.filter(kind => kind !== 'paxos-kv')) expect(result.current.plans[1][kind].state).toBe('disabled');
    expect(deployServer).not.toHaveBeenCalled();
  });

  it('starts only PKV before Group 0 and resumes DiskDB immediately when Group 0 appears', async () => {
    vi.mocked(listServers).mockResolvedValue([]);
    vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans' ? {} : path === '/deployment-defaults'
      ? { 'paxos-kv': { http_port: 19910, rpc_port: 19920 }, diskdb: { rpc_port: 29920 } } : { revision: 1, ready: true });
    const { result, rerender } = renderHook(({ value }) => useNodeServicePlans(value, {}, async () => {}, true), { initialProps: { value: [] as EnrichedStoreView[] } });
    await act(async () => { await result.current.start(1, ['paxos-kv', 'diskdb'], { diskdb: { rpc_port: 29999 } }); });
    expect(deployServer).toHaveBeenCalledTimes(1);
    expect(deployDiskdb).not.toHaveBeenCalled();
    expect(result.current.plans[1].diskdb.state).toBe('waiting');
    expect(result.current.plans[1].diskio.state).toBe('disabled');
    vi.mocked(listServers).mockResolvedValue([{ node_id: 1, service_type: 'paxos-kv', pid: 10 }] as Awaited<ReturnType<typeof listServers>>);
    rerender({ value: stores });
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(deployDiskdb).toHaveBeenCalledWith(1, { rpc_port: 29999 });
  });
  it('retains disabled services and saved listener overrides through recovery and retry', async () => {
    const steps = newPlan();
    for (const kind of serviceOrder) steps[kind] = { state: 'disabled' };
    steps['paxos-kv'] = { state: 'failed', detail: 'port bind failed' };
    vi.mocked(listServers).mockResolvedValue([]);
    vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans'
      ? { 1: { revision: 4, steps, overrides: { 'paxos-kv': { http_port: 19222, rpc_port: 19223 } } } }
      : path === '/deployment-defaults' ? { 'paxos-kv': { http_port: 19910, rpc_port: 19920 } } : { revision: 5 });
    const { result } = renderHook(() => useNodeServicePlans([], {}, async () => {}, true));
    await act(async () => { await result.current.start(1); });
    expect(deployServer).toHaveBeenCalledWith(1, { rest_port: 19222, rpc_port: 19223 });
    for (const kind of serviceOrder.filter(kind => kind !== 'paxos-kv')) expect(result.current.plans[1][kind].state).toBe('disabled');
    expect(serviceRequest).toHaveBeenCalledWith('/nodes/1/service-plan', 'PUT', expect.objectContaining({ overrides: { 'paxos-kv': { http_port: 19222, rpc_port: 19223 } } }));
  });
  it('retries a saved fixed-slot warning as an idle ChunkDB deployment', async () => {
    const steps = newPlan();
    for (const kind of serviceOrder) steps[kind] = { state: 'disabled' };
    steps.chunkdb = { state: 'warning', detail: 'CDB instance is outside the fixed slot plan' };
    vi.mocked(listServers).mockResolvedValue([{ node_id: 1, service_type: 'diskio', pid: 10 }] as Awaited<ReturnType<typeof listServers>>);
    vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans'
      ? { 1: { revision: 4, steps } } : path === '/deployment-defaults'
        ? { chunkdb: { instance_id: '4', http_port: 12010, rpc_port: 12110 } } : { revision: 5, ready: true });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await result.current.start(1); });
    expect(result.current.plans[1].chunkdb.state).toBe('deployed');
  });
  it('keeps disabled steps disabled when server observation fails', async () => {
    vi.mocked(listServers).mockRejectedValue(new Error('Registry unavailable'));
    const { result } = renderHook(() => useNodeServicePlans([], {}, async () => {}, true));
    await act(async () => { await result.current.start(1, ['paxos-kv']); });
    expect(result.current.plans[1]['paxos-kv'].state).toBe('failed');
    for (const kind of serviceOrder.filter(kind => kind !== 'paxos-kv')) expect(result.current.plans[1][kind].state).toBe('disabled');
  });
  it('waits for live ownership even when DiskIO processes are registered and resumes when it is published', async () => {
    vi.mocked(listServers).mockResolvedValue([{ node_id: 1, service_type: 'diskio', pid: 10 }] as Awaited<ReturnType<typeof listServers>>);
    let ready = false;
    vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans' ? {} : path === '/chunk-storage-readiness'
      ? { ready, reason: 'Waiting: diskio-1 must publish live disk-group ownership' }
      : path === '/deployment-defaults' ? { chunkdb: { instance_id: '4', http_port: 12010, rpc_port: 12110 } } : { revision: 1, ready: true });
    const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
    await act(async () => { await result.current.start(1, ['chunkdb']); });
    expect(result.current.plans[1].chunkdb).toEqual({ state: 'waiting', detail: 'Waiting: diskio-1 must publish live disk-group ownership' });
    expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path.endsWith('/services/deploy'))).toBe(false);
    ready = true;
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(result.current.plans[1].chunkdb.state).toBe('deployed');
  });

});
