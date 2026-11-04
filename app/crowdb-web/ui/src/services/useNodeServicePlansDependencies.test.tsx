// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { listServers, restartServer, restartDiskdb } from '../api';
import { serviceRequest } from './client';
import { useNodeServicePlans } from './useNodeServicePlans';
import type { EnrichedStoreView } from '../types';

vi.mock('../api', () => ({ listServers: vi.fn(), deployServer: vi.fn(), deployDiskdb: vi.fn(), restartServer: vi.fn(), restartDiskdb: vi.fn() }));
vi.mock('./client', () => ({ serviceNames: {}, serviceRequest: vi.fn() }));

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

it.each([
  ['chunk-kv', 'Waiting: config error: monitor SSH command failed: Error: managed service exited before readiness: node=1 service=chunk-kv-1 exit_code=1\n'],
  ['access-server', 'Waiting: service dependency has no committed intent'],
] as const)('observes the accepted %s dependency wait without another deployment or restart', async (kind, detail) => {
  const stores = [{ store_id: '0', groups: [{ group_id: '0' }, { group_id: '1' }] }] as unknown as EnrichedStoreView[];
  let accepted = false;
  let running = false;
  vi.mocked(listServers).mockImplementation(async () => [
    { node_id: 1, service_type: 'chunkdb', pid: 11 },
    { node_id: 1, service_type: 'diskio', pid: 12 },
    { node_id: 2, service_type: 'diskio', pid: 22 },
    { node_id: 2, service_type: 'chunk-kv', pid: 23 },
    ...(accepted ? [{ id: `${kind}-1`, node_id: 1, service_type: kind,
      pid: running ? 42 : undefined, start_pending: !running }] : []),
  ] as Awaited<ReturnType<typeof listServers>>);
  vi.mocked(serviceRequest).mockImplementation(async (path, method) => {
    if (path === '/service-plans') return {};
    if (path === '/deployment-defaults') return { [kind]: { instance_id: '1', http_port: 7400, rpc_port: 7401 } };
    if (path === '/nodes/1/services/deploy' && method === 'POST') {
      accepted = true;
      throw new Error(`HTTP 409: ${detail}`);
    }
    return { revision: 1, ready: true };
  });
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => { await result.current.start(1, [kind]); });
  expect(result.current.plans[1][kind]).toEqual({ state: 'waiting', detail });
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1][kind]).toEqual({ state: 'waiting', detail: 'Waiting: target monitor is starting the registered service' });
  running = true;
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1][kind]).toEqual({ state: 'deployed' });
  expect(vi.mocked(serviceRequest).mock.calls.filter(([path, method]) =>
    path.endsWith('/services/deploy') && method === 'POST')).toHaveLength(1);
  expect(restartServer).not.toHaveBeenCalled();
  expect(restartDiskdb).not.toHaveBeenCalled();
  expect(vi.mocked(serviceRequest).mock.calls.some(([path]) => path.endsWith('/restart'))).toBe(false);
});
