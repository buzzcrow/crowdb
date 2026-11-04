// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { deployServer, deployDiskdb, listServers, restartServer, restartDiskdb } from '../api';
import type { ServerSummary } from '../api';
import { serviceRequest } from './client';
import { newPlan, serviceOrder, useNodeServicePlans } from './useNodeServicePlans';
import type { EnrichedStoreView } from '../types';

vi.mock('../api', () => ({ listServers: vi.fn(), deployServer: vi.fn(), deployDiskdb: vi.fn(), restartServer: vi.fn(), restartDiskdb: vi.fn() }));
vi.mock('./client', () => ({ serviceNames: {}, serviceRequest: vi.fn() }));
type ProcessState = 'running' | 'stopped' | 'unknown';
type ObservedServer = ServerSummary & { process_observed: boolean };
const stores = [{ store_id: '0', groups: [{ group_id: '0' }, { group_id: '1' }] }] as unknown as EnrichedStoreView[];
const kv = (state: ProcessState): ObservedServer => ({ id: 'kv-1', node_id: 1, service_type: 'paxos-kv',
  pid: state === 'running' ? 126 : undefined, process_observed: state !== 'unknown', start_pending: false, health: 'up' });
const diskdb = (running: boolean, pending: boolean): ObservedServer => ({ id: 'diskdb-1', node_id: 1,
  service_type: 'diskdb', pid: running ? 272 : undefined, process_observed: !pending,
  start_pending: pending, health: 'up' });
function savedPlan() {
  const steps = newPlan();
  for (const kind of serviceOrder) steps[kind] = { state: 'disabled' };
  steps['paxos-kv'] = { state: 'deployed' };
  steps.diskdb = { state: 'waiting', detail: 'Waiting: target monitor is starting the registered service' };
  vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans'
    ? { 1: { revision: 7, steps } } : { revision: 8, ready: true });
}
function noLifecycleMutation() {
  expect(deployServer).not.toHaveBeenCalled();
  expect(deployDiskdb).not.toHaveBeenCalled();
  expect(restartServer).not.toHaveBeenCalled();
  expect(restartDiskdb).not.toHaveBeenCalled();
  expect(vi.mocked(serviceRequest).mock.calls.some(([path, method]) => method === 'POST'
    && (path.endsWith('/deploy') || path.endsWith('/restart')))).toBe(false);
}
beforeEach(() => { vi.useFakeTimers(); savedPlan(); });
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

it('observes an unknown current PKV process fact and recovers the same PID without Retry', async () => {
  let state: ProcessState = 'running';
  let finished = false;
  vi.mocked(listServers).mockImplementation(async () => [kv(state), diskdb(finished, !finished)]);
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => {});
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  state = 'unknown';
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('waiting');
  expect(result.current.plans[1]['paxos-kv'].detail).toMatch(/process|observation/i);
  noLifecycleMutation();
  state = 'running'; finished = true;
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  expect(result.current.plans[1].diskdb.state).toBe('deployed');
  noLifecycleMutation();
});

it.each([['confirmed observation', true], ['legacy field omitted', undefined]] as const)('observes a stopped PKV and reconciles its recovery without a lifecycle mutation with %s', async (_label, processObserved) => {
  let state: ProcessState = 'running';
  vi.mocked(listServers).mockImplementation(async () => [{ ...kv(state), process_observed: processObserved }, diskdb(false, true)]);
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => {});
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  state = 'stopped';
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('failed');
  await act(async () => { await vi.advanceTimersByTimeAsync(4000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('failed');
  noLifecycleMutation();
  state = 'running';
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  noLifecycleMutation();
});

it('observes an exhausted startup without restarting it and reconciles a later owned PID', async () => {
  let exhausted = false;
  let laterPid = false;
  vi.mocked(listServers).mockImplementation(async () => [kv('running'), diskdb(laterPid, !exhausted)]);
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => {});
  expect(result.current.plans[1].diskdb.state).toBe('waiting');
  exhausted = true;
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1].diskdb.state).toBe('failed');
  await act(async () => { await vi.advanceTimersByTimeAsync(4000); });
  expect(result.current.plans[1].diskdb.state).toBe('failed');
  noLifecycleMutation();
  laterPid = true;
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1].diskdb.state).toBe('deployed');
  noLifecycleMutation();
});

it('observes unknown process state every two seconds without loaded Capacity data', async () => {
  let state: ProcessState = 'running';
  let finished = false;
  vi.mocked(listServers).mockImplementation(async () => [kv(state), diskdb(finished, !finished)]);
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => {});
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  // Finish the only other queued service before observing the unknown process.
  // The next timer must observe this process rather than use the disk backoff.
  state = 'unknown'; finished = true;
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('waiting');
  expect(result.current.plans[1].diskdb.state).toBe('deployed');
  state = 'running';
  await act(async () => { await vi.advanceTimersByTimeAsync(1999); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('waiting');
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  noLifecycleMutation();
});

function failedPlan() {
  const steps = newPlan();
  for (const kind of serviceOrder) steps[kind] = { state: 'disabled' };
  steps['paxos-kv'] = { state: 'failed', detail: 'Previously reported stopped' };
  return steps;
}
function recoverPlan(steps = failedPlan(), second?: ReturnType<typeof failedPlan>) {
  vi.mocked(serviceRequest).mockImplementation(async path => path === '/service-plans'
    ? { 1: { revision: 7, steps }, ...(second ? { 2: { revision: 3, steps: second } } : {}) }
    : { revision: 8, ready: true });
}

it('observes a persisted failed-only plan and recovers only after current process evidence returns', async () => {
  recoverPlan();
  let state: ProcessState = 'unknown';
  vi.mocked(listServers).mockImplementation(async () => [kv(state)]);
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => {});
  expect(result.current.plans[1]['paxos-kv'].state).toBe('failed');
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('failed');
  noLifecycleMutation();
  state = 'running';
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  noLifecycleMutation();
});

it('does not deploy a failed missing service while observing its later registered recovery', async () => {
  recoverPlan();
  let present = false;
  vi.mocked(listServers).mockImplementation(async () => present ? [kv('running')] : []);
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => { await vi.advanceTimersByTimeAsync(4000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('failed');
  noLifecycleMutation();
  present = true;
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  noLifecycleMutation();
});

it('keeps failed-service observation at two seconds while another plan waits for unloaded disks', async () => {
  const second = failedPlan();
  second['paxos-kv'] = { state: 'disabled' };
  second['chunk-kv'] = { state: 'waiting', detail: 'Waiting: deploy DiskIO on at least two nodes for journal mirrors' };
  recoverPlan(failedPlan(), second);
  let state: ProcessState = 'stopped';
  vi.mocked(listServers).mockImplementation(async () => [kv(state)]);
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => {});
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('failed');
  state = 'running';
  await act(async () => { await vi.advanceTimersByTimeAsync(1999); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('failed');
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  noLifecycleMutation();
});


it('preserves an existing failure and fixed-slot warning across inventory read errors and observes recovery', async () => {
  const steps = failedPlan();
  steps.chunkdb = { state: 'warning', detail: 'Healthy service outside the fixed slot plan' };
  recoverPlan(steps);
  let unavailable = true;
  vi.mocked(listServers).mockImplementation(async () => {
    if (unavailable) throw new Error('Controlled inventory unavailable');
    return [kv('running')];
  });
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv']).toEqual(steps['paxos-kv']);
  expect(result.current.plans[1].chunkdb).toEqual(steps.chunkdb);
  noLifecycleMutation();
  unavailable = false;
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  expect(result.current.plans[1].chunkdb).toEqual(steps.chunkdb);
  noLifecycleMutation();
});

it('keeps unattempted work waiting across an unavailable inventory and resumes observation within two seconds', async () => {
  const steps = failedPlan();
  steps['paxos-kv'] = { state: 'pending' };
  recoverPlan(steps);
  let unavailable = true;
  vi.mocked(listServers).mockImplementation(async () => {
    if (unavailable) throw new Error('Controlled inventory unavailable');
    return [kv('running')];
  });
  const { result } = renderHook(() => useNodeServicePlans(stores, {}, async () => {}, true));
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('waiting');
  expect(result.current.plans[1]['paxos-kv'].detail).toContain('inventory');
  noLifecycleMutation();
  unavailable = false;
  await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
  expect(result.current.plans[1]['paxos-kv'].state).toBe('deployed');
  noLifecycleMutation();
});
